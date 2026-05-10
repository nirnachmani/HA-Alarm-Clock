"""Handle media playback for HA Alarm Clock alarms and reminders."""
from typing import Optional, Callable, Any, Dict, List

import asyncio
import logging
from time import perf_counter
from urllib.parse import urlparse, urljoin

from homeassistant.components import media_source
from homeassistant.components.media_source import MediaSourceError
from homeassistant.components.tts import async_create_stream, async_resolve_engine
from homeassistant.core import Context, HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.network import get_url

from .const import SPOTIFY_PLATFORMS

try:  # Music Assistant integration is optional
    from homeassistant.components.music_assistant.actions import (
        get_music_assistant_client,
    )
except ImportError:  # pragma: no cover - MA not installed
    get_music_assistant_client = None

_LOGGER = logging.getLogger(__name__)


class _PlaybackWatcher:
    """Observe playback transitions for a media player."""

    def __init__(
        self,
        hass: HomeAssistant,
        entity_id: str,
        context: Context | None,
        stop_event: asyncio.Event | None,
        label: str,
    ) -> None:
        self._hass = hass
        self._entity_id = entity_id
        self._context = context
        self._stop_event = stop_event
        self._label = label
        self._remove: Callable[[], None] | None = async_track_state_change_event(
            hass,
            [entity_id],
            self._handle_state,
        )
        self.started_event = asyncio.Event()
        self.last_state: str | None = None
        self.last_attrs: Dict[str, Any] | None = None
        initial = hass.states.get(entity_id)
        if initial:
            self.last_state = initial.state
            self.last_attrs = dict(initial.attributes or {})

    def close(self) -> None:
        if self._remove:
            self._remove()
            self._remove = None

    def _handle_state(self, event) -> None:
        if self._stop_event and self._stop_event.is_set():
            return
        new_state = event.data.get("new_state")
        if not new_state:
            return

        self.last_state = new_state.state
        self.last_attrs = dict(new_state.attributes or {})
        state = new_state.state

        if state in ("playing", "buffering"):
            if (
                self._matches_context(new_state.context)
                or self._context is None
                or new_state.context is None
            ):
                if not self.started_event.is_set():
                    _LOGGER.debug(
                        "MediaHandler: watcher detected %s playback state=%s on %s",
                        self._label,
                        state,
                        self._entity_id,
                    )
                    self.started_event.set()

    def _matches_context(self, context: Context | None) -> bool:
        if context is None or self._context is None:
            return False
        context_ids = {context.id, context.parent_id}
        target_ids = {self._context.id, self._context.parent_id}
        return bool(context_ids & target_ids)

    async def wait_started(self, timeout: float | None = None) -> bool:
        return await self._wait_event(self.started_event, timeout)

    async def _wait_event(self, event: asyncio.Event, timeout: float | None) -> bool:
        if event.is_set():
            return True

        tasks: List[asyncio.Task] = [asyncio.create_task(event.wait())]
        if self._stop_event is not None:
            tasks.append(asyncio.create_task(self._stop_event.wait()))

        try:
            await asyncio.wait(
                tasks,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

        if self._stop_event is not None and self._stop_event.is_set():
            return False

        return event.is_set()


class MediaHandler:
    """Handles playing sounds and TTS on media players."""
    
    def __init__(self, hass: HomeAssistant, alarm_sound: str, reminder_sound: str):
        """Initialize media handler."""
        self.hass = hass
        self.alarm_sound = alarm_sound
        self.reminder_sound = reminder_sound
        self._tts_entity: str | None = None
        self._logged_missing_tts = False
        self._player_profile_resolver: Callable[[Optional[str]], Dict[str, Any]] | None = None
        self._local_profile_cache: Dict[str, Dict[str, Any]] = {}
        self._player_volume_stack: Dict[str, list[Dict[str, Any]]] = {}

    def set_media_player_profile_resolver(
        self, resolver: Callable[[Optional[str]], Dict[str, Any]]
    ) -> None:
        """Allow coordinator to supply a shared media-player classifier."""
        self._player_profile_resolver = resolver
        self._local_profile_cache.clear()

    def _get_media_player_profile(self, entity_id: Optional[str]) -> Dict[str, Any]:
        """Return cached profile for the requested media player."""
        cache_key = entity_id or "__none__"
        cached = self._local_profile_cache.get(cache_key)
        if cached is not None:
            return cached

        profile: Dict[str, Any] | None = None
        if self._player_profile_resolver:
            try:
                profile = self._player_profile_resolver(entity_id)
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "MediaHandler: resolver lookup failed for %s: %s",
                    entity_id,
                    err,
                )

        if not profile:
            profile = self._detect_media_player_profile(entity_id)

        # Cache a shallow copy to avoid external mutation surprises.
        stored = dict(profile)
        self._local_profile_cache[cache_key] = stored
        return stored

    @staticmethod
    def _redact_media_url(url: str | None) -> str:
        """Mask sensitive query strings when logging media URLs."""
        if not url:
            return "<none>"
        try:
            parsed = urlparse(str(url))
        except Exception:  # noqa: BLE001
            return "<invalid>"
        if parsed.scheme in {"http", "https"} and (parsed.query or parsed.password):
            sanitized = parsed._replace(query="***", password="***")
            return sanitized.geturl()
        return str(url)

    def _detect_media_player_profile(self, entity_id: Optional[str]) -> Dict[str, Any]:
        """Determine platform family for a media player using local state."""
        if not entity_id:
            return {
                "entity_id": None,
                "family": "unknown",
                "platform": None,
                "mass_player_type": None,
                "attributes": {},
            }

        registry = er.async_get(self.hass)
        entry = registry.async_get(entity_id)
        platform = entry.platform if entry else None

        state = self.hass.states.get(entity_id)
        attributes = dict(state.attributes) if state and state.attributes else {}

        mass_player_type = attributes.get("mass_player_type")
        mass_provider = attributes.get("mass_provider")
        platform_normalized = platform.lower() if isinstance(platform, str) else None
        is_spotify_player = platform_normalized in SPOTIFY_PLATFORMS
        is_music_assistant = platform_normalized == "music_assistant" or bool(
            mass_player_type or mass_provider or attributes.get("ma_source")
        )

        if is_music_assistant:
            family = "music_assistant"
        elif is_spotify_player:
            family = "spotify"
        else:
            family = "home_assistant"

        return {
            "entity_id": entity_id,
            "family": family,
            "platform": platform,
            "mass_player_type": mass_player_type,
            "attributes": attributes,
        }

    @staticmethod
    def _normalize_volume_value(raw_value: Any) -> float | None:
        """Normalize raw volume info from HA/MA attributes to 0.0-1.0 scale."""
        if raw_value is None:
            return None

        value: float
        if isinstance(raw_value, (int, float)):
            value = float(raw_value)
        elif isinstance(raw_value, str):
            raw_str = raw_value.strip()
            if not raw_str:
                return None
            try:
                value = float(raw_str)
            except ValueError:
                return None
        else:
            return None

        if value <= 0:
            return 0.0
        if value <= 1.0:
            return value

        # Music Assistant exposes volumes as 1-100 for announcement services per HA docs.
        if value <= 100.0:
            return max(0.0, min(value / 100.0, 1.0))

        return 1.0

    async def _async_determine_mass_announce_volume(
        self,
        media_player: str,
        player_profile: Dict[str, Any] | None,
    ) -> int | None:
        """Return the current volume in MA percentage scale (1-100) if known."""
        volume_normalized: float | None = None
        volume_source = "state"

        state = self.hass.states.get(media_player)
        if state and state.attributes:
            for key in ("volume_level", "volume", "announce_volume"):
                volume_normalized = self._normalize_volume_value(state.attributes.get(key))
                if volume_normalized is not None:
                    break

        if volume_normalized is None and player_profile:
            attrs = player_profile.get("attributes") or {}
            volume_source = "profile"
            for key in ("volume_level", "volume", "announce_volume"):
                volume_normalized = self._normalize_volume_value(attrs.get(key))
                if volume_normalized is not None:
                    break

        if volume_normalized is None:
            mass_volume = await self._async_lookup_mass_volume(media_player)
            if mass_volume is not None:
                volume_normalized = mass_volume
                volume_source = "music_assistant"

        if volume_normalized is None:
            _LOGGER.debug(
                "MediaHandler: MA player %s lacks volume info even after MA lookup; leaving announce_volume unset",
                media_player,
            )
            return None

        percentage = int(round(volume_normalized * 100))
        if percentage <= 0:
            _LOGGER.debug(
                "MediaHandler: MA player %s volume (%s source) rounded below 1%%; using minimum",
                media_player,
                volume_source,
            )
            percentage = 1

        if percentage > 100:
            percentage = 100

        _LOGGER.debug(
            "MediaHandler: MA player %s using announce_volume=%s from %s",
            media_player,
            percentage,
            volume_source,
        )

        return percentage

    def _resolve_media(
        self,
        sound_media,
        is_alarm: bool,
        player_profile: Dict[str, Any] | None,
    ) -> tuple[str, str]:
        """Resolve media identifier and type from descriptor or fallback."""
        default_id = self.alarm_sound if is_alarm else self.reminder_sound
        default_type = "music"
        family = (player_profile or {}).get("family", "home_assistant")
        entity_id = (player_profile or {}).get("entity_id")

        if isinstance(sound_media, dict):
            resolved = sound_media.get("resolved_url")
            original = sound_media.get("original_id")
            media_type = sound_media.get("content_type") or default_type

            selected: str | None = None

            if family == "music_assistant":
                if isinstance(original, str) and original:
                    selected = original
                elif isinstance(resolved, str) and resolved:
                    selected = resolved
            else:
                if isinstance(original, str) and original:
                    selected = original
                elif isinstance(resolved, str) and resolved:
                    selected = resolved

            if selected:
                _LOGGER.debug(
                    "MediaHandler: selected media %s for %s (family=%s)",
                    self._redact_media_url(selected),
                    entity_id or "unknown-player",
                    family,
                )
                return selected, media_type
        elif sound_media:
            return str(sound_media), default_type

        return default_id, default_type

    async def play_on_media_player(
        self,
        media_player: str,
        message: str,
        is_alarm: bool,
        sound_media=None,
        spotify_source: str | None = None,
        stop_event: asyncio.Event | None = None,
        register_context: Callable[[Context, str], None] | None = None,
        *,
        item_id: str | None = None,
        volume: float | None = None,
    ) -> bool:
        """Play TTS and sound on media player and return whether sound playback started."""
        playback_monitor: _PlaybackWatcher | None = None
        media_monitor: _PlaybackWatcher | None = None
        sound_started = False
        try:
            if 'player_profile' not in locals():
                player_profile = self._get_media_player_profile(media_player)
                player_family = player_profile.get("family", "home_assistant")
                is_spotify_player = player_family == "spotify"
            should_pre_stop = player_family not in {"spotify", "music_assistant"}
            if should_pre_stop:
                await self.stop_media_player(media_player, register_context)
            _LOGGER.debug(
                "MediaHandler: starting TTS on %s with message=%r (is_alarm=%s)",
                media_player,
                message,
                is_alarm,
            )
            announce_volume_override: int | None = None
            normalized_volume_override: float | None = None
            pending_spotify_volume: float | None = None
            if volume is not None:
                try:
                    normalized_volume_override = max(0.0, min(1.0, float(volume)))
                except (TypeError, ValueError):
                    normalized_volume_override = None
            if normalized_volume_override is not None and item_id:
                if player_family == "music_assistant":
                    announce_volume_override = await self._async_apply_volume_override(
                        item_id,
                        media_player,
                        normalized_volume_override,
                        player_profile,
                        register_context,
                    )
                elif is_spotify_player:
                    pending_spotify_volume = normalized_volume_override
                else:
                    await self._async_apply_volume_override(
                        item_id,
                        media_player,
                        normalized_volume_override,
                        player_profile,
                        register_context,
                    )

            tts_context: Context | None = None
            monitor_state: str | None = None
            monitor_attrs: Dict[str, Any] | None = None
            tts_entity = self._ensure_tts_entity()
            attempted_tts = False
            if is_spotify_player:
                if message:
                    _LOGGER.debug(
                        "MediaHandler: Spotify player %s does not support TTS; skipping announcement.",
                        media_player,
                    )
            elif tts_entity and message:
                tts_context = Context()
                if register_context:
                    register_context(tts_context, "tts")
                playback_monitor = _PlaybackWatcher(
                    self.hass,
                    media_player,
                    tts_context,
                    stop_event,
                    "tts",
                )
                if player_family == "music_assistant":
                    _LOGGER.debug(
                        "MediaHandler: attempting Music Assistant announcement on %s",
                        media_player,
                    )
                    attempted_tts = await self._play_tts_via_music_assistant(
                        media_player,
                        tts_entity,
                        message,
                        tts_context,
                        stop_event,
                        player_profile,
                        announce_volume_override=announce_volume_override,
                    )
                    if not attempted_tts:
                        _LOGGER.debug(
                            "MediaHandler: fallback to tts.speak for %s after Music Assistant attempt",
                            media_player,
                        )
                        attempted_tts = await self._play_tts_via_service(
                            media_player,
                            tts_entity,
                            message,
                            tts_context,
                            stop_event,
                        )
                else:
                    attempted_tts = await self._play_tts_via_service(
                        media_player,
                        tts_entity,
                        message,
                        tts_context,
                        stop_event,
                    )
            elif not tts_entity or not message:
                _LOGGER.debug(
                    "MediaHandler: skipping TTS on %s (no TTS entity available)",
                    media_player,
                )

            if attempted_tts and stop_event and stop_event.is_set():
                return sound_started

            playback_started = False
            playback_latency: float | None = None
            if attempted_tts:
                if playback_monitor:
                    wait_started = perf_counter()
                    playback_started = await playback_monitor.wait_started(timeout=4.0)
                    playback_latency = perf_counter() - wait_started
                    monitor_state = playback_monitor.last_state
                    monitor_attrs = (
                        dict(playback_monitor.last_attrs)
                        if playback_monitor.last_attrs is not None
                        else None
                    )
                else:
                    state = self.hass.states.get(media_player)
                    if state:
                        monitor_state = state.state
                        monitor_attrs = dict(state.attributes or {})
                if stop_event and stop_event.is_set():
                    return sound_started
            else:
                _LOGGER.debug(
                    "MediaHandler: no TTS dispatched on %s; proceeding directly to sound",
                    media_player,
                )

            # Once playback has started, wait for it to complete before playing the sound
            if playback_started:
                if playback_latency is not None:
                    _LOGGER.debug(
                        "MediaHandler: TTS playback on %s started after %.3fs (state=%s, attrs=%s)",
                        media_player,
                        playback_latency,
                        monitor_state or "<unknown>",
                        monitor_attrs or {},
                    )
                for _ in range(160):
                    if stop_event and stop_event.is_set():
                        return sound_started
                    state = self.hass.states.get(media_player)
                    if not state or state.state not in ("playing", "buffering"):
                        break
                    await asyncio.sleep(0.25)
            elif attempted_tts:
                _LOGGER.debug(
                    "MediaHandler: TTS on %s never entered playing state; proceeding to sound (last_state=%s, attrs=%s)",
                    media_player,
                    monitor_state or "<unknown>",
                    monitor_attrs or {},
                )

            # Ensure the player is idle before starting looped media even if TTS left it paused.
            if player_family not in {"spotify", "music_assistant"}:
                await self.stop_media_player(media_player, register_context)

            if is_spotify_player:
                if not spotify_source:
                    _LOGGER.error(
                        "MediaHandler: spotify_source missing for player %s; cannot start playback.",
                        media_player,
                    )
                    return sound_started
                try:
                    await self._ensure_spotify_source_selected(
                        media_player,
                        spotify_source,
                        register_context,
                    )
                    if pending_spotify_volume is not None and item_id:
                        await self._async_apply_volume_override(
                            item_id,
                            media_player,
                            pending_spotify_volume,
                            player_profile,
                            register_context,
                        )
                except Exception:
                    return sound_started

            media_source, media_type = self._resolve_media(
                sound_media,
                is_alarm,
                player_profile,
            )
            _LOGGER.debug(
                "MediaHandler: playing sound %s on %s",
                media_source,
                media_player,
            )
            play_context = Context(parent_id=tts_context.id if tts_context else None)
            if register_context:
                register_context(play_context, "media")
            play_service_data = {
                "entity_id": media_player,
                "media_content_id": media_source,
                "media_content_type": media_type,
            }

            if player_family == "music_assistant":
                play_service_data["announce"] = False

            media_monitor = _PlaybackWatcher(
                self.hass,
                media_player,
                play_context,
                stop_event,
                "media",
            )
            await self.hass.services.async_call(
                "media_player",
                "play_media",
                play_service_data,
                blocking=True,
                context=play_context,
            )

            if stop_event:
                sound_latency: float | None = None
                sound_state: str | None = None
                sound_attrs: Dict[str, Any] | None = None
                if media_monitor:
                    wait_started = perf_counter()
                    sound_started = await media_monitor.wait_started(timeout=10.0)
                    sound_latency = perf_counter() - wait_started
                    sound_state = media_monitor.last_state
                    sound_attrs = (
                        dict(media_monitor.last_attrs)
                        if media_monitor.last_attrs is not None
                        else None
                    )
                else:
                    state = self.hass.states.get(media_player)
                    if state:
                        sound_state = state.state
                        sound_attrs = dict(state.attributes or {})

                if not sound_started:
                    state = self.hass.states.get(media_player)
                    if state and state.state in ("playing", "buffering"):
                        sound_started = True
                        sound_state = state.state
                        sound_attrs = dict(state.attributes or {})

                if sound_started:
                    if sound_latency is not None:
                        _LOGGER.debug(
                            "MediaHandler: media playback on %s started after %.3fs (state=%s, attrs=%s)",
                            media_player,
                            sound_latency,
                            sound_state or "<unknown>",
                            sound_attrs or {},
                        )
                    while not stop_event.is_set():
                        state = self.hass.states.get(media_player)
                        if not state or state.state not in ("playing", "buffering"):
                            break
                        await asyncio.sleep(0.5)
                else:
                    _LOGGER.debug(
                        "MediaHandler: sound on %s never entered playing/buffering within 10s after play_media (last_state=%s, attrs=%s)",
                        media_player,
                        sound_state or "<unknown>",
                        sound_attrs or {},
                    )

        except Exception as err:
            _LOGGER.error("Error playing on media player %s: %s", media_player, err)
        finally:
            if playback_monitor:
                playback_monitor.close()
            if media_monitor:
                media_monitor.close()
        return sound_started

    def _ensure_tts_entity(self) -> str | None:
        """Find or cache a TTS entity to use for announcements."""
        if self._tts_entity and self.hass.states.get(self._tts_entity):
            return self._tts_entity

        entity_ids = self.hass.states.async_entity_ids("tts")
        if entity_ids:
            self._tts_entity = entity_ids[0]
            _LOGGER.debug("MediaHandler: using TTS entity %s", self._tts_entity)
            return self._tts_entity

        if not self._logged_missing_tts:
            _LOGGER.warning(
                "No TTS entities are available; media_player alarms will skip spoken announcements"
            )
            self._logged_missing_tts = True
        return None

    async def _generate_tts_media(
        self, tts_entity: str, message: str
    ) -> tuple[str, str] | None:
        """Generate a TTS media URL and type for the given message."""
        try:
            engine = async_resolve_engine(self.hass, tts_entity)
        except HomeAssistantError as err:
            _LOGGER.error("MediaHandler: failed to resolve TTS engine for %s: %s", tts_entity, err)
            return None

        if not engine:
            _LOGGER.debug("MediaHandler: no TTS engine resolved for %s", tts_entity)
            return None

        try:
            stream = async_create_stream(self.hass, engine)
        except HomeAssistantError as err:
            _LOGGER.error("MediaHandler: unable to create TTS stream for %s: %s", engine, err)
            return None

        stream.async_set_message(message)

        try:
            resolved = await media_source.async_resolve_media(
                self.hass, stream.media_source_id, None
            )
        except MediaSourceError as err:
            _LOGGER.error("MediaHandler: failed to resolve TTS media for %s: %s", engine, err)
            return None

        media_url = getattr(resolved, "url", None)
        media_type = getattr(resolved, "mime_type", None) or getattr(
            resolved, "media_content_type", None
        )

        if media_url:
            parsed = urlparse(str(media_url))
            if parsed.scheme.lower() not in ("http", "https"):
                if str(media_url).startswith("/"):
                    try:
                        base_url = get_url(self.hass)
                    except HomeAssistantError as err:
                        fallback = (
                            getattr(self.hass.config, "external_url", None)
                            or getattr(self.hass.config, "internal_url", None)
                            or getattr(getattr(self.hass.config, "api", None), "base_url", None)
                        )
                        if not fallback:
                            _LOGGER.error(
                                "MediaHandler: cannot determine base URL for TTS stream %s: %s",
                                engine,
                                err,
                            )
                            media_url = None
                        else:
                            media_url = urljoin(str(fallback), str(media_url))
                            parsed = urlparse(str(media_url))
                    else:
                        media_url = urljoin(base_url, str(media_url))
                        parsed = urlparse(str(media_url))
                else:
                    _LOGGER.debug(
                        "MediaHandler: TTS media for %s has unsupported scheme %s",
                        engine,
                        parsed.scheme or "<empty>",
                    )
                    media_url = None

            if media_url and parsed.scheme.lower() in ("http", "https") and not media_type:
                media_type = "audio/mpeg"

        # Music Assistant announcement API requires an absolute HTTP(S) URL.
        if not media_url:
            _LOGGER.error(
                "MediaHandler: resolved TTS media missing URL for engine %s", engine
            )
            return None

        _LOGGER.debug(
            "MediaHandler: generated TTS media url=%s type=%s for engine %s",
            self._redact_media_url(media_url),
            media_type or "audio/mpeg",
            engine,
        )

        return media_url, media_type or "audio/mpeg"

    async def _play_tts_via_music_assistant(
        self,
        media_player: str,
        tts_entity: str,
        message: str,
        context: Context,
        stop_event: asyncio.Event | None,
        player_profile: Dict[str, Any],
        announce_volume_override: int | None = None,
    ) -> bool:
        """Prefer the Music Assistant announcement service for MA players."""
        if stop_event and stop_event.is_set():
            return False

        media_info = await self._generate_tts_media(tts_entity, message)
        if not media_info:
            return False

        media_url, media_type = media_info
        redacted_url = self._redact_media_url(media_url)

        payload: Dict[str, Any] = {
            "entity_id": media_player,
            "url": media_url,
            "use_pre_announce": False,
        }

        if announce_volume_override is not None:
            payload["announce_volume"] = announce_volume_override
        else:
            announce_volume = await self._async_determine_mass_announce_volume(
                media_player,
                player_profile,
            )
            if announce_volume is not None:
                payload["announce_volume"] = announce_volume

        _LOGGER.debug(
            "MediaHandler: music_assistant.play_announcement payload for %s url=%s type=%s pre=%s volume=%s",
            media_player,
            redacted_url,
            media_type,
            payload.get("use_pre_announce"),
            payload.get("announce_volume", "<default>"),
        )

        try:
            await self.hass.services.async_call(
                "music_assistant",
                "play_announcement",
                payload,
                blocking=True,
                context=context,
            )
            _LOGGER.debug(
                "MediaHandler: dispatched music_assistant.play_announcement for %s",
                media_player,
            )
            return True
        except HomeAssistantError as err:
            _LOGGER.error(
                "MediaHandler: music_assistant announcement failed for %s (url=%s): %s",
                media_player,
                redacted_url,
                err,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "MediaHandler: unexpected MA announcement error for %s (url=%s): %s",
                media_player,
                redacted_url,
                err,
            )

        return False

    async def _async_lookup_mass_volume(self, media_player: str) -> float | None:
        """Query Music Assistant directly for a player's volume level."""
        if get_music_assistant_client is None:
            return None

        registry = er.async_get(self.hass)
        reg_entry = registry.async_get(media_player)
        if not reg_entry or not reg_entry.unique_id:
            return None

        player_id = reg_entry.unique_id
        entries = self.hass.config_entries.async_entries("music_assistant")
        for entry in entries:
            try:
                mass_client = get_music_assistant_client(self.hass, entry.entry_id)
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "MediaHandler: unable to obtain MA client for entry %s: %s",
                    entry.entry_id,
                    err,
                )
                continue

            try:
                player = mass_client.players[player_id]
            except KeyError:
                continue
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "MediaHandler: MA player lookup failed for %s via %s: %s",
                    media_player,
                    entry.entry_id,
                    err,
                )
                continue

            volume = getattr(player, "volume_level", None)
            if volume is None and hasattr(player, "group_volume"):
                volume = getattr(player, "group_volume")

            normalized = self._normalize_volume_value(volume)
            if normalized is not None:
                return normalized

        return None

    def _read_volume_from_state(self, media_player: str) -> float | None:
        """Read current volume from HA state attributes if available."""
        state = self.hass.states.get(media_player)
        if not state or not state.attributes:
            return None
        for key in ("volume_level", "volume", "announce_volume"):
            normalized = self._normalize_volume_value(state.attributes.get(key))
            if normalized is not None:
                return normalized
        return None

    async def _async_wake_player(self, media_player: str, register_context: Callable[[Context, str], None] | None) -> bool:
        """Turn on a media player so it exposes volume attributes."""
        state = self.hass.states.get(media_player)
        if state and state.state not in ("off", "standby", "unavailable"):
            return True

        wake_context = Context()
        if register_context:
            register_context(wake_context, "turn_on")

        try:
            await self.hass.services.async_call(
                "media_player",
                "turn_on",
                {"entity_id": media_player},
                blocking=True,
                context=wake_context,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("MediaHandler: failed to wake %s for volume snapshot: %s", media_player, err)
            return False

        for _ in range(10):
            await asyncio.sleep(0.3)
            state = self.hass.states.get(media_player)
            if state and state.state not in ("off", "standby", "unavailable"):
                return True

        return False

    async def _async_get_player_volume(
        self,
        media_player: str,
        *,
        player_profile: Dict[str, Any] | None,
        wake_if_off: bool,
        register_context: Callable[[Context, str], None] | None,
    ) -> float | None:
        """Return best-known normalized volume for a player."""
        volume = self._read_volume_from_state(media_player)
        if volume is None and player_profile and player_profile.get("family") == "music_assistant":
            volume = await self._async_lookup_mass_volume(media_player)

        if volume is None and wake_if_off:
            woke = await self._async_wake_player(media_player, register_context)
            if woke:
                volume = self._read_volume_from_state(media_player)
                if volume is None and player_profile and player_profile.get("family") == "music_assistant":
                    volume = await self._async_lookup_mass_volume(media_player)
        return volume

    async def _async_set_player_volume(
        self,
        media_player: str,
        target_volume: float,
        register_context: Callable[[Context, str], None] | None,
        *,
        restoring: bool = False,
    ) -> None:
        """Set the HA media_player volume to the requested level."""
        try:
            normalized = max(0.0, min(1.0, float(target_volume)))
        except (TypeError, ValueError):
            _LOGGER.debug("MediaHandler: invalid target volume %s for %s", target_volume, media_player)
            return

        context = Context()
        if register_context:
            register_context(context, "volume" if not restoring else "volume_restore")

        try:
            await self.hass.services.async_call(
                "media_player",
                "volume_set",
                {"entity_id": media_player, "volume_level": normalized},
                blocking=True,
                context=context,
            )
        except HomeAssistantError as err:
            _LOGGER.error("MediaHandler: volume_set failed for %s: %s", media_player, err)
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("MediaHandler: unexpected error setting volume on %s: %s", media_player, err)

    async def _async_apply_volume_override(
        self,
        item_id: str,
        media_player: str,
        target_volume: float,
        player_profile: Dict[str, Any] | None,
        register_context: Callable[[Context, str], None] | None,
    ) -> int | None:
        """Capture current volume, apply override, and return MA announce volume if needed."""
        stack = self._player_volume_stack.get(media_player)
        entry = None
        if stack:
            for existing in stack:
                if existing["item_id"] == item_id:
                    entry = existing
                    break

        if entry is None:
            snapshot = await self._async_get_player_volume(
                media_player,
                player_profile=player_profile,
                wake_if_off=True,
                register_context=register_context,
            )
            if snapshot is not None:
                entry = {"item_id": item_id, "volume": snapshot, "restore_ready": False}
                self._player_volume_stack.setdefault(media_player, []).append(entry)

        await self._async_set_player_volume(
            media_player,
            target_volume,
            register_context,
        )

        if player_profile and player_profile.get("family") == "music_assistant":
            percentage = int(round(max(0.0, min(1.0, target_volume)) * 100))
            return max(1, min(100, percentage))
        return None

    async def restore_player_volume(
        self,
        item_id: str,
        media_player: str | None,
        register_context: Callable[[Context, str], None] | None = None,
    ) -> None:
        """Restore a player's original volume when an item stops."""
        if not media_player:
            return
        stack = self._player_volume_stack.get(media_player)
        if not stack:
            return

        for entry in stack:
            if entry["item_id"] == item_id:
                entry["restore_ready"] = True
                break
        else:
            return

        await self._async_process_volume_stack(media_player, register_context)

    async def _async_process_volume_stack(
        self,
        media_player: str,
        register_context: Callable[[Context, str], None] | None,
    ) -> None:
        """Pop and restore stacked volume overrides when possible."""
        stack = self._player_volume_stack.get(media_player)
        while stack and stack[-1].get("restore_ready"):
            entry = stack.pop()
            volume_value = entry.get("volume")
            if volume_value is not None:
                await self._async_set_player_volume(
                    media_player,
                    volume_value,
                    register_context,
                    restoring=True,
                )
        if stack is not None and not stack:
            self._player_volume_stack.pop(media_player, None)

    async def _play_tts_via_service(
        self,
        media_player: str,
        tts_entity: str,
        message: str,
        context: Context,
        stop_event: asyncio.Event | None,
    ) -> bool:
        """Fallback to the built-in tts.speak service."""
        if stop_event and stop_event.is_set():
            return False

        try:
            await self.hass.services.async_call(
                "tts",
                "speak",
                {
                    "entity_id": tts_entity,
                    "media_player_entity_id": media_player,
                    "message": message,
                },
                blocking=True,
                context=context,
            )
            _LOGGER.debug(
                "MediaHandler: dispatched tts.speak for %s via %s",
                media_player,
                tts_entity,
            )
            return True
        except HomeAssistantError as err:
            _LOGGER.error(
                "MediaHandler: tts.speak call failed for %s via %s: %s",
                media_player,
                tts_entity,
                err,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "MediaHandler: unexpected error calling tts.speak for %s: %s",
                media_player,
                err,
            )
        return False

    async def _ensure_spotify_source_selected(
        self,
        media_player: str,
        source: str,
        register_context: Callable[[Context, str], None] | None = None,
    ) -> None:
        """Switch the Spotify player to the requested source if needed."""
        source_normalized = str(source).strip()
        if not source_normalized:
            raise HomeAssistantError("Spotify source name must be provided")

        state = self.hass.states.get(media_player)
        current_source = None
        if state and state.attributes:
            raw_source = state.attributes.get("source")
            if isinstance(raw_source, str):
                current_source = raw_source.strip()

        if current_source == source_normalized:
            _LOGGER.debug(
                "MediaHandler: Spotify player %s already using source %s",
                media_player,
                source_normalized,
            )
            return

        select_context = Context()
        if register_context:
            register_context(select_context, "media")

        try:
            await self.hass.services.async_call(
                "media_player",
                "select_source",
                {"entity_id": media_player, "source": source_normalized},
                blocking=True,
                context=select_context,
            )
            _LOGGER.debug(
                "MediaHandler: selected Spotify source %s on %s",
                source_normalized,
                media_player,
            )
        except HomeAssistantError as err:
            _LOGGER.error(
                "MediaHandler: select_source failed for %s on %s: %s",
                source_normalized,
                media_player,
                err,
            )
            raise
        except Exception as err:
            _LOGGER.error(
                "MediaHandler: unexpected error selecting source %s on %s: %s",
                source_normalized,
                media_player,
                err,
            )
            raise HomeAssistantError(
                f"Failed to select Spotify source {source_normalized} on {media_player}: {err}"
            ) from err

    async def stop_media_player(
        self,
        media_player: Optional[str],
        register_context: Callable[[Context, str], None] | None = None,
    ) -> None:
        """Send stop command to the media player."""
        if not media_player:
            return
        profile = self._get_media_player_profile(media_player)
        family = (profile or {}).get("family")
        prefer_pause = family == "spotify"
        stop_context = Context()
        if register_context:
            register_context(stop_context, "stop")
        service_calls: list[str] = []
        if prefer_pause:
            service_calls.append("media_pause")
        service_calls.append("media_stop")

        last_error: Exception | None = None
        for service in service_calls:
            try:
                await self.hass.services.async_call(
                    "media_player",
                    service,
                    {"entity_id": media_player},
                    blocking=True,
                    context=stop_context,
                )
                return
            except Exception as err:  # noqa: BLE001
                last_error = err
                if service == "media_pause":
                    _LOGGER.debug(
                        "MediaHandler: media_pause failed on %s (%s); falling back to media_stop",
                        media_player,
                        err,
                    )
                    continue
                _LOGGER.error("Error stopping media player %s using %s: %s", media_player, service, err)
