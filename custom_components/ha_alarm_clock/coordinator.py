from __future__ import annotations
"""Coordinator for scheduling alarms and reminders."""
import logging
import re
import unicodedata
from typing import Dict, Any, Callable, Optional, Iterable
from datetime import datetime, timedelta, time as dt_time
import time
import asyncio
import contextlib
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote

from homeassistant.core import HomeAssistant, ServiceCall, callback, Context
from homeassistant.helpers.event import async_track_point_in_time, async_track_state_change_event
from homeassistant.const import EVENT_CALL_SERVICE
from homeassistant.util import dt as dt_util
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.network import get_url
import voluptuous as vol
from homeassistant.components import media_source
from homeassistant.components.media_source import MediaSourceError
from homeassistant.components.jellyfin.const import DOMAIN as JELLYFIN_DOMAIN
from homeassistant.components.plex.const import DOMAIN as PLEX_DOMAIN
from homeassistant.exceptions import HomeAssistantError

from .const import (
    DOMAIN,
    DEFAULT_SNOOZE_MINUTES,
    DEFAULT_NAME,
    DEFAULT_ALARM_SOUND,
    DEFAULT_REMINDER_SOUND,
    DEFAULT_ACTIVE_PRESS_MODE,
    DEFAULT_RESTRICT_NON_ADMIN_OWNERSHIP,
    ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE,
    ACTIVE_PRESS_MODE_SHORT_SNOOZE_LONG_STOP,
    ACTIVE_RESUME_GRACE_MINUTES,
    ALARM_ENTITY_DOMAIN,
    REMINDER_ENTITY_DOMAIN,
    DASHBOARD_ENTITY_ID,
    ATTR_SPOTIFY_SOURCE,
    ATTR_VOLUME,
    ATTR_USER_ID,
    ATTR_USER_NAME,
    SPOTIFY_PLATFORMS,
)

from .storage import AlarmReminderStorage

_LOGGER = logging.getLogger(__name__)

_DLNA_HASH_ID_PATTERN = re.compile(r"^:[0-9a-f]{32}$", re.IGNORECASE)
_MIN_ANNOUNCEMENT_INTERVAL_SECONDS = 30.0

__all__ = ["AlarmAndReminderCoordinator"]


WEEKDAY_NAME_TO_INDEX = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "weds": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}
WEEKDAY_INDEX_TO_NAME = [
    "mon",
    "tue",
    "wed",
    "thu",
    "fri",
    "sat",
    "sun",
]
ALL_WEEKDAYS = {0, 1, 2, 3, 4, 5, 6}

MEDIA_SOURCE_PREFIX = "media-source://"
LOCAL_MEDIA_PREFIX = "/media/"
LOCAL_STATIC_PREFIX = "/local/"
MUSIC_ASSISTANT_URI_SCHEMES = {
    "mass",
    "ma",
    "library",
    "radio",
    "database",
    "provider",
    "spotify",
    "tidal",
    "ytmusic",
    "qobuz",
    "deezer",
}


class _PlaybackSession:
    """Manage playback loop for a single alarm/reminder."""

    def __init__(self, coordinator: "AlarmAndReminderCoordinator", item_id: str, stop_event: asyncio.Event):
        self.coordinator = coordinator
        self.hass = coordinator.hass
        self.item_id = item_id
        self.stop_event = stop_event
        self._media_listener_remove = None
        self._media_state_listener_remove = None
        self._last_target: str | None = None
        self._context_index: dict[str, tuple[float, str]] = {}
        self._context_ttl_seconds = 120.0
        self._media_request_active = False
        self._media_started = False
        self._media_started_at: float | None = None
        self._tts_active = False
        self._ma_tts_stop_scheduled = False
        self._manual_stop_dispatched = False

    async def run(self) -> None:
        item = self.coordinator._active_items.get(self.item_id)
        if not item:
            return
        _LOGGER.debug("[%s] Starting playback session run.", self.item_id)
        await self._setup_listeners(item)
        try:
            while not self.stop_event.is_set():
                _LOGGER.debug("[%s] Loop start: stop_event is %s", self.item_id, self.stop_event.is_set())
                current = self.coordinator._active_items.get(self.item_id)
                if not current or not current.get("enabled", True):
                    _LOGGER.debug("[%s] Breaking loop: item not found or disabled", self.item_id)
                    break
                target = self._resolve_target(current)
                if not target:
                    _LOGGER.debug("[%s] Breaking loop: no target found", self.item_id)
                    break
                cycle_started_at = time.monotonic()
                sound_started = await self._run_cycle(current, target)
                if self.stop_event.is_set():
                    _LOGGER.debug("[%s] Breaking loop: stop_event was set during cycle", self.item_id)
                    break
                if not sound_started:
                    elapsed = time.monotonic() - cycle_started_at
                    remaining = max(0.0, _MIN_ANNOUNCEMENT_INTERVAL_SECONDS - elapsed)
                    if remaining > 0:
                        _LOGGER.debug(
                            "[%s] Sound never started; waiting %.2fs before next announcement",
                            self.item_id,
                            remaining,
                        )
                        with contextlib.suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(self.stop_event.wait(), timeout=remaining)
        finally:
            _LOGGER.debug("[%s] Playback loop finished. Cleaning up.", self.item_id)
            await self._cleanup()

    async def stop(self, reason: str = "stopped") -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
        current = self.coordinator._active_items.get(self.item_id)
        is_alarm = bool(current.get("is_alarm", True)) if current else True
        target = self._resolve_target(current) if current else None
        if target:
            await self.coordinator.media_handler.stop_media_player(
                target,
                register_context=self._register_service_context,
            )

    async def _run_cycle(self, item: Dict[str, Any], target: str) -> bool:
        message = self.coordinator._build_announcement_text(item)
        sound_media = item.get("sound_media")
        fallback_sound = item.get("sound_file")
        playback_media = sound_media if sound_media else fallback_sound
        is_alarm = item.get("is_alarm", False)
        self._last_target = target
        self._prepare_new_cycle()
        _LOGGER.debug(
            "[%s] Dispatching playback cycle to %s with announcement=%r",
            self.item_id,
            target,
            message,
        )
        return await self.coordinator.media_handler.play_on_media_player(
            target,
            message,
            is_alarm,
            sound_media=playback_media,
            spotify_source=item.get(ATTR_SPOTIFY_SOURCE),
            stop_event=self.stop_event,
            register_context=self._register_service_context,
            item_id=self.item_id,
            volume=item.get(ATTR_VOLUME),
        )

    def _resolve_target(self, item: Dict[str, Any]) -> str | None:
        media_player = item.get("media_player")
        if media_player:
            return media_player
        return self.coordinator.get_default_media_player()

    async def _setup_listeners(self, item: Dict[str, Any]) -> None:
        target = self._resolve_target(item)
        if not target:
            return
        self._setup_media_listener(target)

    def _setup_media_listener(self, media_player: str) -> None:
        def _media_service_listener(event):
            if self.stop_event.is_set():
                return
            if event.data.get("domain") != "media_player":
                return
            service = event.data.get("service")
            if service not in {"media_stop", "turn_off", "media_pause"}:
                return
            service_data = event.data.get("service_data") or {}
            entity_id = service_data.get("entity_id")
            if not entity_id:
                return
            if isinstance(entity_id, list):
                match = media_player in entity_id
            else:
                match = entity_id == media_player
            if not match:
                return
            if self._is_owned_context(event.context):
                _LOGGER.debug(
                    "[%s] Ignoring service event %s.%s triggered by session context",
                    self.item_id,
                    event.data.get("domain"),
                    event.data.get("service"),
                )
                return
            self.hass.loop.call_soon_threadsafe(self.stop_event.set)

        self._media_listener_remove = self.hass.bus.async_listen(
            EVENT_CALL_SERVICE, _media_service_listener
        )
        self._media_state_listener_remove = async_track_state_change_event(
            self.hass,
            [media_player],
            self._handle_media_state_change,
        )

    def _register_service_context(self, context: Context | None, purpose: str) -> None:
        if context is None:
            return
        now = time.monotonic()
        normalized = purpose.lower() if purpose else "unknown"
        self._context_index[context.id] = (now, normalized)
        if context.parent_id:
            self._context_index[context.parent_id] = (now, normalized)
        if normalized == "media":
            self._media_request_active = True
            self._media_started = False
        self._prune_context_ids(now=now)

    def _prune_context_ids(self, *, now: float | None = None) -> None:
        if now is None:
            now = time.monotonic()
        cutoff = now - self._context_ttl_seconds
        stale_ids = [ctx_id for ctx_id, (ts, _) in self._context_index.items() if ts < cutoff]
        for ctx_id in stale_ids:
            self._context_index.pop(ctx_id, None)

    def _clear_contexts_by_purpose(self, purpose: str) -> None:
        to_remove = [ctx_id for ctx_id, (_, stored) in self._context_index.items() if stored == purpose]
        for ctx_id in to_remove:
            self._context_index.pop(ctx_id, None)

    def _context_matches_purpose(self, context: Context | None, purpose: str) -> bool:
        if context is None:
            return False
        for ctx_id in (context.id, context.parent_id):
            if not ctx_id:
                continue
            record = self._context_index.get(ctx_id)
            if record and record[1] == purpose:
                return True
        return False

    def _prepare_new_cycle(self) -> None:
        self._clear_contexts_by_purpose("tts")
        self._clear_contexts_by_purpose("media")
        self._media_request_active = False
        self._media_started = False
        self._media_started_at = None
        self._tts_active = False
        self._ma_tts_stop_scheduled = False
        self._manual_stop_dispatched = False

    def _is_owned_context(self, context: Context | None) -> bool:
        if context is None:
            return False
        for ctx_id in (context.id, context.parent_id):
            if ctx_id and ctx_id in self._context_index:
                return True
        return False

    async def _handle_media_state_change(self, event) -> None:
        if self.stop_event.is_set():
            return
        new_state = event.data.get("new_state")
        if not new_state:
            return
        self._log_media_state_debug("new_state", new_state)
        self._prune_context_ids()

        state = new_state.state
        old_state = event.data.get("old_state")
        self._log_media_state_debug("old_state", old_state)
        if state in {"playing", "buffering"}:
            if self._context_matches_purpose(new_state.context, "tts"):
                self._tts_active = True
                return
            if self._context_matches_purpose(new_state.context, "media") or self._media_request_active:
                self._media_started = True
                self._media_request_active = False
                self._media_started_at = time.monotonic()
            return

        if state not in {"idle", "off", "standby", "paused"}:
            return

        if self._context_matches_purpose(new_state.context, "stop"):
            _LOGGER.debug(
                "[%s] Ignoring state change triggered by integration stop command (state=%s)",
                self.item_id,
                state,
            )
            return

        if self._context_matches_purpose(new_state.context, "tts"):
            if state in {"idle", "off", "standby"}:
                self._tts_active = False
                return
            return

        idle_reason_raw = (new_state.attributes or {}).get("media_idle_reason")
        idle_reason = idle_reason_raw.upper() if isinstance(idle_reason_raw, str) else None
        manual_hint = False
        manual_reason = None
        if idle_reason:
            if idle_reason in {"STOPPED", "CANCELLED", "INTERRUPTED", "ERROR", "USER_STOPPED", "PLAYER_ERROR"}:
                manual_hint = True
                manual_reason = f"idle_reason={idle_reason}"
            elif idle_reason in {"FINISHED", "END_OF_MEDIA"}:
                manual_hint = False

        context = new_state.context
        if not manual_hint and context and context.user_id:
            manual_hint = True
            manual_reason = f"context_user={context.user_id}"

        tts_candidate = self._tts_active and not self._media_started

        if not manual_hint and tts_candidate:
            if state == "paused":
                manual_hint = True
                manual_reason = "paused_tts_no_context"
            elif state in {"idle", "off", "standby"}:
                self._tts_active = False
                return

        if not manual_hint and state == "paused" and self._media_started:
            if self._player_family_is_spotify() and self._looks_like_track_completion(old_state, new_state):
                _LOGGER.debug(
                    "[%s] Spotify playback reached track end; treating pause as natural completion",
                    self.item_id,
                )
            else:
                manual_hint = True
                manual_reason = "paused_media_no_context"

        if not manual_hint and self._media_started:
            manual_hint = self._infer_manual_stop_from_state(
                old_state,
                new_state,
            )
            if manual_hint:
                manual_reason = "position_delta"

        # if not manual_hint and self._media_started:
        #     elapsed = self._elapsed_media_playback()
        #     if elapsed is not None:
        #         expected_floor = self._expected_playback_floor(new_state)
        #         if elapsed < expected_floor:
        #             manual_hint = True
        #             manual_reason = f"elapsed={elapsed:.2f}s<floor={expected_floor:.2f}s"

        if not manual_hint and self._is_owned_context(new_state.context):
            _LOGGER.debug(
                "[%s] Ignoring state change triggered by session context (state=%s)",
                self.item_id,
                state,
            )
            return

        if manual_hint and manual_reason == "paused_tts_no_context":
            if self._player_family_is_music_assistant():
                self._schedule_music_assistant_tts_stop()

        if manual_hint:
            _LOGGER.debug(
                "[%s] Detected external stop for %s (state=%s idle_reason=%s reason=%s)",
                self.item_id,
                new_state.entity_id,
                state,
                idle_reason,
                manual_reason,
            )
            if not self._manual_stop_dispatched:
                self._manual_stop_dispatched = True
                item = self.coordinator._active_items.get(self.item_id)
                is_alarm = bool(item.get("is_alarm", True)) if item else True
                stop_reason = manual_reason or "stopped"
                self.hass.async_create_task(
                    self.coordinator.stop_item(
                        self.item_id,
                        is_alarm,
                        reason=stop_reason,
                    )
                )
            self._media_started = False
            self._media_request_active = False
            self._media_started_at = None
            self._tts_active = False
            self.stop_event.set()
        elif self._media_started:
            # Natural completion of alarm sound
            self._media_started = False
            self._media_request_active = False
            self._media_started_at = None
            _LOGGER.debug(
                "[%s] Media player state=%s idle_reason=%s treated as natural completion",
                self.item_id,
                state,
                idle_reason,
            )

    def _player_family_is_music_assistant(self) -> bool:
        target = self._last_target
        if not target:
            current = self.coordinator._active_items.get(self.item_id)
            if current:
                target = self._resolve_target(current)
        if not target:
            return False
        profile = self.coordinator.get_media_player_profile(target)
        return (profile or {}).get("family") == "music_assistant"

    def _player_family_is_spotify(self) -> bool:
        target = self._last_target
        if not target:
            current = self.coordinator._active_items.get(self.item_id)
            if current:
                target = self._resolve_target(current)
        if not target:
            return False
        profile = self.coordinator.get_media_player_profile(target)
        return (profile or {}).get("family") == "spotify"

    def _schedule_music_assistant_tts_stop(self) -> None:
        if self._ma_tts_stop_scheduled:
            return
        item = self.coordinator._active_items.get(self.item_id)
        if not item:
            return
        is_alarm = bool(item.get("is_alarm", True))
        self._ma_tts_stop_scheduled = True

        def _run_stop():
            self.hass.async_create_task(
                self.coordinator.stop_item(self.item_id, is_alarm, reason="stopped")
            )

        self.hass.loop.call_soon_threadsafe(_run_stop)

    def _log_media_state_debug(self, label: str, state) -> None:
        if not _LOGGER.isEnabledFor(logging.DEBUG):
            return
        if state is None:
            _LOGGER.debug("[%s] %s: <None>", self.item_id, label)
            return
        attrs = state.attributes or {}
        context = state.context
        _LOGGER.debug(
            "[%s] %s: entity=%s state=%s idle_reason=%s media_position=%s media_duration=%s content_id=%s ctx_id=%s ctx_parent=%s ctx_user=%s",
            self.item_id,
            label,
            state.entity_id,
            state.state,
            attrs.get("media_idle_reason"),
            attrs.get("media_position"),
            attrs.get("media_duration"),
            attrs.get("media_content_id"),
            context.id if context else None,
            context.parent_id if context else None,
            context.user_id if context else None,
        )

    def _infer_manual_stop_from_state(self, old_state, new_state, *, assume_tts: bool = False) -> bool:
        if not old_state or old_state.state not in {"playing", "buffering"}:
            return False

        duration = self._safe_to_float(
            old_state.attributes.get("media_duration") if old_state.attributes else None
        )
        if duration is None and new_state and new_state.attributes:
            duration = self._safe_to_float(new_state.attributes.get("media_duration"))

        position = self._safe_to_float(
            old_state.attributes.get("media_position") if old_state.attributes else None
        )

        if duration and position is not None and position > 0 and duration > 0:
            remaining = duration - position
            min_seconds = 1.0 if assume_tts else 3.0
            threshold = max(min_seconds, duration * 0.15)
            if remaining > threshold:
                return True

        return False

    def _looks_like_track_completion(self, old_state, new_state) -> bool:
        if not old_state or old_state.state not in {"playing", "buffering"}:
            return False
        duration = self._safe_to_float(
            (old_state.attributes or {}).get("media_duration") if old_state.attributes else None
        )
        if duration is None or duration <= 0:
            duration = self._safe_to_float(
                (new_state.attributes or {}).get("media_duration") if new_state and new_state.attributes else None
            )
        if duration is None or duration <= 0:
            return False

        position = self._safe_to_float(
            (old_state.attributes or {}).get("media_position") if old_state.attributes else None
        )
        if position is None:
            position = self._safe_to_float(
                (new_state.attributes or {}).get("media_position") if new_state and new_state.attributes else None
            )
        if position is None:
            return False

        remaining = duration - position
        tolerance = max(0.75, duration * 0.2)
        return remaining <= tolerance

    def _elapsed_media_playback(self) -> float | None:
        if self._media_started_at is None:
            return None
        return time.monotonic() - self._media_started_at

    def _expected_playback_floor(self, state) -> float:
        duration = None
        if state and state.attributes:
            duration = self._safe_to_float(state.attributes.get("media_duration"))
        if duration and duration > 0:
            # Require roughly half the track or all but five seconds (whichever is smaller), but never below 3s.
            near_end = duration - 5.0 if duration > 5.0 else duration * 0.8
            fraction = duration * 0.5
            candidate = min(near_end, fraction)
            return max(3.0, candidate)
        # Fallback when duration not available: expect at least a few seconds of playback.
        return 4.0

    @staticmethod
    def _safe_to_float(value) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _cleanup(self) -> None:
        if self._media_listener_remove:
            self._media_listener_remove()
            self._media_listener_remove = None
        if self._media_state_listener_remove:
            self._media_state_listener_remove()
            self._media_state_listener_remove = None
        self._context_index.clear()
        self._media_started = False
        self._media_request_active = False
        self._last_target = None
        self._tts_active = False
class AlarmAndReminderCoordinator:
    """Coordinates scheduling of alarms and reminders."""

    def __init__(self, hass: HomeAssistant, media_handler):
        """Initialize coordinator."""
        self.hass = hass
        self.media_handler = media_handler
        self._active_items: Dict[str, Dict[str, Any]] = {}
        self._stop_events: Dict[str, asyncio.Event] = {}
        self._scheduled_callbacks: Dict[str, Callable[[], None]] = {}
        self._playback_tasks: Dict[str, asyncio.Task] = {}
        self._playback_sessions: Dict[str, _PlaybackSession] = {}
        self._last_alarm_time: Optional[datetime] = None
        self.async_add_entities = None
        self._alarm_counter = 0
        self._reminder_counter = 0
        self.storage = AlarmReminderStorage(hass)
        self._default_media_player: str | None = None
        self._hidden_media_players: set[str] = set()
        self._allowed_activation_entities: set[str] | None = None
        self._default_snooze_minutes: int = DEFAULT_SNOOZE_MINUTES
        self._active_press_mode: str = DEFAULT_ACTIVE_PRESS_MODE
        self._restrict_non_admin_ownership: bool = DEFAULT_RESTRICT_NON_ADMIN_OWNERSHIP
        _LOGGER.debug("New coordinator instance created: %s", id(self))
        
        # Remove legacy entities (pre-refactor namespace) if present to avoid duplication
        hass.states.async_remove(f"{DOMAIN}.items")
        for state in list(hass.states.async_all()):
            if state.entity_id.startswith(f"{DOMAIN}."):
                hass.states.async_remove(state.entity_id)
        
        # Ensure domain data structure exists
        if DOMAIN not in self.hass.data:
            self.hass.data[DOMAIN] = {}
        
        # Initialize entities list if not exists
        for config_entry in self.hass.config_entries.async_entries(DOMAIN):
            if config_entry.entry_id not in self.hass.data[DOMAIN]:
                self.hass.data[DOMAIN][config_entry.entry_id] = {}
            if "entities" not in self.hass.data[DOMAIN][config_entry.entry_id]:
                self.hass.data[DOMAIN][config_entry.entry_id]["entities"] = []

        # Add these new methods
        self._used_alarm_ids = set()  # Track used alarm IDs
        self._used_reminder_ids = set()  # Track used reminder IDs

        # Notification action mapping: listen once globally and dispatch by tag
        self._notification_listener = hass.bus.async_listen(
            "mobile_app_notification_action", self._on_mobile_notification_action
        )
        self._notification_tag_map: Dict[str, str] = {}  # tag -> item_id

        # Cache media-player profiles so we can tailor playback behavior per platform.
        self._media_player_profile_cache: Dict[str, Dict[str, Any]] = {}
        self._cached_base_url: str | None = None
        self._resolved_media_metadata_cache: dict[str, tuple[float, Dict[str, Any]]] = {}

        # Allow the media handler to reuse our player classification logic.
        if hasattr(self.media_handler, "set_media_player_profile_resolver"):
            self.media_handler.set_media_player_profile_resolver(
                self.get_media_player_profile
            )

    def set_default_media_player(self, entity_id: str | None) -> None:
        """Store the default media player used when none is provided."""
        if entity_id:
            try:
                entity_id = cv.entity_id(entity_id)
            except vol.Invalid:
                _LOGGER.warning("Ignored invalid default media player: %s", entity_id)
                entity_id = None
        self._default_media_player = entity_id

    def get_default_media_player(self) -> str | None:
        """Return the configured default media player, if any."""
        return self._default_media_player

    def set_hidden_media_players(self, entities: Iterable[str] | None) -> None:
        """Store the configured hidden media-player list."""
        hidden: set[str] = set()
        if entities is not None:
            for entity in entities:
                if not entity:
                    continue
                try:
                    normalized = cv.entity_id(str(entity))
                except vol.Invalid:
                    _LOGGER.warning(
                        "Ignoring invalid hidden media player '%s' in options.",
                        entity,
                    )
                    continue
                if self._default_media_player and normalized == self._default_media_player:
                    continue
                hidden.add(normalized)
        self._hidden_media_players = hidden

    def get_hidden_media_players(self) -> list[str]:
        """Return the configured hidden media-player list."""
        return sorted(self._hidden_media_players)

    def set_allowed_activation_entities(self, entities: Iterable[str] | None) -> None:
        """Store the configured activation entities allow list."""
        if entities is None:
            self._allowed_activation_entities = None
            return

        allowed: set[str] = set()
        for entity in entities:
            if not entity:
                continue
            try:
                allowed.add(cv.entity_id(str(entity)))
            except vol.Invalid:
                _LOGGER.warning(
                    "Ignoring invalid activation entity '%s' in options.",
                    entity,
                )
        self._allowed_activation_entities = allowed

    def set_default_snooze_minutes(self, minutes: int | None) -> None:
        """Set the default snooze duration used when not provided explicitly."""
        value = minutes
        if value is None:
            value = DEFAULT_SNOOZE_MINUTES
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = DEFAULT_SNOOZE_MINUTES
        if value <= 0:
            value = DEFAULT_SNOOZE_MINUTES
        self._default_snooze_minutes = value

    def get_default_snooze_minutes(self) -> int:
        """Return the configured default snooze duration."""
        return self._default_snooze_minutes

    def set_active_press_mode(self, mode: str | None) -> None:
        """Set how the companion card interprets short vs long presses."""
        valid_modes = {
            ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE,
            ACTIVE_PRESS_MODE_SHORT_SNOOZE_LONG_STOP,
        }
        if mode not in valid_modes:
            mode = DEFAULT_ACTIVE_PRESS_MODE
        self._active_press_mode = mode

    def get_active_press_mode(self) -> str:
        """Return the configured active press mode."""
        return self._active_press_mode

    def set_restrict_non_admin_ownership(self, enabled: bool | None) -> None:
        """Set whether companion cards restrict non-admin users to owned items."""
        self._restrict_non_admin_ownership = bool(enabled)

    def get_restrict_non_admin_ownership(self) -> bool:
        """Return whether companion cards should restrict non-admin ownership views."""
        return self._restrict_non_admin_ownership

    async def _prepare_sound_descriptor(self, raw_media, *, is_alarm: bool) -> Dict[str, Any]:
        """Convert raw service input into a normalized media descriptor."""
        if raw_media in (None, "", False):
            return await self._default_sound_descriptor(is_alarm)

        friendly_title: str | None = None
        metadata = None
        media_browser_path: list[dict[str, str]] | None = None
        if isinstance(raw_media, dict):
            content_id = raw_media.get("media_content_id") or raw_media.get("media_id")
            content_type = raw_media.get("media_content_type")
            metadata = raw_media.get("metadata") if isinstance(raw_media.get("metadata"), dict) else None
            friendly_title = (
                raw_media.get("media_content_title")
                or raw_media.get("title")
                or (metadata.get("title") if metadata else None)
                or (metadata.get("name") if metadata else None)
            )
            media_browser_path = self._normalize_media_browser_path_input(raw_media.get("media_browser_path"))
            descriptor = await self._build_descriptor_from_content_id(
                content_id,
                content_type,
                is_alarm=is_alarm,
                title=friendly_title,
            )
        else:
            descriptor = await self._build_descriptor_from_content_id(
                str(raw_media),
                None,
                is_alarm=is_alarm,
                title=friendly_title,
            )

        if media_browser_path:
            descriptor["media_browser_path"] = media_browser_path
        return descriptor

    async def _default_sound_descriptor(self, is_alarm: bool) -> Dict[str, Any]:
        """Return descriptor for integration defaults."""
        default_source = DEFAULT_ALARM_SOUND if is_alarm else DEFAULT_REMINDER_SOUND
        return await self._build_descriptor_from_content_id(
            default_source,
            "music",
            is_alarm=is_alarm,
            title=self._friendly_media_title(default_source),
        )

    def _get_base_url(self) -> str | None:
        """Return a fully qualified base URL for resolving relative media paths."""
        if self._cached_base_url:
            return self._cached_base_url

        try:
            base = get_url(self.hass)
            if base:
                self._cached_base_url = str(base)
                return self._cached_base_url
        except HomeAssistantError as err:
            _LOGGER.debug("Coordinator: get_url failed to resolve base URL: %s", err)

        config = self.hass.config
        for attr in ("external_url", "internal_url"):
            candidate = getattr(config, attr, None)
            if candidate:
                self._cached_base_url = str(candidate)
                return self._cached_base_url

        api = getattr(config, "api", None)
        base = getattr(api, "base_url", None) if api else None
        if base:
            self._cached_base_url = str(base)
            return self._cached_base_url

        return None

    @staticmethod
    def _redact_media_url(url: str | None) -> str:
        """Mask sensitive tokens in logged URLs."""
        if not url:
            return "<none>"
        try:
            parsed = urlparse(str(url))
        except Exception:  # noqa: BLE001
            return "<invalid>"
        if parsed.scheme in {"http", "https"} and parsed.query:
            sanitized = parsed._replace(query="***")
            return sanitized.geturl()
        return str(url)

    async def _build_descriptor_from_content_id(
        self,
        content_id: str | None,
        content_type: str | None,
        *,
        is_alarm: bool,
        title: str | None = None,
    ) -> Dict[str, Any]:
        if not content_id:
            return await self._default_sound_descriptor(is_alarm)

        content_id = self._normalize_local_media_input(content_id)

        resolved_url: str | None = None
        kind = "direct"
        friendly_title = title
        metadata_title = friendly_title.strip() if isinstance(friendly_title, str) and friendly_title.strip() else None
        try:
            if media_source.is_media_source_id(content_id):
                try:
                    media = await media_source.async_resolve_media(self.hass, content_id, None)
                except MediaSourceError as err:
                    raise ValueError(f"Unable to resolve media source '{content_id}': {err}") from err
                resolved_url = getattr(media, "url", None)
                mime_type = getattr(media, "mime_type", None)
                if mime_type:
                    content_type = content_type or mime_type
                kind = "media_source"
                friendly_title = friendly_title or getattr(media, "title", None)
                if metadata_title is None and isinstance(friendly_title, str) and friendly_title.strip():
                    metadata_title = friendly_title.strip()
            else:
                resolved_url = content_id
                parsed = urlparse(content_id)
                scheme = parsed.scheme.lower()
                if scheme in ("http", "https"):
                    media_source_id = self._http_local_to_media_source_id(content_id)
                    if media_source_id:
                        _LOGGER.debug(
                            "Converted local HTTP media %s to media source %s",
                            self._redact_media_url(content_id),
                            media_source_id,
                        )
                        return await self._build_descriptor_from_content_id(
                            media_source_id,
                            content_type,
                            is_alarm=is_alarm,
                        )
                    kind = "external_url"
                elif scheme and scheme in MUSIC_ASSISTANT_URI_SCHEMES:
                    kind = "music_assistant"
                elif content_id.startswith(LOCAL_MEDIA_PREFIX) or content_id.startswith(LOCAL_STATIC_PREFIX) or not scheme:
                    kind = "file"
                else:
                    kind = "unknown"
        except Exception:
            _LOGGER.exception("Error resolving media content id %s", content_id)
            raise

        candidate_url = resolved_url or content_id
        duration = await self._probe_media_duration(candidate_url)
        if not friendly_title:
            friendly_title = self._friendly_media_title(content_id) or self._friendly_media_title(candidate_url)
        descriptor = {
            "kind": kind,
            "original_id": content_id,
            "resolved_url": candidate_url,
            "content_type": content_type or "music",
            "duration": duration,
            "media_content_id": content_id,
            "media_content_type": content_type or "music",
        }
        if isinstance(friendly_title, str):
            normalized_title = friendly_title.strip()
        else:
            normalized_title = None
        if normalized_title:
            descriptor["media_content_title"] = self._resolve_media_title(
                normalized_title,
                metadata_title=metadata_title,
                content_id=content_id,
                resolved_url=candidate_url,
            )
        return descriptor

    def _normalize_local_media_input(self, content_id: str) -> str:
        """Normalize user-supplied local media identifiers."""
        if not content_id:
            return content_id

        if content_id.startswith("media/"):
            content_id = f"/{content_id}"
        elif content_id.startswith("local/"):
            content_id = f"/{content_id}"

        if content_id.startswith(LOCAL_MEDIA_PREFIX):
            remainder = content_id[len(LOCAL_MEDIA_PREFIX):].strip("/")
            if not remainder or "/" not in remainder:
                raise ValueError(
                    "Local media paths must look like /media/<share>/<file>. Select the file via the media browser."
                )
        elif content_id.startswith(LOCAL_STATIC_PREFIX):
            remainder = content_id[len(LOCAL_STATIC_PREFIX):].strip("/")
            if not remainder:
                raise ValueError(
                    "Local media paths must look like /local/<file>. Select the file via the media browser."
                )

        return content_id

    @staticmethod
    def _normalize_media_browser_path_input(path) -> list[dict[str, str]]:
        """Validate and normalize breadcrumb paths supplied by the UI."""
        if isinstance(path, (str, bytes)) or not isinstance(path, Iterable):
            return []
        normalized: list[dict[str, str]] = []
        seen: set[str] = set()
        for entry in path:
            entry_id: str | None = None
            entry_type: str | None = None
            if isinstance(entry, dict):
                candidate_id = entry.get("id")
                entry_id = candidate_id if isinstance(candidate_id, str) else None
                candidate_type = entry.get("type")
                entry_type = candidate_type if isinstance(candidate_type, str) else None
            elif isinstance(entry, (list, tuple)) and entry:
                candidate_id = entry[0]
                entry_id = candidate_id if isinstance(candidate_id, str) else None
                if len(entry) > 1 and isinstance(entry[1], str):
                    entry_type = entry[1]
            elif isinstance(entry, str):
                entry_id = entry
            if not entry_id:
                continue
            trimmed = entry_id.strip()
            if not trimmed:
                continue
            normalized_id = trimmed
            if trimmed.startswith(MEDIA_SOURCE_PREFIX):
                normalized_id = trimmed
            if normalized_id in seen:
                continue
            record: dict[str, str] = {"id": normalized_id}
            if entry_type:
                type_trimmed = entry_type.strip()
                if type_trimmed:
                    record["type"] = type_trimmed
            normalized.append(record)
            seen.add(normalized_id)
        return normalized

    @staticmethod
    def _friendly_media_title(candidate: str | None) -> str | None:
        """Generate a friendly title from a raw media identifier."""
        if not candidate or not isinstance(candidate, str):
            return None
        working = candidate
        # Remove scheme prefixes
        if "://" in working:
            working = working.split("://", 1)[1]
        # Trim query/fragment
        for sep in ("?", "#"):
            if sep in working:
                working = working.split(sep, 1)[0]
        # Keep only final segment
        working = working.rstrip("/").split("/")[-1]
        working = working.split("\\")[-1]
        if "." in working:
            working = working.rsplit(".", 1)[0]
        working = working.strip()
        return working or None

    def _resolve_media_title(
        self,
        title: str,
        *,
        metadata_title: str | None,
        content_id: str | None,
        resolved_url: str | None,
    ) -> str:
        """Return the preferred title for a media descriptor."""
        trimmed = title.strip()
        if not trimmed:
            return title

        if metadata_title:
            meta_trimmed = metadata_title.strip()
            if meta_trimmed:
                reference_tokens = {
                    token.strip().lower()
                    for token in (
                        self._friendly_media_title(content_id),
                        self._friendly_media_title(resolved_url),
                    )
                    if token
                }
                meta_lower = meta_trimmed.lower()
                meta_no_ext = meta_lower.rsplit(".", 1)[0] if "." in meta_lower else meta_lower
                if meta_lower in reference_tokens or meta_no_ext in reference_tokens:
                    return self._friendly_media_title(meta_trimmed) or meta_trimmed
                return meta_trimmed

        return self._friendly_media_title(trimmed) or trimmed

    def _http_local_to_media_source_id(self, url: str) -> str | None:
        """Convert a Home Assistant-served HTTP URL into a media-source identifier."""
        path = self._map_url_to_local_path(url)
        if not path:
            return None

        media_root = Path(self.hass.config.path("media"))
        static_root = Path(self.hass.config.path("www"))

        try:
            relative = path.relative_to(media_root)
            return f"{MEDIA_SOURCE_PREFIX}media_source/{relative.as_posix()}"
        except ValueError:
            pass

        try:
            relative = path.relative_to(static_root)
            return f"{MEDIA_SOURCE_PREFIX}media_source/local/{relative.as_posix()}"
        except ValueError:
            pass

        return None

    def _classify_media_descriptor(self, descriptor: Dict[str, Any]) -> str:
        """Categorize the descriptor to drive compatibility handling."""
        if not isinstance(descriptor, dict):
            return "unknown"

        candidates: list[str] = []
        resolved = descriptor.get("resolved_url")
        original = descriptor.get("original_id")
        if isinstance(resolved, str) and resolved:
            candidates.append(resolved)
        if isinstance(original, str) and original:
            candidates.append(original)

        for candidate in candidates:
            if candidate.startswith(MEDIA_SOURCE_PREFIX):
                return "ha_media_source"
            if candidate.startswith(LOCAL_MEDIA_PREFIX) or candidate.startswith(LOCAL_STATIC_PREFIX):
                return "local_path"
            scheme = urlparse(candidate).scheme.lower()
            if scheme == "spotify":
                return "spotify_uri"
            if scheme in ("http", "https"):
                return "http"
            if scheme.startswith("spotify--"):
                return "music_assistant"
            if scheme in MUSIC_ASSISTANT_URI_SCHEMES:
                return "music_assistant"
            if scheme:
                return "other_uri"
        return "unknown"

    @staticmethod
    def _coerce_media_source_id(candidate: str | None) -> str | None:
        """Map legacy local paths to media-source identifiers."""
        if not candidate:
            return None
        if candidate.startswith(MEDIA_SOURCE_PREFIX):
            return candidate
        if candidate.startswith(LOCAL_MEDIA_PREFIX):
            rel_path = candidate[len(LOCAL_MEDIA_PREFIX):].lstrip("/")
            if rel_path:
                return f"{MEDIA_SOURCE_PREFIX}media_source/{rel_path}"
        if candidate.startswith(LOCAL_STATIC_PREFIX):
            rel_path = candidate[len(LOCAL_STATIC_PREFIX):].lstrip("/")
            if rel_path:
                return f"{MEDIA_SOURCE_PREFIX}media_source/local/{rel_path}"
        return None

    async def _ensure_streamable_local_media(
        self,
        descriptor: Dict[str, Any],
        media_player: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve local media paths to signed URLs when possible."""
        if not isinstance(descriptor, dict):
            return descriptor

        for key in ("resolved_url", "original_id"):
            candidate = descriptor.get(key)
            if not isinstance(candidate, str) or not candidate:
                continue
            media_source_id = self._coerce_media_source_id(candidate)
            if not media_source_id:
                continue
            try:
                media = await media_source.async_resolve_media(
                    self.hass,
                    media_source_id,
                    media_player,
                )
            except MediaSourceError as err:
                _LOGGER.debug(
                    "Failed to resolve media source '%s' for local media normalisation: %s",
                    media_source_id,
                    err,
                )
                continue

            resolved_url = getattr(media, "url", None)
            mime_type = getattr(media, "mime_type", None)

            normalized = dict(descriptor)
            normalized["kind"] = "media_source"
            normalized["original_id"] = media_source_id
            if resolved_url:
                if isinstance(resolved_url, str) and resolved_url.startswith("/"):
                    base_url = self._get_base_url()
                    if base_url:
                        resolved_url = urljoin(base_url, resolved_url)
                        _LOGGER.debug(
                            "Normalized local media %s to absolute URL %s",
                            media_source_id,
                            resolved_url,
                        )
                    else:
                        _LOGGER.warning(
                            "Unable to resolve base URL for local media %s; Music Assistant players may fail to play it.",
                            media_source_id,
                        )
                normalized["resolved_url"] = resolved_url
            if mime_type and not normalized.get("content_type"):
                normalized["content_type"] = mime_type
            return normalized

        return descriptor

    async def _ensure_media_player_media_compatibility(
        self,
        media_player: Optional[str],
        descriptor: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate and adjust media descriptor for the selected media player."""
        if not isinstance(descriptor, dict):
            return descriptor

        normalized = dict(descriptor)
        classification = self._classify_media_descriptor(normalized)

        if classification in {"local_path", "ha_media_source"}:
            normalized = await self._ensure_streamable_local_media(normalized, media_player)
            classification = self._classify_media_descriptor(normalized)

        if not media_player:
            return normalized

        profile = self.get_media_player_profile(media_player)
        family = profile.get("family", "unknown")

        media_display = normalized.get("original_id") or normalized.get("resolved_url") or "<unknown>"
        redacted_url = self._redact_media_url(normalized.get("resolved_url"))
        _LOGGER.debug(
            "Checking media compatibility: player=%s family=%s media=%s resolved=%s classification=%s",
            media_player,
            family,
            media_display,
            redacted_url,
            classification,
        )

        if family == "spotify":
            if classification != "spotify_uri":
                raise ValueError(
                    (
                        f"Media '{media_display}' is not a Spotify URI. Select music from the Spotify media browser "
                        f"when using Spotify player {media_player}."
                    )
                )
            return normalized

        if family == "home_assistant":
            if classification == "music_assistant":
                raise ValueError(
                    f"Media '{media_display}' is only supported by Music Assistant players and cannot be used with {media_player}."
                )
            return normalized

        if family == "music_assistant":
            if classification in {"music_assistant", "http"}:
                return normalized
            if classification in {"ha_media_source", "local_path"}:
                normalized = await self._ensure_streamable_local_media(normalized, media_player)
                classification = self._classify_media_descriptor(normalized)
                if classification == "http":
                    return normalized
                raise ValueError(
                    f"Media '{media_display}' must resolve to an accessible URL before it can be played on Music Assistant player {media_player}."
                )
            if classification == "other_uri":
                raise ValueError(
                    f"Media '{media_display}' uses an unsupported URI scheme for Music Assistant player {media_player}."
                )
            return normalized

        # Unknown family: still try to provide best-effort normalisation.
        if classification in {"local_path", "ha_media_source"}:
            normalized = await self._ensure_streamable_local_media(normalized, media_player)
            _LOGGER.debug(
                "Post-normalization descriptor for %s: resolved=%s",
                media_display,
                self._redact_media_url(normalized.get("resolved_url")),
            )
        return normalized

    def _select_media_identifier_for_player(
        self,
        descriptor: Dict[str, Any],
        media_player: Optional[str],
    ) -> Optional[str]:
        """Choose the best media identifier for the given player family."""
        if not isinstance(descriptor, dict):
            return None

        profile = self.get_media_player_profile(media_player) if media_player else None
        family = (profile or {}).get("family", "home_assistant")
        resolved = descriptor.get("resolved_url")
        original = descriptor.get("original_id")

        if family == "spotify":
            if isinstance(descriptor.get("media_content_id"), str) and descriptor["media_content_id"]:
                return descriptor["media_content_id"]
            if isinstance(original, str) and original:
                return original
            return resolved if isinstance(resolved, str) else None

        if family == "music_assistant":
            if isinstance(original, str) and original:
                scheme = urlparse(original).scheme.lower()
                if original.startswith(MEDIA_SOURCE_PREFIX) or (
                    scheme and scheme not in {"http", "https"}
                ):
                    return original
            if isinstance(resolved, str) and resolved:
                scheme = urlparse(resolved).scheme.lower()
                if scheme in {"http", "https"}:
                    return resolved
            if isinstance(original, str):
                return original
            return resolved if isinstance(resolved, str) else None

        if isinstance(resolved, str) and resolved:
            return resolved
        return original if isinstance(original, str) else None

    def _map_url_to_local_path(self, url: str | None) -> Path | None:
        if not url:
            return None
        candidate = url.split("?", 1)[0]
        if candidate.startswith("media-source://media_source/local/"):
            rel = candidate[len("media-source://media_source/local/"):].lstrip("/")
            return Path(self.hass.config.path("media", rel))
        if candidate.startswith("/media/"):
            rel = candidate.lstrip("/")
            return Path(self.hass.config.path(rel))
        if candidate.startswith("/local/"):
            rel = candidate[len("/local/"):]
            return Path(self.hass.config.path("www", rel))
        parsed = urlparse(candidate)
        if not parsed.scheme and not candidate.startswith("//"):
            return Path(self.hass.config.path(candidate))
        return None

    async def _probe_media_duration(self, url: str | None) -> float | None:
        path = self._map_url_to_local_path(url)
        if not path:
            return None
        return await self.hass.async_add_executor_job(self._read_duration_with_mutagen, path)

    @staticmethod
    def _read_duration_with_mutagen(path: Path) -> float | None:
        try:
            from mutagen import File as MutagenFile  # type: ignore
        except ImportError:
            return None

        try:
            if not path.exists() or not path.is_file():
                return None
            audio = MutagenFile(path)
        except Exception:
            return None

        info = getattr(audio, "info", None)
        if not info or not getattr(info, "length", None):
            return None
        try:
            return float(info.length)
        except (TypeError, ValueError):
            return None

    async def async_resolve_media_metadata(
        self,
        media_content_id: str,
        media_content_type: str | None = None,
        provider_hint: str | None = None,
    ) -> Dict[str, Any]:
        """Resolve extended metadata for supported media providers."""
        if not media_content_id:
            raise HomeAssistantError("Missing media_content_id")

        provider = self._normalize_media_provider(provider_hint) or self._detect_media_provider(media_content_id)
        if provider == "plex":
            return await self.async_resolve_plex_media_metadata(media_content_id, media_content_type)
        if provider == "dlna_dms":
            return await self.async_resolve_dlna_media_metadata(media_content_id, media_content_type)
        if provider == "jellyfin":
            return await self.async_resolve_jellyfin_media_metadata(media_content_id, media_content_type)

        raise HomeAssistantError("Unsupported media provider for metadata resolution")

    async def async_resolve_plex_media_metadata(
        self,
        media_content_id: str,
        media_content_type: str | None = None,
    ) -> Dict[str, Any]:
        """Resolve additional Plex metadata for a media-source identifier."""
        _LOGGER.debug(
            "Plex resolver request: media_content_id=%s media_content_type=%s",
            media_content_id,
            media_content_type,
        )
        if not media_content_id:
            raise HomeAssistantError("Missing media_content_id")

        try:
            server_id, item_key = self._parse_plex_media_source_id(media_content_id)
        except ValueError as err:
            raise HomeAssistantError(str(err)) from err
        _LOGGER.debug(
            "Parsed Plex media-source id -> server_id=%s item_key=%s",
            server_id,
            item_key,
        )

        now = time.monotonic()
        cache_key = f"plex:{server_id}:{item_key}"
        cached = self._resolved_media_metadata_cache.get(cache_key)
        if cached and now - cached[0] < 300:
            _LOGGER.debug(
                "Plex metadata cache hit for %s (age=%.1fs)",
                cache_key,
                now - cached[0],
            )
            return cached[1]
        _LOGGER.debug("Plex metadata cache miss for %s", cache_key)

        plex_domain_data = self.hass.data.get(PLEX_DOMAIN)
        if not plex_domain_data:
            raise HomeAssistantError("Plex integration is not configured")

        servers = plex_domain_data.get("servers")
        if not servers:
            raise HomeAssistantError("No Plex servers available")

        plex_server = servers.get(server_id)
        if plex_server is None:
            raise HomeAssistantError("Requested Plex server is not available")

        try:
            _LOGGER.debug(
                "Fetching Plex metadata from server=%s item_key=%s",
                server_id,
                item_key,
            )
            plex_item = await self.hass.async_add_executor_job(
                plex_server.library.fetchItem,
                item_key,
            )
            plex_item_data = await self.hass.async_add_executor_job(
                self._snapshot_plex_item_attributes,
                plex_item,
            )
            _LOGGER.debug(
                "Fetched Plex item: type=%s title=%s parent=%s grandparent=%s",
                plex_item_data.get("type"),
                plex_item_data.get("title"),
                plex_item_data.get("parentTitle"),
                plex_item_data.get("grandparentTitle"),
            )
        except Exception as err:  # noqa: BLE001
            raise HomeAssistantError("Unable to fetch Plex metadata") from err

        metadata = self._normalize_plex_metadata(
            plex_server,
            plex_item_data,
            media_content_id,
            media_content_type,
        )
        _LOGGER.debug(
            "Normalized Plex metadata: %s",
            self._summarize_media_metadata(metadata),
        )
        self._resolved_media_metadata_cache[cache_key] = (now, metadata)
        self._prune_resolved_media_metadata_cache(now)
        return metadata

    async def async_resolve_dlna_media_metadata(
        self,
        media_content_id: str,
        media_content_type: str | None = None,
    ) -> Dict[str, Any]:
        """Resolve metadata for DLNA media-source identifiers."""
        _LOGGER.debug(
            "DLNA resolver request: media_content_id=%s media_content_type=%s",
            media_content_id,
            media_content_type,
        )
        if not media_source.is_media_source_id(media_content_id):
            raise HomeAssistantError("DLNA metadata resolution requires a media-source identifier")

        parsed = urlparse(media_content_id)
        if (parsed.netloc or "").lower() != "dlna_dms":
            raise HomeAssistantError("Media identifier is not a DLNA media-source reference")

        now = time.monotonic()
        cache_key = f"dlna_dms:{media_content_id}"
        cached = self._resolved_media_metadata_cache.get(cache_key)
        if cached and now - cached[0] < 300:
            _LOGGER.debug(
                "DLNA metadata cache hit for %s (age=%.1fs)",
                cache_key,
                now - cached[0],
            )
            return cached[1]

        try:
            play_media = await media_source.async_resolve_media(self.hass, media_content_id, None)
        except MediaSourceError as err:
            raise HomeAssistantError(f"Unable to resolve DLNA media: {err}") from err

        browse_media = None
        try:
            browse_media = await media_source.async_browse_media(self.hass, media_content_id)
        except MediaSourceError:
            browse_media = None

        child_track_hints = None
        if self._dlna_browse_entry_looks_like_album(browse_media):
            try:
                child_track_hints = await self._fetch_dlna_child_track_hints(browse_media)
                if child_track_hints:
                    _LOGGER.debug(
                        "DLNA child track hints extracted for %s: %s",
                        media_content_id,
                        child_track_hints,
                    )
            except HomeAssistantError as err:
                _LOGGER.debug("Unable to fetch DLNA child metadata for %s: %s", media_content_id, err)

        metadata = self._normalize_dlna_metadata(
            play_media,
            browse_media,
            media_content_id,
            media_content_type,
            child_track_hints,
        )
        _LOGGER.debug(
            "Normalized DLNA metadata: %s",
            self._summarize_media_metadata(metadata),
        )
        self._resolved_media_metadata_cache[cache_key] = (now, metadata)
        self._prune_resolved_media_metadata_cache(now)
        return metadata

    async def async_resolve_jellyfin_media_metadata(
        self,
        media_content_id: str,
        media_content_type: str | None = None,
    ) -> Dict[str, Any]:
        """Resolve metadata for Jellyfin media-source identifiers."""
        _LOGGER.debug(
            "Jellyfin resolver request: media_content_id=%s media_content_type=%s",
            media_content_id,
            media_content_type,
        )
        if not media_source.is_media_source_id(media_content_id):
            raise HomeAssistantError("Jellyfin metadata resolution requires a media-source identifier")

        parsed = urlparse(media_content_id)
        if (parsed.netloc or "").lower() != "jellyfin":
            raise HomeAssistantError("Media identifier is not a Jellyfin media-source reference")

        now = time.monotonic()
        cache_key = f"jellyfin:{media_content_id}"
        cached = self._resolved_media_metadata_cache.get(cache_key)
        if cached and now - cached[0] < 300:
            _LOGGER.debug(
                "Jellyfin metadata cache hit for %s (age=%.1fs)",
                cache_key,
                now - cached[0],
            )
            return cached[1]
        else:
            _LOGGER.debug("Jellyfin metadata cache miss for %s", cache_key)

        play_media = None
        try:
            play_media = await media_source.async_resolve_media(self.hass, media_content_id, None)
            _LOGGER.debug(
                "Jellyfin play media resolved: mime=%s title=%s",
                getattr(play_media, "mime_type", None),
                getattr(play_media, "title", None),
            )
        except MediaSourceError as err:
            _LOGGER.debug("Jellyfin play media resolve failed (media source error): %s", err)
        except HomeAssistantError as err:  # type: ignore[misc] - HA may raise its own error subclass
            _LOGGER.debug("Jellyfin play media resolve raised HomeAssistantError: %s", err)
        except Exception as err:  # noqa: BLE001 - instrumentation only
            _LOGGER.debug("Jellyfin play media resolve raised unexpected error: %s", err)

        try:
            browse_media = await media_source.async_browse_media(self.hass, media_content_id)
            _LOGGER.debug(
                "Jellyfin browse media resolved: class=%s title=%s children=%s",
                getattr(browse_media, "media_class", None),
                getattr(browse_media, "title", None),
                len(getattr(browse_media, "children", []) or []),
            )
        except MediaSourceError as err:
            browse_media = None
            _LOGGER.debug("Jellyfin browse media lookup failed (media source error): %s", err)
        except HomeAssistantError as err:  # type: ignore[misc]
            browse_media = None
            _LOGGER.debug("Jellyfin browse media lookup raised HomeAssistantError: %s", err)
        except Exception as err:  # noqa: BLE001
            browse_media = None
            _LOGGER.debug("Jellyfin browse media lookup raised unexpected error: %s", err)

        _LOGGER.debug("Jellyfin raw item fetch starting for %s", media_content_id)
        jellyfin_item = await self._async_fetch_jellyfin_item(media_content_id)
        if jellyfin_item is None:
            _LOGGER.debug("Jellyfin raw item fetch returned no data; falling back to media_source metadata")
        metadata = self._normalize_jellyfin_metadata(
            play_media,
            browse_media,
            media_content_id,
            media_content_type,
            raw_item=jellyfin_item,
        )
        _LOGGER.debug(
            "Normalized Jellyfin metadata: %s",
            self._summarize_media_metadata(metadata),
        )
        self._resolved_media_metadata_cache[cache_key] = (now, metadata)
        self._prune_resolved_media_metadata_cache(now)
        return metadata

    def _parse_plex_media_source_id(self, media_content_id: str) -> tuple[str, str]:
        parsed = urlparse(media_content_id)
        scheme = (parsed.scheme or "").lower()

        if scheme == "media-source" and parsed.netloc == "plex":
            path = parsed.path.lstrip("/")
            if not path or "/" not in path:
                raise ValueError("Plex media-source id is missing the item key")

            server_id, remainder = path.split("/", 1)
            if not server_id or not remainder:
                raise ValueError("Invalid Plex media-source identifier")

            item_key = "/" + remainder
            if parsed.query:
                item_key = f"{item_key}?{parsed.query}"

            server = unquote(server_id)
            key = unquote(item_key)
            _LOGGER.debug("Plex identifier parsed via media-source scheme: server=%s key=%s", server, key)
            return server, key

        if scheme == "plex":
            server_id = parsed.netloc
            if not server_id:
                raise ValueError("Plex identifier is missing the server id")

            remainder = parsed.path.lstrip("/")
            if not remainder:
                raise ValueError("Plex identifier is missing the item key")

            if not remainder.startswith("library/metadata/"):
                remainder = remainder.lstrip("/")
                remainder = f"library/metadata/{remainder}"

            item_key = "/" + remainder
            if parsed.query:
                item_key = f"{item_key}?{parsed.query}"

            server = unquote(server_id)
            key = unquote(item_key)
            _LOGGER.debug("Plex identifier parsed via legacy plex:// scheme: server=%s key=%s", server, key)
            return server, key

        raise ValueError("media_content_id is not a Plex media-source reference")

    def _parse_jellyfin_media_source_id(self, media_content_id: str) -> str:
        parsed = urlparse(media_content_id)
        scheme = (parsed.scheme or "").lower()
        if scheme != "media-source" or (parsed.netloc or "").lower() != "jellyfin":
            raise ValueError("media_content_id is not a Jellyfin media-source reference")
        identifier = parsed.path.lstrip("/")
        if not identifier:
            raise ValueError("Jellyfin media-source id is missing the item identifier")
        return unquote(identifier)

    async def _async_get_loaded_jellyfin_coordinator(self):
        config_entries = getattr(self.hass, "config_entries", None)
        if not config_entries:
            _LOGGER.debug("Jellyfin metadata: config_entries manager unavailable")
            return None
        try:
            entries = config_entries.async_entries(JELLYFIN_DOMAIN)
        except Exception as err:  # noqa: BLE001 - guard against config registry access issues
            _LOGGER.debug("Jellyfin metadata: failed to enumerate config entries: %s", err)
            return None
        for entry in entries:
            coordinator = getattr(entry, "runtime_data", None)
            if coordinator is not None:
                _LOGGER.debug("Jellyfin metadata: using coordinator from entry %s", entry.entry_id)
                return coordinator
        _LOGGER.debug("Jellyfin metadata: no loaded config entries with runtime_data")
        return None

    async def _async_fetch_jellyfin_item(self, media_content_id: str) -> Dict[str, Any] | None:
        try:
            item_id = self._parse_jellyfin_media_source_id(media_content_id)
        except ValueError:
            _LOGGER.debug("Jellyfin metadata: media_content_id %s was not a Jellyfin media-source", media_content_id)
            return None

        coordinator = await self._async_get_loaded_jellyfin_coordinator()
        if coordinator is None:
            _LOGGER.debug("No active Jellyfin coordinator available for metadata request")
            return None

        api_client = getattr(coordinator, "api_client", None)
        jellyfin_api = getattr(api_client, "jellyfin", None) if api_client else None
        if jellyfin_api is None:
            _LOGGER.debug("Jellyfin API client missing on coordinator for metadata request")
            return None

        def _get_item() -> Dict[str, Any] | None:
            return jellyfin_api.get_item(item_id)

        try:
            item = await self.hass.async_add_executor_job(_get_item)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Failed to fetch Jellyfin metadata for %s: %s", item_id, err)
            return None

        if not isinstance(item, dict):
            _LOGGER.debug("Jellyfin metadata response for %s was not a mapping", item_id)
            return None
        _LOGGER.debug(
            "Fetched Jellyfin metadata for %s: name=%s type=%s album=%s artist=%s",
            item_id,
            item.get("Name"),
            item.get("Type"),
            item.get("Album"),
            (item.get("AlbumArtists") or item.get("Artists")),
        )
        return item

    def _normalize_plex_metadata(
        self,
        plex_server,
        plex_item_data: Dict[str, Any],
        media_content_id: str,
        media_content_type: str | None,
    ) -> Dict[str, Any]:
        plex_type = plex_item_data.get("type") or media_content_type or "audio"
        title = plex_item_data.get("title")
        grandparent = plex_item_data.get("grandparentTitle")
        parent_title = plex_item_data.get("parentTitle")
        original_title = plex_item_data.get("originalTitle")
        summary = plex_item_data.get("summary")
        artist = grandparent or original_title or parent_title
        album = parent_title if plex_type == "track" else None
        duration_ms = plex_item_data.get("duration")
        duration = int(round(duration_ms / 1000)) if isinstance(duration_ms, (int, float)) and duration_ms > 0 else None
        thumb = self._build_plex_thumb_url(
            plex_server,
            plex_item_data.get("thumb"),
            plex_item_data.get("thumbUrl"),
        )
        display_title = self._build_display_title(plex_type, title, artist, album)

        metadata: Dict[str, Any] = {
            "provider": "plex",
            "media_content_id": media_content_id,
            "media_content_type": plex_type,
            "title": title,
            "artist": artist,
            "album": album,
            "grandparent_title": grandparent,
            "summary": summary,
            "thumb": thumb,
            "display_title": display_title,
        }
        if duration is not None:
            metadata["duration"] = duration
        rating_key = plex_item_data.get("ratingKey")
        if rating_key:
            metadata["rating_key"] = rating_key
        return metadata

    def _normalize_dlna_metadata(
        self,
        play_media,
        browse_media,
        media_content_id: str,
        media_content_type: str | None,
        child_track_hints: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        didl_metadata = getattr(play_media, "didl_metadata", None)
        extra_attrs = getattr(didl_metadata, "extra_attributes", None)
        extra_dict = extra_attrs if isinstance(extra_attrs, dict) else {}
        generic_metadata = getattr(play_media, "metadata", None)
        metadata_dict = generic_metadata if isinstance(generic_metadata, dict) else {}

        browse_title = getattr(browse_media, "title", None) if browse_media else None
        browse_media_class = getattr(browse_media, "media_class", None) if browse_media else None
        browse_children_class = getattr(browse_media, "children_media_class", None) if browse_media else None
        browse_thumb = getattr(browse_media, "thumbnail", None) if browse_media else None

        title = self._extract_first_string(
            getattr(didl_metadata, "title", None) if didl_metadata else None,
            getattr(play_media, "title", None),
            metadata_dict.get("title"),
            extra_dict.get("title"),
            extra_dict.get("dc:title"),
            extra_dict.get("upnp:title"),
            browse_title,
        ) or self._friendly_media_title(media_content_id)

        if self._looks_like_dlna_object_id(title) and browse_title:
            title = browse_title

        artist = self._extract_first_string(
            getattr(didl_metadata, "artist", None) if didl_metadata else None,
            getattr(didl_metadata, "artists", None) if didl_metadata else None,
            getattr(didl_metadata, "album_artist", None) if didl_metadata else None,
            getattr(didl_metadata, "album_artists", None) if didl_metadata else None,
            getattr(didl_metadata, "creator", None) if didl_metadata else None,
            metadata_dict.get("artist"),
            metadata_dict.get("artists"),
            metadata_dict.get("album_artist"),
            extra_dict.get("artist"),
            extra_dict.get("artists"),
            extra_dict.get("albumArtist"),
            extra_dict.get("albumArtists"),
            extra_dict.get("upnp:artist"),
            extra_dict.get("upnp:author"),
            extra_dict.get("dc:creator"),
            extra_dict.get("dc:artist"),
        )

        album = self._extract_first_string(
            getattr(didl_metadata, "album", None) if didl_metadata else None,
            getattr(didl_metadata, "album_name", None) if didl_metadata else None,
            metadata_dict.get("album"),
            metadata_dict.get("album_name"),
            extra_dict.get("album"),
            extra_dict.get("albumName"),
            extra_dict.get("upnp:album"),
            extra_dict.get("dc:album"),
        )

        dlna_type = self._extract_first_string(getattr(didl_metadata, "upnp_class", None) if didl_metadata else None)
        mime_type = getattr(play_media, "mime_type", None)
        resolved_type = dlna_type or mime_type or media_content_type or "music"
        browse_class = browse_media_class or browse_children_class
        if browse_class:
            normalized_class = browse_class.strip().lower()
            if normalized_class:
                if "/" in (resolved_type or "") or resolved_type in {"image", "video"}:
                    resolved_type = normalized_class

        thumb = self._extract_first_string(
            getattr(didl_metadata, "album_art_uri", None) if didl_metadata else None,
            extra_dict.get("albumArtURI"),
            extra_dict.get("album_art"),
            browse_thumb,
        )

        duration = self._coerce_duration_seconds(getattr(didl_metadata, "duration", None) if didl_metadata else None)

        if not title and browse_title:
            title = browse_title

        if not album and browse_class and "album" in browse_class.lower():
            album = browse_title or title

        if not artist and browse_class and "artist" in browse_class.lower():
            artist = browse_title or title

        if self._looks_like_dlna_object_id(title) and (album or artist):
            title = album or artist

        if child_track_hints:
            if not artist:
                artist = child_track_hints.get("artist") or artist
            if not album:
                album = child_track_hints.get("album") or album
            if self._looks_like_dlna_object_id(title) and child_track_hints.get("album"):
                title = child_track_hints.get("album")

        display_title = self._build_display_title(resolved_type, title, artist, album)

        metadata: Dict[str, Any] = {
            "provider": "dlna_dms",
            "media_content_id": media_content_id,
            "media_content_type": resolved_type,
            "title": title,
            "artist": artist,
            "album": album,
            "thumb": thumb,
            "display_title": display_title or title,
        }
        if duration is not None:
            metadata["duration"] = duration
        return metadata

    def _normalize_jellyfin_metadata(
        self,
        play_media,
        browse_media,
        media_content_id: str,
        media_content_type: str | None,
        *,
        raw_item: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        title = self._extract_first_string(
            getattr(play_media, "title", None),
            getattr(browse_media, "title", None) if browse_media else None,
        ) or self._friendly_media_title(media_content_id)

        media_class = getattr(browse_media, "media_class", None) if browse_media else None
        child_class = getattr(browse_media, "children_media_class", None) if browse_media else None
        resolved_type = self._extract_first_string(media_class, child_class, media_content_type) or "audio"

        thumbnail = getattr(browse_media, "thumbnail", None) if browse_media else None

        raw_item = raw_item or self._extract_jellyfin_item(browse_media)
        item_metadata = getattr(play_media, "metadata", None)
        resolved_artist = self._extract_first_string(
            self._jellyfin_first_named_field(raw_item, "AlbumArtist"),
            self._jellyfin_first_named_field(raw_item, "AlbumArtists"),
            self._jellyfin_first_named_field(raw_item, "Artists"),
            self._jellyfin_first_named_field(raw_item, "ArtistItems"),
            self._jellyfin_first_named_field(raw_item, "Contributor"),
            self._jellyfin_first_named_field(item_metadata, "artists"),
            self._jellyfin_first_named_field(item_metadata, "artist"),
        )
        resolved_album = self._extract_first_string(
            self._jellyfin_first_named_field(raw_item, "Album"),
            self._jellyfin_first_named_field(raw_item, "AlbumItems"),
            self._jellyfin_first_named_field(raw_item, "Series"),
            self._jellyfin_first_named_field(raw_item, "ParentAlbum"),
            self._jellyfin_first_named_field(item_metadata, "album"),
        )

        runtime_ticks = None
        if isinstance(raw_item, dict):
            runtime_ticks = raw_item.get("RunTimeTicks") or raw_item.get("RuntimeTicks")
        if not runtime_ticks and isinstance(item_metadata, dict):
            runtime_ticks = item_metadata.get("runtime") or item_metadata.get("duration")
        duration = None
        if isinstance(runtime_ticks, (int, float)):
            duration = int(round(runtime_ticks / 10_000_000)) if runtime_ticks > 0 else None
        elif isinstance(runtime_ticks, str):
            try:
                duration = int(round(float(runtime_ticks)))
            except (ValueError, TypeError):
                duration = None

        if not title:
            title = self._extract_first_string(
                self._jellyfin_first_named_field(raw_item, "Name"),
                self._jellyfin_first_named_field(item_metadata, "title"),
            ) or self._friendly_media_title(media_content_id)
        elif title and isinstance(title, str) and title == media_content_id.split("/")[-1]:
            # Some Jellyfin media sources expose an opaque item ID as the title; prefer the raw item name when available.
            fallback_name = self._extract_first_string(
                self._jellyfin_first_named_field(raw_item, "Name"),
                self._jellyfin_first_named_field(raw_item, "OriginalTitle"),
            )
            if fallback_name:
                _LOGGER.debug(
                    "Jellyfin title looked like identifier; using raw item name instead: %s -> %s",
                    title,
                    fallback_name,
                )
                title = fallback_name

        display_title = self._build_display_title(resolved_type, title, resolved_artist, resolved_album)

        metadata: Dict[str, Any] = {
            "provider": "jellyfin",
            "media_content_id": media_content_id,
            "media_content_type": resolved_type,
            "title": title,
            "artist": resolved_artist,
            "album": resolved_album,
            "thumb": thumbnail,
            "display_title": display_title or title,
        }
        if duration is not None:
            metadata["duration"] = duration
        return metadata

    @staticmethod
    def _extract_jellyfin_item(browse_media) -> Dict[str, Any] | None:
        if not browse_media:
            return None
        extra = getattr(browse_media, "extra", None)
        if not isinstance(extra, dict):
            return None
        candidate_keys = (
            "item",
            "Item",
            "media_item",
            "mediaItem",
            "raw_item",
            "rawItem",
        )
        for key in candidate_keys:
            if key in extra and isinstance(extra[key], dict):
                return extra[key]
        items = extra.get("items")
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict):
                return first
        return None

    @staticmethod
    def _jellyfin_first_named_field(source: Dict[str, Any] | None, key: str):
        if source is None or not key:
            return None
        if not isinstance(source, dict):
            return AlarmAndReminderCoordinator._jellyfin_first_named_value(source)
        lookup_keys = {key, key.lower(), key.upper(), key.capitalize()}
        candidate = None
        for lookup in lookup_keys:
            if lookup in source:
                candidate = source[lookup]
                break
        return AlarmAndReminderCoordinator._jellyfin_first_named_value(candidate)

    @staticmethod
    def _jellyfin_first_named_value(value):
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, dict):
            for field in ("Name", "name", "Title", "title", "DisplayTitle", "displayTitle", "Label", "label"):
                text = value.get(field)
                if isinstance(text, str) and text.strip():
                    return text.strip()
        if isinstance(value, (list, tuple, set)):
            for entry in value:
                text = AlarmAndReminderCoordinator._jellyfin_first_named_value(entry)
                if text:
                    return text
        return None

    @staticmethod
    def _build_display_title(
        media_type: str | None,
        title: str | None,
        artist: str | None,
        album: str | None,
    ) -> str | None:
        normalized = (media_type or "").strip().lower()

        def _matches_any(value: str | None, *needles: str) -> bool:
            if not value:
                return False
            lowered = value.lower()
            return any(needle in lowered for needle in needles)

        is_track_like = False
        if normalized:
            if normalized in {"track", "song", "audio", "music"}:
                is_track_like = True
            elif normalized.startswith("audio/") or normalized.startswith("music/"):
                is_track_like = True
            elif normalized.startswith("object.item.audioitem"):
                is_track_like = True
            elif "musictrack" in normalized or ":track" in normalized:
                is_track_like = True

        is_album_like = False
        if normalized:
            if normalized in {"album"}:
                is_album_like = True
            elif "album" in normalized or ":album" in normalized:
                is_album_like = True

        is_playlist_like = normalized in {"playlist"} or "playlist" in normalized
        is_artist_like = False
        if normalized:
            if normalized == "artist" or normalized.endswith(".musicartist"):
                is_artist_like = True
            elif _matches_any(normalized, "person.music", "container.person", "artist"):
                is_artist_like = True

        # prefer semantic titles depending on type
        base_title = title
        if is_album_like and album:
            base_title = album
        elif is_artist_like and artist:
            base_title = artist

        if is_track_like and artist and (base_title or title):
            result = f"{artist} - {base_title or title}"
        elif is_album_like:
            album_title = base_title or album or title
            if artist and album_title:
                result = f"{artist} - {album_title}"
            else:
                result = album_title or artist or title
        elif is_artist_like:
            result = artist or title or album
        elif is_playlist_like and artist and (base_title or title):
            result = f"{artist} - {base_title or title}"
        else:
            result = base_title or title or artist or album

        _LOGGER.debug(
            "Display title computed: media_type=%s title=%s artist=%s album=%s -> %s",
            media_type,
            title,
            artist,
            album,
            result,
        )
        return result

    @staticmethod
    def _looks_like_dlna_object_id(value: str | None) -> bool:
        if not value or not isinstance(value, str):
            return False
        trimmed = value.strip()
        if not trimmed:
            return False
        return bool(_DLNA_HASH_ID_PATTERN.match(trimmed))

    def _dlna_browse_entry_looks_like_album(self, browse_media) -> bool:
        if not browse_media:
            return False
        media_class = getattr(browse_media, "media_class", None)
        children_class = getattr(browse_media, "children_media_class", None)
        return self._dlna_media_class_is_album(media_class) or self._dlna_media_class_is_album(children_class)

    def _dlna_media_class_is_album(self, media_class: str | None) -> bool:
        if not media_class or not isinstance(media_class, str):
            return False
        normalized = media_class.strip().lower()
        return "album" in normalized

    def _dlna_media_class_is_track(self, media_class: str | None) -> bool:
        if not media_class or not isinstance(media_class, str):
            return False
        normalized = media_class.strip().lower()
        return "track" in normalized or "audio" in normalized or normalized.endswith("music")

    async def _fetch_dlna_child_track_hints(self, browse_media) -> Dict[str, Any] | None:
        children = getattr(browse_media, "children", None) or []
        if not children:
            return None
        for child in children:
            if not child:
                continue
            media_class = getattr(child, "media_class", None)
            can_play = bool(getattr(child, "can_play", False))
            if not can_play and not self._dlna_media_class_is_track(media_class):
                continue
            child_id = getattr(child, "media_content_id", None) or getattr(child, "identifier", None)
            if not child_id:
                continue
            try:
                play_media = await media_source.async_resolve_media(self.hass, child_id, None)
            except MediaSourceError:
                continue
            hints = self._extract_dlna_track_hints(play_media)
            if hints:
                return hints
        return None

    def _extract_dlna_track_hints(self, play_media) -> Dict[str, Any] | None:
        didl_metadata = getattr(play_media, "didl_metadata", None)
        extra_attrs = getattr(didl_metadata, "extra_attributes", None) if didl_metadata else None
        extra_dict = extra_attrs if isinstance(extra_attrs, dict) else {}

        title = self._extract_first_string(
            getattr(didl_metadata, "title", None) if didl_metadata else None,
            getattr(play_media, "title", None),
        )

        artist = self._extract_first_string(
            getattr(didl_metadata, "artist", None) if didl_metadata else None,
            getattr(didl_metadata, "artists", None) if didl_metadata else None,
            getattr(didl_metadata, "album_artist", None) if didl_metadata else None,
            getattr(didl_metadata, "album_artists", None) if didl_metadata else None,
            getattr(didl_metadata, "creator", None) if didl_metadata else None,
            extra_dict.get("artist"),
            extra_dict.get("artists"),
            extra_dict.get("albumArtist"),
            extra_dict.get("albumArtists"),
            extra_dict.get("upnp:artist"),
            extra_dict.get("dc:creator"),
        )

        album = self._extract_first_string(
            getattr(didl_metadata, "album", None) if didl_metadata else None,
            getattr(didl_metadata, "album_name", None) if didl_metadata else None,
            extra_dict.get("album"),
            extra_dict.get("albumName"),
            extra_dict.get("upnp:album"),
        )

        if not any([artist, album, title]):
            return None
        return {
            "artist": artist,
            "album": album,
            "title": title,
        }

    @staticmethod
    def _build_plex_thumb_url(plex_server, thumb_value: str | None, thumb_url: str | None) -> str | None:
        if not thumb_value and thumb_url:
            thumb_value = thumb_url
        if not thumb_value:
            return None
        try:
            return plex_server.url(thumb_value)
        except Exception:  # noqa: BLE001
            return thumb_value

    @staticmethod
    def _snapshot_plex_item_attributes(plex_item) -> Dict[str, Any]:
        def _safe_get(attr: str) -> Any:
            try:
                return getattr(plex_item, attr)
            except Exception:  # noqa: BLE001
                return None

        snapshot = {
            "type": _safe_get("type"),
            "title": _safe_get("title"),
            "grandparentTitle": _safe_get("grandparentTitle"),
            "parentTitle": _safe_get("parentTitle"),
            "originalTitle": _safe_get("originalTitle"),
            "summary": _safe_get("summary"),
            "duration": _safe_get("duration"),
            "thumb": _safe_get("thumb"),
            "thumbUrl": _safe_get("thumbUrl"),
            "ratingKey": _safe_get("ratingKey"),
        }
        return {key: value for key, value in snapshot.items() if value is not None}

    @staticmethod
    def _extract_first_string(*values: Any) -> str | None:
        for value in values:
            text = AlarmAndReminderCoordinator._coerce_didl_text(value)
            if text:
                return text
        return None

    @staticmethod
    def _coerce_didl_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        if isinstance(value, (list, tuple, set)):
            for entry in value:
                text = AlarmAndReminderCoordinator._coerce_didl_text(entry)
                if text:
                    return text
            return None
        for attr in ("value", "text", "name"):
            if hasattr(value, attr):
                text = AlarmAndReminderCoordinator._coerce_didl_text(getattr(value, attr))
                if text:
                    return text
        try:
            text = str(value).strip()
        except Exception:  # noqa: BLE001
            return None
        return text or None

    @staticmethod
    def _coerce_duration_seconds(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            duration = int(round(value))
            return duration if duration >= 0 else None
        if isinstance(value, str):
            parts = value.strip().split(":")
            if not parts or any(part == "" for part in parts):
                return None
            try:
                parts = [int(float(part)) for part in parts]
            except ValueError:
                return None
            while len(parts) < 3:
                parts.insert(0, 0)
            hours, minutes, seconds = parts[-3:]
            total = hours * 3600 + minutes * 60 + seconds
            return total if total >= 0 else None
        return None

    def _prune_resolved_media_metadata_cache(self, now: float) -> None:
        stale_keys = [
            key
            for key, (timestamp, _) in self._resolved_media_metadata_cache.items()
            if now - timestamp > 900
        ]
        for key in stale_keys:
            self._resolved_media_metadata_cache.pop(key, None)
            _LOGGER.debug("Evicted stale media metadata cache entry: %s", key)

    @staticmethod
    def _summarize_media_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(metadata, dict):
            return {"raw": metadata}
        summary = {
            "provider": metadata.get("provider"),
            "media_content_type": metadata.get("media_content_type"),
            "title": metadata.get("title"),
            "display_title": metadata.get("display_title"),
            "artist": metadata.get("artist"),
            "album": metadata.get("album"),
            "duration": metadata.get("duration"),
            "rating_key": metadata.get("rating_key"),
            "has_thumb": bool(metadata.get("thumb")),
        }
        return {key: value for key, value in summary.items() if value is not None}

    @staticmethod
    def _normalize_media_provider(value: str | None) -> str | None:
        if not value or not isinstance(value, str):
            return None
        normalized = value.strip().lower()
        return normalized or None

    @staticmethod
    def _detect_media_provider(media_content_id: str | None) -> str | None:
        if not media_content_id or not isinstance(media_content_id, str):
            return None
        lowered = media_content_id.lower()
        try:
            parsed = urlparse(media_content_id)
        except ValueError:
            parsed = None
        scheme = (parsed.scheme or "").lower() if parsed else ""
        netloc = (parsed.netloc or "").lower() if parsed else ""
        if scheme == "plex" or (scheme == "media-source" and netloc == "plex"):
            return "plex"
        if scheme == "media-source" and netloc == "dlna_dms":
            return "dlna_dms"
        if scheme == "media-source" and netloc == "jellyfin":
            return "jellyfin"
        if lowered.startswith("media-source://dlna_dms/"):
            return "dlna_dms"
        if lowered.startswith("media-source://jellyfin/"):
            return "jellyfin"
        return None


    def _get_next_available_id(self, prefix: str) -> str:
        """Get next available ID for alarms."""
        counter = 1
        while True:
            potential_id = f"{prefix}_{counter}"
            if potential_id not in self._active_items:
                return potential_id
            counter += 1

    @staticmethod
    def _slugify_name(value: Optional[str]) -> str:
        """Convert a user-provided name into a Home Assistant-safe slug."""
        if not isinstance(value, str):
            return ""
        normalized = unicodedata.normalize("NFKD", value)
        ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
        lowered = ascii_only.lower()
        replaced = re.sub(r"[^a-z0-9]+", "_", lowered)
        collapsed = re.sub(r"_+", "_", replaced)
        return collapsed.strip("_")

    def _unique_name_slug(self, base_slug: str, prefix: str) -> str:
        """Ensure the generated slug is unique among active items."""
        if not base_slug:
            return self._get_next_available_id(prefix)
        if base_slug not in self._active_items:
            return base_slug
        counter = 2
        while True:
            candidate = f"{base_slug}_{counter}"
            if candidate not in self._active_items:
                return candidate
            counter += 1

    @staticmethod
    def _looks_like_generated_name(value: str | None, prefix: str) -> bool:
        """Return whether the stored name looks auto-generated."""
        if not isinstance(value, str) or not value:
            return False
        return re.fullmatch(rf"{re.escape(prefix)}_\d+", value) is not None

    def _duplicate_name_for_item(self, item: Dict[str, Any], *, is_alarm: bool) -> str:
        """Allocate the duplicate item's stored name."""
        prefix = "alarm" if is_alarm else "reminder"
        existing_name = item.get("name")
        if isinstance(existing_name, str) and existing_name.strip():
            stripped_name = existing_name.strip()
            if is_alarm and self._looks_like_generated_name(stripped_name, prefix):
                return self._get_next_available_id(prefix)
            copy_base = self._slugify_name(f"{stripped_name}-copy")
            return self._unique_name_slug(copy_base, prefix)
        return self._get_next_available_id(prefix)

    @staticmethod
    def _humanize_name(slug: str) -> str:
        """Return a human-friendly version of a slugified name."""
        if not isinstance(slug, str) or not slug:
            return ""
        replaced = re.sub(r"[_\s]+", " ", slug).strip()
        if not replaced:
            return ""
        return re.sub(r"\b([a-z])", lambda match: match.group(1).upper(), replaced)

    def _normalize_media_player(self, value) -> Optional[str]:
        """Normalize media_player input to a single entity_id string or None."""
        if not value:
            return None

        # Handle dict structures like {"entity_id": "..."} or {"entity_ids": [...]}
        if isinstance(value, dict):
            if value.get("entity_id"):
                value = value["entity_id"]
            elif value.get("entity_ids"):
                value = value["entity_ids"]
            else:
                return None

        # Handle iterables (lists / tuples / sets)
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if item:
                    return str(item)
            return None

        return str(value)

    @staticmethod
    def _normalize_spotify_source_value(value: Any) -> str | None:
        """Normalize a spotify_source input into a clean string or None."""
        if value in (None, "", False):
            return None
        candidate = str(value).strip()
        return candidate or None

    def _get_known_spotify_sources(self, media_player: str | None) -> list[str]:
        """Return the list of known spotify sources for the player, if any."""
        if not media_player:
            return []

        def _extend_sources(container, results: list[str]) -> None:
            if not container:
                return
            if isinstance(container, str):
                normalized = self._normalize_spotify_source_value(container)
                if normalized and normalized not in results:
                    results.append(normalized)
                return
            if isinstance(container, dict):
                values = container.values()
            else:
                values = container
            try:
                iterator = iter(values)
            except TypeError:
                return
            for entry in iterator:
                normalized = self._normalize_spotify_source_value(entry)
                if normalized and normalized not in results:
                    results.append(normalized)

        collected: list[str] = []
        state = self.hass.states.get(media_player)
        attrs = state.attributes if state and state.attributes else {}
        _extend_sources(attrs.get("source_list"), collected)

        profile = self.get_media_player_profile(media_player)
        profile_attrs = profile.get("attributes") or {}
        _extend_sources(profile_attrs.get("source_list"), collected)

        return collected

    def _validate_spotify_player_usage(
        self,
        media_player: Optional[str],
        *,
        is_alarm: bool,
        spotify_source: Any,
    ) -> str | None:
        """Ensure spotify-specific requirements are satisfied and return normalized source."""
        normalized_source = self._normalize_spotify_source_value(spotify_source)

        if not media_player:
            if normalized_source:
                raise ValueError("spotify_source requires a media_player to be specified.")
            return None

        profile = self.get_media_player_profile(media_player)
        family = profile.get("family", "home_assistant")

        if family != "spotify":
            if normalized_source:
                raise ValueError("The spotify_source field is only valid for Spotify media players.")
            return None

        if not is_alarm:
            raise ValueError(
                "Spotify media players can only be used for alarms; reminders require a player that can speak the reminder message."
            )

        if not normalized_source:
            raise ValueError(
                "Specify the spotify_source (for example, the Spotify Connect device name) when scheduling an alarm on a Spotify media player."
            )

        available = self._get_known_spotify_sources(media_player)
        if available and normalized_source not in available:
            sample = ", ".join(available[:10])
            raise ValueError(
                (
                    f"spotify_source '{normalized_source}' is not available on {media_player}. "
                    f"Choose one of: {sample}"
                )
            )

        return normalized_source

    def get_media_player_profile(self, entity_id: Optional[str]) -> Dict[str, Any]:
        """Return cached media player profile details for downstream logic."""
        if not entity_id:
            return {
                "entity_id": None,
                "family": "unknown",
                "platform": None,
                "mass_player_type": None,
                "attributes": {},
            }

        profile = self._media_player_profile_cache.get(entity_id)
        if profile:
            return profile

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

        profile = {
            "entity_id": entity_id,
            "family": family,
            "platform": platform,
            "mass_player_type": mass_player_type,
            "attributes": attributes,
        }

        self._media_player_profile_cache[entity_id] = profile
        return profile

    def _normalize_activation_entity(
        self,
        value: Any,
        *,
        enforce_allowed: bool,
        item_name: str | None = None,
    ) -> str | None:
        """Normalize activation entity input and enforce allow list when requested."""
        if value in (None, "", False):
            return None

        candidate = value
        if isinstance(candidate, dict):
            candidate = candidate.get("entity_id") or candidate.get("entity")
        if isinstance(candidate, (list, tuple, set)):
            candidate = next((entry for entry in candidate if entry), None)
            if isinstance(candidate, dict):
                candidate = candidate.get("entity_id") or candidate.get("entity")

        if candidate in (None, "", False):
            return None

        candidate_str = str(candidate).strip()
        if not candidate_str:
            return None

        try:
            entity_id = cv.entity_id(candidate_str)
        except vol.Invalid as err:
            message = f"Invalid activation entity: {candidate_str}"
            if enforce_allowed:
                raise ValueError(message) from err
            _LOGGER.warning(
                "%s on item %s; clearing field.",
                message,
                item_name or "<unknown>",
            )
            return None

        if (
            self._allowed_activation_entities is not None
            and entity_id not in self._allowed_activation_entities
        ):
            if enforce_allowed:
                raise ValueError(
                    (
                        f"Activation entity '{entity_id}' is not in the configured allow list. "
                        "Update the integration options to include it."
                    )
                )
            _LOGGER.warning(
                "Activation entity '%s' on item %s is not in the allowed list; clearing field.",
                entity_id,
                item_name or "<unknown>",
            )
            return None

        return entity_id

    def _normalize_volume_override(self, value: Any) -> float | None:
        """Normalize optional volume override to 0.0-1.0 range."""
        if value in (None, "", False):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            _LOGGER.debug("Ignoring invalid volume override %s", value)
            return None
        if number < 0:
            number = 0.0
        if number <= 1.0:
            return number
        if number <= 100.0:
            return min(1.0, number / 100.0)
        return 1.0

    async def _resolve_owner_metadata(
        self,
        explicit_user_id: Any = None,
        *,
        context: Context | None = None,
        use_context_default: bool = False,
    ) -> Dict[str, str | None]:
        """Resolve persisted owner metadata from an explicit field or HA context."""
        if explicit_user_id in ("", None):
            if not use_context_default:
                return {ATTR_USER_ID: None, ATTR_USER_NAME: None}
            user_id = getattr(context, "user_id", None) if context else None
        else:
            user_id = str(explicit_user_id).strip()

        if not user_id:
            return {ATTR_USER_ID: None, ATTR_USER_NAME: None}

        user = await self.hass.auth.async_get_user(user_id)
        if user is None:
            raise ValueError(f"Unknown Home Assistant user: {user_id}")

        return {
            ATTR_USER_ID: user.id,
            ATTR_USER_NAME: user.name or user.id,
        }

    def _normalize_item_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize internal representation of an alarm/reminder item."""
        normalized = dict(item)
        # Consolidate media player field (legacy data may contain media_players list)
        legacy_media_players = normalized.pop("media_players", None)
        if "media_player" not in normalized and legacy_media_players is not None:
            normalized["media_player"] = legacy_media_players
        normalized["media_player"] = self._normalize_media_player(
            normalized.get("media_player")
        )
        if not normalized["media_player"]:
            default_player = self.get_default_media_player()
            if default_player:
                normalized["media_player"] = default_player

        # Normalize scheduled_time
        sched = normalized.get("scheduled_time")
        if isinstance(sched, str):
            try:
                normalized["scheduled_time"] = dt_util.parse_datetime(sched)
            except Exception:
                pass

        sched_dt = normalized.get("scheduled_time")
        if isinstance(sched_dt, datetime):
            try:
                normalized["scheduled_time"] = dt_util.as_local(sched_dt)
            except Exception:
                normalized["scheduled_time"] = sched_dt
            sched_dt = normalized["scheduled_time"]

        canonical = normalized.get("scheduled_time_canonical")
        canonical_dt: datetime | None = None
        if isinstance(canonical, str):
            try:
                canonical_dt = dt_util.parse_datetime(canonical)
            except Exception:
                canonical_dt = None
        elif isinstance(canonical, datetime):
            canonical_dt = canonical

        if isinstance(canonical_dt, datetime):
            try:
                canonical_dt = dt_util.as_local(canonical_dt)
            except Exception:
                pass
        else:
            canonical_dt = sched_dt if isinstance(sched_dt, datetime) else None

        normalized["scheduled_time_canonical"] = canonical_dt

        # Normalize repeat fields
        repeat_value = normalized.get("repeat", "once")
        if isinstance(repeat_value, str):
            repeat_value = repeat_value.lower()
        repeat_days = normalized.get("repeat_days")
        if repeat_days is None:
            repeat_days = []
        elif not isinstance(repeat_days, list):
            repeat_days = list(repeat_days)
        normalized["repeat_days"] = [
            str(day).strip().lower()
            for day in repeat_days
            if day is not None and str(day).strip()
        ]

        if repeat_value == "weekly":
            weekday_index: int | None = None
            scheduled_dt = normalized.get("scheduled_time")
            if isinstance(scheduled_dt, datetime):
                weekday_index = dt_util.as_local(scheduled_dt).weekday()
            elif normalized["repeat_days"]:
                weekday_index = WEEKDAY_NAME_TO_INDEX.get(normalized["repeat_days"][0])

            if weekday_index is not None and 0 <= weekday_index < len(WEEKDAY_INDEX_TO_NAME):
                candidate_day = WEEKDAY_INDEX_TO_NAME[weekday_index]
                if candidate_day not in normalized["repeat_days"]:
                    normalized["repeat_days"].append(candidate_day)
            repeat_value = "custom"

        normalized["repeat"] = repeat_value

        user_id = normalized.get(ATTR_USER_ID)
        if user_id in ("", None):
            normalized[ATTR_USER_ID] = None
            normalized[ATTR_USER_NAME] = None
        else:
            normalized[ATTR_USER_ID] = str(user_id)
            user_name = normalized.get(ATTR_USER_NAME)
            normalized[ATTR_USER_NAME] = str(user_name) if user_name not in ("", None) else None

        raw_name = normalized.get("name")
        if isinstance(raw_name, str):
            slug = self._slugify_name(raw_name)
            if slug:
                normalized["name"] = slug

        sound_media = normalized.get("sound_media")
        if isinstance(sound_media, dict):
            duration_value = sound_media.get("duration")
            if isinstance(duration_value, (int, float)):
                duration_value = float(duration_value)
            else:
                try:
                    duration_value = float(duration_value) if duration_value is not None else None
                except (TypeError, ValueError):
                    duration_value = None

            descriptor = {
                "kind": sound_media.get("kind", "unknown"),
                "original_id": sound_media.get("original_id"),
                "resolved_url": sound_media.get("resolved_url"),
                "content_type": sound_media.get("content_type"),
                "duration": duration_value,
                "media_content_id": sound_media.get("media_content_id") or sound_media.get("original_id"),
                "media_content_type": sound_media.get("media_content_type") or sound_media.get("content_type"),
            }
            if sound_media.get("media_content_title"):
                descriptor["media_content_title"] = sound_media.get("media_content_title")
            breadcrumb_path = self._normalize_media_browser_path_input(sound_media.get("media_browser_path"))
            if breadcrumb_path:
                descriptor["media_browser_path"] = breadcrumb_path
            normalized["sound_media"] = descriptor
            fallback_media = descriptor.get("original_id") or descriptor.get("resolved_url")
            if fallback_media:
                normalized["sound_file"] = fallback_media
        elif normalized.get("sound_file") in (None, ""):
            normalized["sound_file"] = (
                DEFAULT_ALARM_SOUND if normalized.get("is_alarm") else DEFAULT_REMINDER_SOUND
            )

        # Ensure message and sound defaults exist to avoid KeyErrors downstream
        normalized.setdefault("message", "")
        normalized.setdefault("notify_device", None)
        normalized.setdefault("enabled", True)
        normalized.setdefault("status", "scheduled")

        spotify_source = self._normalize_spotify_source_value(normalized.get(ATTR_SPOTIFY_SOURCE))
        if spotify_source:
            normalized[ATTR_SPOTIFY_SOURCE] = spotify_source
        else:
            normalized.pop(ATTR_SPOTIFY_SOURCE, None)

        volume_override = self._normalize_volume_override(normalized.get(ATTR_VOLUME))
        if volume_override is not None:
            normalized[ATTR_VOLUME] = volume_override
        else:
            normalized.pop(ATTR_VOLUME, None)

        normalized["activation_entity"] = self._normalize_activation_entity(
            normalized.get("activation_entity"),
            enforce_allowed=False,
            item_name=normalized.get("entity_id") or normalized.get("name"),
        )

        announce_flag = normalized.get("announce_time", True)
        if announce_flag is None:
            announce_flag = True
        elif isinstance(announce_flag, str):
            announce_flag = announce_flag.lower() not in ("false", "0", "no")
        normalized["announce_time"] = bool(announce_flag)

        announce_name_flag = normalized.get("announce_name")
        if normalized.get("is_alarm"):
            if announce_name_flag is None:
                announce_name_flag = True
            elif isinstance(announce_name_flag, str):
                announce_name_flag = announce_name_flag.lower() not in ("false", "0", "no")
            normalized["announce_name"] = bool(announce_name_flag)
        else:
            normalized["announce_name"] = True

        return normalized

    def _resolve_repeat_weekdays(
        self,
        repeat: str,
        repeat_days: list[str] | None,
        base_weekday: int,
    ) -> set[int] | None:
        """Return the set of weekdays an item should run on for a repeat pattern."""
        repeat_key = (repeat or "once").lower()
        if repeat_key == "once":
            return None
        if repeat_key == "daily":
            return set(ALL_WEEKDAYS)
        if repeat_key == "weekdays":
            return {0, 1, 2, 3, 4}
        if repeat_key == "weekends":
            return {5, 6}
        if repeat_key == "custom":
            resolved: set[int] = set()
            if repeat_days:
                for raw_day in repeat_days:
                    if not isinstance(raw_day, str):
                        continue
                    day = raw_day.strip().lower()
                    if day in WEEKDAY_NAME_TO_INDEX:
                        resolved.add(WEEKDAY_NAME_TO_INDEX[day])
            if not resolved:
                _LOGGER.warning("Custom repeat configured without valid repeat_days; treating as once")
                return None
            return resolved
        return None

    def _next_matching_weekday(
        self,
        candidate: datetime,
        allowed_days: set[int] | None,
        *,
        include_today: bool,
    ) -> datetime | None:
        """Advance candidate to the next date whose weekday is allowed."""
        if not allowed_days or allowed_days == ALL_WEEKDAYS:
            return candidate
        cursor = candidate
        check_today = include_today
        for _ in range(7):
            if check_today and cursor.weekday() in allowed_days:
                return cursor
            cursor += timedelta(days=1)
            check_today = True
            if cursor.weekday() in allowed_days:
                return cursor
        return None

    def _ensure_future_schedule_time(
        self,
        scheduled_time: datetime | None,
        *,
        repeat: str,
        repeat_days: list[str] | None = None,
        reference: datetime | None = None,
        force_advance: bool = False,
    ) -> datetime | None:
        """Normalize scheduled_time so it lands on the next valid repeat slot."""
        if not isinstance(scheduled_time, datetime):
            return None

        candidate = dt_util.as_local(scheduled_time)
        reference_point = dt_util.as_local(reference or dt_util.now())

        repeat_key = (repeat or "once").lower()
        allowed_days = self._resolve_repeat_weekdays(repeat_key, repeat_days or [], candidate.weekday())
        if repeat_key == "custom" and not allowed_days:
            repeat_key = "once"

        # Align to an allowed weekday before comparing to the reference.
        aligned = self._next_matching_weekday(candidate, allowed_days, include_today=True)
        if aligned is None:
            return None
        candidate = aligned

        # For one-off items, simply ensure the timestamp is in the future.
        if repeat_key == "once":
            if candidate <= reference_point:
                candidate = candidate + timedelta(days=1)
            return candidate

        # When we explicitly need the next occurrence (e.g. after a trigger), make sure
        # we advance at least once even if the reference is still in the future.
        if force_advance and candidate > reference_point:
            reference_point = candidate

        # Bump forward until the scheduled time is in the future relative to reference.
        while candidate <= reference_point:
            candidate = candidate + timedelta(days=1)
            aligned = self._next_matching_weekday(candidate, allowed_days, include_today=True)
            if aligned is None:
                return None
            candidate = aligned

        return candidate

    def _serialize_item_state(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return attributes dict safe for Home Assistant state machine."""
        data = dict(item)
        sched = data.get("scheduled_time")
        if isinstance(sched, datetime):
            data["scheduled_time"] = sched.isoformat()
        canonical = data.get("scheduled_time_canonical")
        if isinstance(canonical, datetime):
            data["scheduled_time_canonical"] = canonical.isoformat()
        data["media_player"] = self._normalize_media_player(data.get("media_player"))
        data.pop("media_players", None)
        if data.get("repeat_days") is None:
            data["repeat_days"] = []
        data["announce_time"] = bool(data.get("announce_time", True))
        if data.get("is_alarm"):
            data["announce_name"] = bool(data.get("announce_name", True))
        else:
            data["announce_name"] = True
        if data.get("activation_entity") in ("", None):
            data["activation_entity"] = None
        spotify_source = self._normalize_spotify_source_value(data.get(ATTR_SPOTIFY_SOURCE))
        if spotify_source:
            data[ATTR_SPOTIFY_SOURCE] = spotify_source
        else:
            data.pop(ATTR_SPOTIFY_SOURCE, None)
        volume_override = self._normalize_volume_override(data.get(ATTR_VOLUME))
        if volume_override is not None:
            data[ATTR_VOLUME] = volume_override
        else:
            data.pop(ATTR_VOLUME, None)
        return data

    def _build_announcement_text(self, item: Dict[str, Any]) -> Optional[str]:
        """Construct announcement message based on item settings."""
        parts: list[str] = []
        is_alarm = item.get("is_alarm", False)
        name = item.get("name") or ""
        display_name: Optional[str] = None
        announce_name_enabled = True
        if is_alarm:
            announce_name_enabled = bool(item.get("announce_name", True))

        if name:
            slug = name.replace(" ", "_").lower()
            default_prefix = "alarm_" if is_alarm else "reminder_"
            if not is_alarm:
                display_name = self._humanize_name(name)
            elif not slug.startswith(default_prefix):
                display_name = self._humanize_name(name)
        if is_alarm and display_name and announce_name_enabled:
            parts.append(f"{display_name} alarm.")
        elif not is_alarm and display_name:
            parts.append(f"Time to {display_name}.")
        if item.get("announce_time", True):
            current_time = dt_util.now().strftime("%I:%M %p").lstrip("0")
            parts.append(f"It's {current_time}")
        message = (item.get("message") or "").strip()
        if message:
            parts.append(message)
        announcement = " ".join(part.strip() for part in parts if part).strip()
        return announcement or None

    def _write_item_state(self, item_id: str) -> None:
        """Push current item data into its individual HA entity."""
        item = self._active_items.get(item_id)
        if not item:
            return
        state = item.get("status", "scheduled")
        attributes = self._serialize_item_state(item)
        entity_id = self._entity_id_for_item(item_id, item)
        self.hass.states.async_set(entity_id, state, attributes)
        self._fire_state_event(entity_id)

    def _fire_state_event(
        self,
        entity_id: str | None,
        *,
        action: str = "updated",
    ) -> None:
        """Send a state-changed event for dashboards and switches."""
        payload = {"action": action}
        if entity_id:
            payload["entity_id"] = entity_id
        self.hass.bus.async_fire(
            f"{DOMAIN}_state_changed",
            payload,
        )

    def _broadcast_state_refresh(self, action: str = "updated") -> None:
        """Fire change events for dashboards and switches."""
        self._fire_state_event(None, action=action)

    async def _mark_item_expired(self, item_id: str, *, reason: str | None = None) -> None:
        """Mark an item as expired because its scheduled time is in the past."""
        item = self._active_items.get(item_id)
        if not item:
            return
        if item.get("status") == "expired":
            return
        item["status"] = "expired"
        item.setdefault("enabled", True)
        self._active_items[item_id] = self._normalize_item_fields(item)
        await self.storage.async_save(self._active_items)
        self._write_item_state(item_id)
        self._update_dashboard_state()
        self._broadcast_state_refresh()
        msg = f"Marked {item_id} as expired"
        if reason:
            msg += f": {reason}"
        _LOGGER.info(msg)

    async def _discard_stale_active_item(
        self,
        item_id: str,
        item: Dict[str, Any],
        now: datetime,
    ) -> Dict[str, Any]:
        """Handle an item restored as "active" whose trigger time is long past.

        Instead of resuming playback (possibly days after the alarm fired),
        reschedule repeating items for their next occurrence and expire
        one-off items, mirroring what stop_item does after playback.
        """
        repeat_value = (item.get("repeat", "once") or "once").lower()
        canonical_time = item.get("scheduled_time_canonical")
        if isinstance(canonical_time, str):
            canonical_time = dt_util.parse_datetime(canonical_time)
        if not isinstance(canonical_time, datetime):
            sched = item.get("scheduled_time")
            if isinstance(sched, str):
                sched = dt_util.parse_datetime(sched)
            canonical_time = sched if isinstance(sched, datetime) else None

        item["status"] = "stopped"
        item["last_stopped"] = now.isoformat()

        next_time = None
        if (
            repeat_value != "once"
            and item.get("enabled", True)
            and isinstance(canonical_time, datetime)
        ):
            next_time = self._ensure_future_schedule_time(
                canonical_time,
                repeat=repeat_value,
                repeat_days=item.get("repeat_days", []) or [],
                reference=now,
                force_advance=True,
            )

        if next_time is not None:
            item["scheduled_time"] = next_time
            item["scheduled_time_canonical"] = next_time
            item["status"] = "scheduled"
            item = self._normalize_item_fields(item)
            self._active_items[item_id] = item
            await self.storage.async_save(self._active_items)
            self._schedule_trigger(item_id, next_time)
            if item.get("is_alarm"):
                self._bump_last_alarm_time(next_time)
            _LOGGER.info(
                "Discarded stale active item %s from a previous run; rescheduled for %s",
                item_id,
                next_time.isoformat(),
            )
        else:
            item = self._normalize_item_fields(item)
            self._active_items[item_id] = item
            await self._mark_item_expired(
                item_id,
                reason="Item was still active from a previous run and its time has long passed",
            )
            item = self._active_items.get(item_id, item)

        return item

    def _bump_last_alarm_time(self, scheduled_time: Optional[datetime]) -> None:
        """Track most recent alarm scheduling for default picker."""
        if scheduled_time and (
            self._last_alarm_time is None or scheduled_time > self._last_alarm_time
        ):
            self._last_alarm_time = scheduled_time

    def _entity_id_for_item(
        self,
        item_id: str,
        item: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Return the entity_id representing an alarm or reminder."""
        if item is None:
            item = self._active_items.get(item_id, {})

        domain = ALARM_ENTITY_DOMAIN if item.get("is_alarm") else REMINDER_ENTITY_DOMAIN
        return f"{domain}.{item_id}"

    @staticmethod
    def _strip_domain(item_id: str) -> str:
        """Normalize an id by removing any known domain prefixes."""
        for prefix in (
            f"{ALARM_ENTITY_DOMAIN}.",
            f"{REMINDER_ENTITY_DOMAIN}.",
            f"{DOMAIN}.",
            "sensor.",
        ):
            if item_id.startswith(prefix):
                return item_id.split(".")[-1]
        return item_id

    def _resolve_active_item_id(self, item_id: str | None) -> str | None:
        """Return the stored active-item identifier for a given raw id."""
        if item_id is None:
            return None

        if isinstance(item_id, str):
            stripped = self._strip_domain(item_id)
        else:
            stripped = item_id

        if stripped in self._active_items:
            return stripped

        if isinstance(stripped, str):
            lowered = stripped.casefold()
            for existing_id in self._active_items.keys():
                if isinstance(existing_id, str) and existing_id.casefold() == lowered:
                    return existing_id

        return None
    def _cancel_scheduled_trigger(self, item_id: str) -> None:
        """Cancel any scheduled callback for an item."""
        remove = self._scheduled_callbacks.pop(item_id, None)
        if remove:
            try:
                remove()
                _LOGGER.debug("Cancelled scheduled trigger for %s", item_id)
            except Exception as err:
                _LOGGER.error("Error cancelling trigger for %s: %s", item_id, err, exc_info=True)

    def _schedule_trigger(self, item_id: str, scheduled_time: datetime) -> None:
        """Register a trigger callback for the given item."""
        if not isinstance(scheduled_time, datetime):
            _LOGGER.warning("Cannot schedule %s, invalid scheduled_time: %s", item_id, scheduled_time)
            return

        scheduled_time = dt_util.as_local(scheduled_time)
        now = dt_util.now()

        # If already due, trigger immediately.
        if scheduled_time <= now:
            _LOGGER.debug(
                "Scheduled time for %s (%s) is in the past; triggering immediately",
                item_id,
                scheduled_time.isoformat(),
            )
            self._cancel_scheduled_trigger(item_id)
            self.hass.async_create_task(self._trigger_item(item_id))
            return

        # Cancel any previous registration.
        self._cancel_scheduled_trigger(item_id)

        @callback
        def _handle(now_dt: datetime, iid: str = item_id) -> None:
            _LOGGER.debug(
                "Trigger fired for %s at %s (scheduled for %s)",
                iid,
                now_dt.isoformat(),
                scheduled_time.isoformat(),
            )
            self._scheduled_callbacks.pop(iid, None)
            self.hass.async_create_task(self._trigger_item(iid))

        remove = async_track_point_in_time(self.hass, _handle, scheduled_time)
        self._scheduled_callbacks[item_id] = remove
        _LOGGER.debug(
            "Registered trigger for %s at %s",
            item_id,
            scheduled_time.isoformat(),
        )

    def get_default_alarm_time(self) -> dt_time:
        """Return default alarm time (last scheduled or 07:00)."""
        if self._last_alarm_time:
            return self._last_alarm_time.time()
        return dt_time(7, 0)

    async def async_load_items(self) -> None:
        """Load items from storage and restore internal state (called at startup)."""
        try:
            # Flattened mapping: item_id -> item dict
            self._active_items = await self.storage.async_load()
            _LOGGER.debug("Loaded items from storage: %s", self._active_items)

            # Rebuild used id sets
            self._used_alarm_ids = {iid for iid, it in self._active_items.items() if it.get("is_alarm")}
            self._used_reminder_ids = {iid for iid, it in self._active_items.items() if not it.get("is_alarm")}

            now = dt_util.now()

            for item_id, item in list(self._active_items.items()):
                # Normalize scheduled_time if string
                if "scheduled_time" in item and isinstance(item["scheduled_time"], str):
                    item["scheduled_time"] = dt_util.parse_datetime(item["scheduled_time"])

                item = self._normalize_item_fields(item)
                self._active_items[item_id] = item

                descriptor: Dict[str, Any] | None
                if not isinstance(item.get("sound_media"), dict):
                    try:
                        descriptor = await self._prepare_sound_descriptor(
                            item.get("sound_file"),
                            is_alarm=item.get("is_alarm", False),
                        )
                    except Exception as err:
                        _LOGGER.error("Failed to normalize media for %s: %s", item_id, err)
                        descriptor = await self._default_sound_descriptor(item.get("is_alarm", False))
                else:
                    descriptor = dict(item["sound_media"])

                media_player_target = item.get("media_player") or self.get_default_media_player()
                if descriptor is not None:
                    try:
                        descriptor = await self._ensure_media_player_media_compatibility(
                            media_player_target,
                            descriptor,
                        )
                    except ValueError as err:
                        _LOGGER.warning(
                            "Media '%s' incompatible with %s during restore: %s. Falling back to default.",
                            descriptor.get("original_id") or descriptor.get("resolved_url"),
                            media_player_target,
                            err,
                        )
                        descriptor = await self._default_sound_descriptor(item.get("is_alarm", False))

                    item["sound_media"] = descriptor
                    playback_id = self._select_media_identifier_for_player(
                        descriptor,
                        media_player_target,
                    )
                    if playback_id:
                        item["sound_file"] = playback_id
                    item = self._normalize_item_fields(item)
                    self._active_items[item_id] = item

                status = item.get("status", "scheduled")

                if item.get("is_alarm") and isinstance(item.get("scheduled_time"), datetime):
                    self._bump_last_alarm_time(item["scheduled_time"])

                # Restore entity state in HA
                state_data = dict(item)
                if "scheduled_time" in state_data and isinstance(state_data["scheduled_time"], datetime):
                    state_data["scheduled_time"] = state_data["scheduled_time"].isoformat()

                # Mark overall state 'active' if any active items exist, otherwise 'idle'
                # and include full items lists as attributes.
                # schedule playback/resume as before per item
                if status == "active":
                    sched = item.get("scheduled_time")
                    if isinstance(sched, str):
                        sched = dt_util.parse_datetime(sched)
                    resume_deadline = None
                    if isinstance(sched, datetime):
                        resume_deadline = dt_util.as_local(sched) + timedelta(
                            minutes=ACTIVE_RESUME_GRACE_MINUTES
                        )
                    if resume_deadline is not None and now <= resume_deadline:
                        self._stop_events[item_id] = asyncio.Event()
                        task = self.hass.async_create_task(self._start_playback(item_id), name=f"playback_{item_id}")
                        self._playback_tasks[item_id] = task
                    else:
                        item = await self._discard_stale_active_item(item_id, item, now)
                        self._active_items[item_id] = item
                # Schedule future triggers for scheduled items
                elif status == "scheduled" and item.get("scheduled_time"):
                    sched = item["scheduled_time"]
                    if isinstance(sched, str):
                        sched = dt_util.parse_datetime(sched)
                        item["scheduled_time"] = sched

                    if isinstance(sched, datetime):
                        adjusted = self._ensure_future_schedule_time(
                            sched,
                            repeat=item.get("repeat", "once"),
                            repeat_days=item.get("repeat_days", []),
                            reference=now,
                        )
                        if adjusted is not None:
                            if adjusted != sched:
                                item["scheduled_time"] = adjusted
                                self._active_items[item_id] = item
                                sched = adjusted
                        self._schedule_trigger(item_id, sched)

                self._write_item_state(item_id)

            # update central dashboard entity
            self._update_dashboard_state()
            self._broadcast_state_refresh()

        except Exception as err:
            _LOGGER.error("Error loading items in coordinator: %s", err, exc_info=True)
        
    async def schedule_item(self, call: ServiceCall, is_alarm: bool, target: dict) -> None:
        """Schedule an alarm or reminder (moved from sensor)."""
        
        _LOGGER.debug("schedule_item called on coordinator: %s", id(self))

        try:
            now = dt_util.now()

            # parse inputs (time/date/message/repeat etc.) - adapt to your service schema keys
            time_input = call.data.get("time")  # expected as time object or "HH:MM" or ISO
            date_input = call.data.get("date")  # optional date object
            message = call.data.get("message", "")

            # If user supplied a name use it; otherwise allocate numeric id (alarm_1, alarm_2, ...)
            supplied_name = call.data.get("name")
            if is_alarm:
                if supplied_name:
                    base_slug = self._slugify_name(supplied_name)
                    item_name = self._unique_name_slug(base_slug, "alarm")
                else:
                    item_name = self._get_next_available_id("alarm")
                display_name = item_name
            else:
                # Reminders MUST have names as requested
                if not supplied_name:
                    raise ValueError("Reminders require a name")
                base_slug = self._slugify_name(supplied_name)
                if not base_slug:
                    raise ValueError("Reminder name must contain letters or numbers")
                if base_slug in self._active_items:
                    # Do not allow duplicate reminder names
                    raise ValueError(f"Reminder name already exists: {supplied_name}")
                item_name = base_slug
                display_name = item_name

            repeat_raw = call.data.get("repeat", "once")
            repeat = repeat_raw.lower() if isinstance(repeat_raw, str) else (repeat_raw or "once")
            repeat_days_raw = call.data.get("repeat_days", [])
            if repeat_days_raw is None:
                repeat_days = []
            elif isinstance(repeat_days_raw, (list, tuple, set)):
                repeat_days = list(repeat_days_raw)
            else:
                repeat_days = [repeat_days_raw]
            repeat_days = [
                str(day).strip().lower()
                for day in repeat_days
                if day is not None and str(day).strip()
            ]
            item_id = item_name

            _LOGGER.debug(
                "schedule_item(is_alarm=%s) raw data=%s target=%s",
                is_alarm,
                dict(call.data),
                target,
            )

            # compute time object from input
            if time_input is None:
                if is_alarm:
                    time_input = self.get_default_alarm_time()
                else:
                    time_input = now.time()

            if isinstance(time_input, str):
                # Accept "HH:MM", "HH:MM:SS", or ISO datetime "YYYY-MM-DDTHH:MM:SS"
                time_str = time_input.split("T")[-1]
                parsed = dt_util.parse_time(time_str)
                if parsed is None:
                    _LOGGER.error("Invalid time format provided: %s", time_input)
                    raise ValueError(f"Invalid time format: {time_input}")
                time_obj = parsed
            elif isinstance(time_input, datetime):
                time_obj = time_input.time()
            else:
                # assume it's already a time object (or None -> use now)
                time_obj = time_input or now.time()

            # combine date/time and make timezone-aware
            if date_input:
                if isinstance(date_input, str):
                    parsed_date = dt_util.parse_date(date_input)
                    if parsed_date is None:
                        _LOGGER.error("Invalid date format provided: %s", date_input)
                        raise ValueError(f"Invalid date format: {date_input}")
                    date_input = parsed_date
                scheduled_time = datetime.combine(date_input, time_obj)
            else:
                scheduled_time = datetime.combine(now.date(), time_obj)

            # Make scheduled_time timezone-aware in Home Assistant's local timezone
            scheduled_time = dt_util.as_local(scheduled_time)

            adjusted_time = self._ensure_future_schedule_time(
                scheduled_time,
                repeat=repeat,
                repeat_days=repeat_days,
                reference=now,
            )
            if adjusted_time is not None:
                scheduled_time = adjusted_time

            # Build item dict
            media_player_target = None
            if target:
                media_player_target = target.get("media_player")
                if media_player_target is None and "media_players" in target:
                    media_player_target = target.get("media_players")

            raw_sound_input = call.data.get("sound_media")
            if raw_sound_input is None:
                raw_sound_input = call.data.get("sound_file")
            descriptor = await self._prepare_sound_descriptor(raw_sound_input, is_alarm=is_alarm)

            media_player_normalized = self._normalize_media_player(media_player_target)
            if not media_player_normalized:
                media_player_normalized = self.get_default_media_player()

            if not media_player_normalized:
                raise ValueError("A media player target is required for playback. Configure a default media player or specify one in the service call.")

            spotify_source_value = self._validate_spotify_player_usage(
                media_player_normalized,
                is_alarm=is_alarm,
                spotify_source=call.data.get(ATTR_SPOTIFY_SOURCE),
            )

            descriptor = await self._ensure_media_player_media_compatibility(
                media_player_normalized,
                descriptor,
            )
            playback_media = self._select_media_identifier_for_player(
                descriptor,
                media_player_normalized,
            )
            if not playback_media:
                playback_media = descriptor.get("original_id") or descriptor.get("resolved_url")

            activation_entity = self._normalize_activation_entity(
                call.data.get("activation_entity"),
                enforce_allowed=True,
                item_name=item_id,
            )
            volume_override = self._normalize_volume_override(call.data.get(ATTR_VOLUME))
            if ATTR_USER_ID in call.data:
                owner_metadata = await self._resolve_owner_metadata(call.data.get(ATTR_USER_ID))
            else:
                owner_metadata = await self._resolve_owner_metadata(
                    None,
                    context=getattr(call, "context", None),
                    use_context_default=True,
                )

            item = {
                "scheduled_time": scheduled_time,
                "scheduled_time_canonical": scheduled_time,
                "media_player": media_player_normalized,
                "message": message,
                "is_alarm": is_alarm,
                "repeat": repeat,
                "repeat_days": repeat_days,
                "status": "scheduled",
                "name": display_name,
                "entity_id": item_id,
                "unique_id": item_id,
                "enabled": True,
                "sound_file": playback_media,
                "sound_media": descriptor,
                "notify_device": call.data.get("notify_device"),
                "announce_time": bool(call.data.get("announce_time", True)),
                "announce_name": bool(call.data.get("announce_name", True)) if is_alarm else True,
                "activation_entity": activation_entity,
                **owner_metadata,
            }

            if spotify_source_value:
                item[ATTR_SPOTIFY_SOURCE] = spotify_source_value
            if volume_override is not None:
                item[ATTR_VOLUME] = volume_override

            # Save and put into memory
            normalized = self._normalize_item_fields(item)
            self._active_items[item_id] = normalized
            await self.storage.async_save(self._active_items)

            # Update central dashboard entity (single switch-like view)
            self._update_dashboard_state()
            self._broadcast_state_refresh()

            # Schedule the trigger and keep unsubscribe handle
            scheduled_time = normalized.get("scheduled_time")
            if isinstance(scheduled_time, str):
                scheduled_time = dt_util.parse_datetime(scheduled_time)
            if isinstance(scheduled_time, datetime):
                self._schedule_trigger(item_id, scheduled_time)
                if normalized.get("is_alarm"):
                    self._bump_last_alarm_time(scheduled_time)
            else:
                _LOGGER.warning(
                    "Unable to schedule %s %s due to invalid scheduled_time after normalization",
                    "alarm" if is_alarm else "reminder",
                    item_id,
                )

            # Publish individual entity state
            self._write_item_state(item_id)

            _LOGGER.info("Scheduled %s %s for %s", "alarm" if is_alarm else "reminder", item_id, scheduled_time)

        except ValueError as err:
            _LOGGER.error("Error scheduling: %s", err)
            raise HomeAssistantError(str(err)) from err
        except Exception as err:
            _LOGGER.error("Error scheduling: %s", err, exc_info=True)
            raise HomeAssistantError("Failed to schedule item") from err

    async def _trigger_item(self, item_id: str) -> None:
        """Trigger the scheduled item."""
        self._cancel_scheduled_trigger(item_id)

        if item_id not in self._active_items:
            _LOGGER.debug("Trigger called for unknown item %s", item_id)
            return

        try:
            item = dict(self._active_items[item_id])
            trigger_time = dt_util.now()
            _LOGGER.debug(
                "Triggering item %s (%s) at %s",
                item_id,
                item.get("name"),
                trigger_time.isoformat(),
            )

            item["last_triggered"] = trigger_time.isoformat()

            # If item is disabled when the trigger fires, mark expired for one-off items
            if not item.get("enabled", True):
                if item.get("repeat", "once") == "once":
                    await self._mark_item_expired(
                        item_id, reason="Disabled when trigger fired"
                    )
                else:
                    item["status"] = "disabled"
                    self._active_items[item_id] = item
                    await self.storage.async_save(self._active_items)
                    self._write_item_state(item_id)
                    self._update_dashboard_state()
                    self._broadcast_state_refresh()
                return

            # Set status to active and persist
            item["status"] = "active"
            self._active_items[item_id] = item
            await self.storage.async_save(self._active_items)

            # Update central dashboard entity
            self._write_item_state(item_id)
            self._update_dashboard_state()
            self._broadcast_state_refresh()

            # Create stop event and start playback in background task
            stop_event = asyncio.Event()
            self._stop_events[item_id] = stop_event

            activation_entity = item.get("activation_entity")
            if activation_entity:
                await self._activate_associated_entity(activation_entity, item_id)

            # Send notification if configured (do not block playback start)
            if item.get("notify_device"):
                # map tag to item_id so global listener can route actions
                self._notification_tag_map[item_id] = item_id
                self.hass.async_create_task(self._send_notification(item_id, item))

            # Start playback non-blocking so stop_item can set stop_event
            task = self.hass.async_create_task(self._start_playback(item_id), name=f"playback_{item_id}")
            self._playback_tasks[item_id] = task

        except Exception as err:
            _LOGGER.error("Error triggering item %s: %s", item_id, err, exc_info=True)
            item["status"] = "error"
            self._active_items[item_id] = item
            await self.storage.async_save(self._active_items)
            self._update_dashboard_state()
            self._broadcast_state_refresh()

    async def _activate_associated_entity(self, entity_id: str, item_id: str) -> None:
        """Turn on an associated entity when an item fires."""
        if not entity_id:
            return
        try:
            await self.hass.services.async_call(
                "homeassistant",
                "turn_on",
                {"entity_id": entity_id},
                blocking=False,
            )
            _LOGGER.debug("Activated entity %s for item %s", entity_id, item_id)
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "Failed to activate entity %s for item %s: %s",
                entity_id,
                item_id,
                err,
                exc_info=True,
            )

    async def _stop_transport(
        self,
        item: Dict[str, Any],
        session: "_PlaybackSession" | None = None,
        *,
        was_active: bool = False,
        reason: str = "stopped",
        is_alarm: bool = True,
    ) -> None:
        """Stop media playback for an item."""
        media_player = item.get("media_player")
        if isinstance(media_player, (list, tuple, set)):
            media_player = next((str(entry) for entry in media_player if entry), None)
        elif media_player:
            media_player = str(media_player)

        if media_player:
            try:
                register_ctx = session._register_service_context if session else None
                await self.media_handler.stop_media_player(
                    media_player,
                    register_context=register_ctx,
                )
            except Exception:
                _LOGGER.exception("Error stopping media player %s", media_player)

    async def _start_playback(self, item_id: str) -> None:
        """Start playback session for an active item (background task)."""
        session: _PlaybackSession | None = None
        _LOGGER.debug("[%s] Starting _start_playback task.", item_id)
        try:
            item = self._active_items.get(item_id)
            if not item:
                _LOGGER.debug("Playback start: item %s not found", item_id)
                return

            stop_event = self._stop_events.get(item_id)
            if not stop_event:
                stop_event = asyncio.Event()
                self._stop_events[item_id] = stop_event

            session = _PlaybackSession(self, item_id, stop_event)
            self._playback_sessions[item_id] = session
            await session.run()

        except asyncio.CancelledError:
            _LOGGER.debug("Playback task for %s cancelled", item_id)
            raise
        except Exception as err:
            _LOGGER.error("Error in playback task for %s: %s", item_id, err, exc_info=True)
            if item_id in self._active_items:
                self._active_items[item_id]["status"] = "error"
                await self.storage.async_save(self._active_items)
                self._write_item_state(item_id)
                self._update_dashboard_state()
                self._broadcast_state_refresh()
        finally:
            _LOGGER.debug("[%s] Entering finally block of _start_playback.", item_id)
            self._playback_tasks.pop(item_id, None)
            self._playback_sessions.pop(item_id, None)
            self._notification_tag_map.pop(item_id, None)
            stop_event = self._stop_events.pop(item_id, None)
            if item_id in self._active_items:
                item = self._active_items[item_id]
                if item.get("status") == "active" and (stop_event is None or stop_event.is_set()):
                    manual_stop = item.pop("_manual_stop_pending", False)
                    if manual_stop:
                        _LOGGER.debug(
                            "[%s] Manual stop detected; letting stop_item handle state persistence",
                            item_id,
                        )
                        return

                    now_dt = dt_util.now()
                    item["status"] = "stopped"
                    item["last_stopped"] = now_dt.isoformat()

                    if isinstance(item.get("scheduled_time"), str):
                        parsed_sched = dt_util.parse_datetime(item.get("scheduled_time"))
                        if parsed_sched is not None:
                            item["scheduled_time"] = parsed_sched

                    next_time = None
                    repeat_value = (item.get("repeat", "once") or "once").lower()
                    canonical_time = item.get("scheduled_time_canonical")
                    if isinstance(canonical_time, str):
                        parsed_canonical = dt_util.parse_datetime(canonical_time)
                        if parsed_canonical is not None:
                            canonical_time = parsed_canonical
                    if isinstance(canonical_time, datetime):
                        canonical_time = dt_util.as_local(canonical_time)
                    elif isinstance(item.get("scheduled_time"), datetime):
                        canonical_time = item.get("scheduled_time")
                    else:
                        canonical_time = None
                    item["scheduled_time_canonical"] = canonical_time
                    if (
                        item.get("enabled", True)
                        and repeat_value != "once"
                        and isinstance(canonical_time, datetime)
                    ):
                        next_time = self._ensure_future_schedule_time(
                            canonical_time,
                            repeat=repeat_value,
                            repeat_days=item.get("repeat_days", []),
                            reference=now_dt,
                            force_advance=True,
                        )
                        if next_time is not None:
                            item["scheduled_time"] = next_time
                            item["status"] = "scheduled"
                            item["scheduled_time_canonical"] = next_time

                    normalized = self._normalize_item_fields(item)
                    self._active_items[item_id] = normalized
                    await self.storage.async_save(self._active_items)

                    if next_time is not None:
                        sched_dt = normalized.get("scheduled_time")
                        if isinstance(sched_dt, str):
                            sched_dt = dt_util.parse_datetime(sched_dt)
                        if isinstance(sched_dt, datetime):
                            self._schedule_trigger(item_id, sched_dt)
                            if normalized.get("is_alarm"):
                                self._bump_last_alarm_time(sched_dt)
                            _LOGGER.debug("Rescheduled repeating item %s for %s", item_id, sched_dt.isoformat())

                    self._write_item_state(item_id)
                    self._update_dashboard_state()
                    self._broadcast_state_refresh()

    async def _send_notification(self, item_id: str, item: dict) -> None:
        """Send notification with action buttons."""
        try:
            device_id = item.get("notify_device")
            if not device_id:
                return

            # Accept either "mobile_app_xxx" or "notify.mobile_app_xxx" or just device id
            # Normalize to notify service target (service = mobile_app_xxx)
            if device_id.startswith("notify."):
                service_target = device_id.split(".", 1)[1]
            elif device_id.startswith("mobile_app_"):
                service_target = device_id
            else:
                # user provided raw id (e.g. mobile_app_sm_a528b) or 'sm_a528b' - assume mobile_app_ prefix if missing 'mobile_app_'
                service_target = device_id if device_id.startswith("mobile_app_") else f"mobile_app_{device_id}"

            message = item.get("message") or f"It's {dt_util.now().strftime('%I:%M %p')}"
            payload = {
                "message": message,
                "title": f"{item.get('name', 'Alarm & Reminder')}",
                "data": {
                    "tag": item_id,
                    "actions": [
                        {"action": "stop", "title": "Stop"},
                        {"action": "snooze", "title": "Snooze"}
                    ]
                }
            }

            _LOGGER.debug("Notify %s -> %s", service_target, payload)
            await self.hass.services.async_call("notify", service_target, payload, blocking=True)

        except Exception as err:
            _LOGGER.error("Error sending notification for item %s: %s", item_id, err, exc_info=True)

    @callback
    def _on_mobile_notification_action(self, event) -> None:
        """Global handler for mobile_app_notification_action events."""
        try:
            tag = event.data.get("tag")
            action = event.data.get("action")
            if not tag:
                return

            # Map tag to item id (we stored item_id as tag earlier)
            item_id = tag if tag in self._active_items else self._notification_tag_map.get(tag)
            if not item_id:
                _LOGGER.debug("Notification action for unknown tag: %s", tag)
                return

            _LOGGER.debug("Notification action '%s' for item %s", action, item_id)
            if action == "stop":
                self.hass.async_create_task(self.stop_item(item_id, self._active_items[item_id]["is_alarm"]))
            elif action == "snooze":
                minutes = self.get_default_snooze_minutes()
                self.hass.async_create_task(
                    self.snooze_item(item_id, minutes, self._active_items[item_id]["is_alarm"])
                )

        except Exception as err:
            _LOGGER.error("Error handling mobile notification action: %s", err, exc_info=True)

    async def stop_item(self, item_id: str, is_alarm: bool, *, reason: str = "stopped") -> None:
        """Stop an active or scheduled item."""
        
        _LOGGER.debug("stop_item called on coordinator: %s", id(self))


        try:
            # Remove domain prefix if present
            raw_id = self._strip_domain(item_id)
            resolved_id = self._resolve_active_item_id(raw_id)

            _LOGGER.debug(
                "Stop request for %s (resolved=%s). Current active items: %s",
                raw_id,
                resolved_id,
                {k: {'name': v.get('name'), 'status': v.get('status')} for k, v in self._active_items.items()},
            )

            # Try to find the item in active items or storage
            item = None
            item_id = resolved_id or raw_id
            if resolved_id:
                item = self._active_items.get(resolved_id)
            else:
                stored = await self.storage.async_load()
                candidate_id = None
                if stored and isinstance(stored, dict):
                    if item_id in stored:
                        candidate_id = item_id
                    else:
                        lowered = item_id.casefold() if isinstance(item_id, str) else None
                        if lowered is not None:
                            for key in stored.keys():
                                if isinstance(key, str) and key.casefold() == lowered:
                                    candidate_id = key
                                    break
                if candidate_id is not None:
                    item = self._normalize_item_fields(stored[candidate_id])
                    self._active_items[candidate_id] = item
                    item_id = candidate_id
                    _LOGGER.debug("Restored item %s from storage", item_id)

            if not item:
                _LOGGER.warning("Item %s not found in active items", raw_id)
                return

            if item.get("is_alarm") != is_alarm:
                _LOGGER.warning("Attempted to stop %s with wrong service: %s", "alarm" if item.get("is_alarm") else "reminder", item_id)
                return

            original_status = item.get("status", "scheduled")
            was_active = original_status == "active"
            media_player_for_restore = self._normalize_media_player(item.get("media_player"))

            self._cancel_scheduled_trigger(item_id)

            # Set stop event if exists (playback loop checks this)
            stop_event = self._stop_events.pop(item_id, None)
            if stop_event:
                stop_event.set()

            playback_task = self._playback_tasks.pop(item_id, None)
            if playback_task:
                item["_manual_stop_pending"] = True
                playback_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await playback_task
                item.pop("_manual_stop_pending", None)

            session = self._playback_sessions.pop(item_id, None)
            if session:
                await session.stop(reason=reason)
            try:
                await self._stop_transport(item, session, was_active=was_active, reason=reason, is_alarm=is_alarm)
            except Exception:
                _LOGGER.exception("Failed to stop transports for %s", item_id)

            if media_player_for_restore:
                try:
                    register_ctx = session._register_service_context if session else None
                    await self.media_handler.restore_player_volume(
                        item_id,
                        media_player_for_restore,
                        register_context=register_ctx,
                    )
                except Exception:  # noqa: BLE001
                    _LOGGER.exception("Failed to restore volume for %s", media_player_for_restore)

            # Update item status and decide whether to reschedule for repeating items
            now_dt = dt_util.now()

            scheduled_time = item.get("scheduled_time")
            if isinstance(scheduled_time, str):
                parsed_sched = dt_util.parse_datetime(scheduled_time)
                if parsed_sched is not None:
                    scheduled_time = parsed_sched
                    item["scheduled_time"] = parsed_sched
            repeat_value = (item.get("repeat", "once") or "once").lower()
            repeat_days = item.get("repeat_days", []) or []

            canonical_time = item.get("scheduled_time_canonical")
            if isinstance(canonical_time, str):
                parsed_canonical = dt_util.parse_datetime(canonical_time)
                if parsed_canonical is not None:
                    canonical_time = parsed_canonical
            if isinstance(canonical_time, datetime):
                canonical_time = dt_util.as_local(canonical_time)
            elif isinstance(scheduled_time, datetime):
                canonical_time = scheduled_time
            else:
                canonical_time = None
            item["scheduled_time_canonical"] = canonical_time

            item["status"] = "stopped"
            item["last_stopped"] = now_dt.isoformat()

            if repeat_value == "once" and reason not in {"snoozed"}:
                expire_reason = "Stopped one-off item after playback" if was_active else "Cancelled one-off item"
                if reason and reason not in {"stopped", "snoozed"}:
                    expire_reason = f"{expire_reason} ({reason})"
                self._active_items[item_id] = item
                await self._mark_item_expired(item_id, reason=expire_reason)
                return

            next_time = None

            if (
                repeat_value != "once"
                and not was_active
                and isinstance(canonical_time, datetime)
                and reason not in {"deleted"}
            ):
                next_time = self._ensure_future_schedule_time(
                    canonical_time,
                    repeat=repeat_value,
                    repeat_days=repeat_days,
                    reference=canonical_time,
                    force_advance=True,
                )
                if next_time is not None:
                    item["scheduled_time"] = next_time
                    item["status"] = "scheduled"
                    item["scheduled_time_canonical"] = next_time
                    _LOGGER.debug(
                        "Skipped pending occurrence for %s; rescheduled for %s",
                        item_id,
                        next_time.isoformat(),
                    )

            if next_time is None:
                should_reschedule = (
                    was_active
                    and item.get("enabled", True)
                    and isinstance(canonical_time, datetime)
                    and repeat_value != "once"
                    and reason not in {"snoozed", "deleted"}
                )

                if should_reschedule:
                    next_time = self._ensure_future_schedule_time(
                        canonical_time,
                        repeat=repeat_value,
                        repeat_days=repeat_days,
                        reference=now_dt,
                        force_advance=True,
                    )
                    if next_time is not None:
                        item["scheduled_time"] = next_time
                        item["status"] = "scheduled"
                        item["scheduled_time_canonical"] = next_time

            normalized = self._normalize_item_fields(item)
            self._active_items[item_id] = normalized
            await self.storage.async_save(self._active_items)

            if next_time is not None:
                sched_dt = normalized.get("scheduled_time")
                if isinstance(sched_dt, str):
                    sched_dt = dt_util.parse_datetime(sched_dt)
                if isinstance(sched_dt, datetime):
                    self._schedule_trigger(item_id, sched_dt)
                    if normalized.get("is_alarm"):
                        self._bump_last_alarm_time(sched_dt)
                    _LOGGER.debug("Rescheduled repeating item %s for %s", item_id, sched_dt.isoformat())

            # Update central dashboard entity
            self._write_item_state(item_id)
            self._update_dashboard_state()
            self._broadcast_state_refresh()
            _LOGGER.info("Successfully stopped %s: %s", "alarm" if is_alarm else "reminder", item_id)

        except Exception as err:
            _LOGGER.error("Error stopping item %s: %s", item_id, err, exc_info=True)

    async def snooze_item(self, item_id: str, minutes: int, is_alarm: bool) -> None:
        """Snooze an active item by stopping and rescheduling it."""
        try:
            resolved_item_id = self._resolve_active_item_id(item_id)
            _LOGGER.debug("Attempting to snooze item %s for %d minutes", resolved_item_id or item_id, minutes)

            if not resolved_item_id:
                _LOGGER.warning(
                    "Item %s not found in active items: %s",
                    item_id,
                    list(self._active_items.keys()),
                )
                return

            item_id = resolved_item_id
            item = self._active_items[item_id]
            
            # Verify item type matches
            if item["is_alarm"] != is_alarm:
                _LOGGER.error(
                    "Cannot snooze %s as %s",
                    "alarm" if is_alarm else "reminder",
                    "reminder" if is_alarm else "alarm"
                )
                return

            # Ensure canonical schedule is preserved before snoozing
            current_sched = item.get("scheduled_time")
            if isinstance(current_sched, str):
                parsed_sched = dt_util.parse_datetime(current_sched)
                if parsed_sched is not None:
                    current_sched = parsed_sched
                    item["scheduled_time"] = parsed_sched
            canonical = item.get("scheduled_time_canonical")
            if isinstance(canonical, str):
                parsed_canonical = dt_util.parse_datetime(canonical)
                if parsed_canonical is not None:
                    canonical = parsed_canonical
            if not isinstance(canonical, datetime):
                canonical = current_sched if isinstance(current_sched, datetime) else None
            if isinstance(canonical, datetime):
                canonical = dt_util.as_local(canonical)
            item["scheduled_time_canonical"] = canonical
            self._active_items[item_id] = item

            # Step 1: Stop the item using stop_item method
            await self.stop_item(item_id, is_alarm, reason="snoozed")
            
            # Wait for stop to complete and verify status
            await asyncio.sleep(1)  # Give time for stop to complete
            
            # Verify item is stopped
            if item_id in self._active_items and self._active_items[item_id]["status"] != "stopped":
                _LOGGER.error("Failed to stop item %s before snoozing", item_id)
                return

            # Step 2: Calculate new time rounded to start of next minute
            now = dt_util.now()
            new_time = now + timedelta(minutes=minutes)
            new_time = new_time.replace(second=0, microsecond=0)
            
            # Step 3: Update item data for rescheduling
            item = self._active_items[item_id]  # Get fresh item data
            item["scheduled_time"] = new_time
            item["status"] = "scheduled"
            if "last_stopped" in item:
                item["last_rescheduled_from"] = item["last_stopped"]
            item["last_stopped"] = now.isoformat()

            # Preserve canonical schedule through snooze
            if "scheduled_time_canonical" not in item or not isinstance(item["scheduled_time_canonical"], datetime):
                item["scheduled_time_canonical"] = canonical
            
            # Step 4: Save to storage
            normalized = self._normalize_item_fields(item)
            self._active_items[item_id] = normalized
            await self.storage.async_save(self._active_items)
            
            # Step 5: Update central dashboard
            self._update_dashboard_state()

            # Step 6: Schedule new trigger
            self._schedule_trigger(item_id, new_time)
            if is_alarm:
                self._bump_last_alarm_time(new_time)

            # Sync entity state
            self._write_item_state(item_id)

            self._broadcast_state_refresh()
            _LOGGER.info(
                "Successfully snoozed %s %s for %d minutes. Will ring at %s",
                "alarm" if is_alarm else "reminder",
                item_id,
                minutes,
                new_time.strftime("%H:%M:%S")
            )

        except Exception as err:
            _LOGGER.error("Error snoozing item %s: %s", item_id, err, exc_info=True)

    async def stop_all_items(self, is_alarm: bool = None) -> None:
        """Stop all active items. If is_alarm is None, stops both alarms and reminders."""
        try:
            stopped_count = 0
            for item_id, item in list(self._active_items.items()):
                if is_alarm is None or item["is_alarm"] == is_alarm:
                    if item.get("status") in ["active", "scheduled"]:
                        await self.stop_item(item_id, item["is_alarm"])
                        stopped_count += 1

            if stopped_count == 0:
                _LOGGER.info("No active items to stop")
            else:
                _LOGGER.info(
                    "Successfully stopped %d %s",
                    stopped_count,
                    "alarms"
                    if is_alarm
                    else "reminders" if is_alarm is not None else "items",
                )

        except Exception as err:
            _LOGGER.error("Error stopping all items: %s", err, exc_info=True)

    async def edit_item(self, item_id: str, changes: dict, is_alarm: bool) -> None:
        """Edit an existing alarm or reminder."""
        try:
            _LOGGER.debug("Starting edit request for %s", item_id)
            _LOGGER.debug("Changes requested: %s", changes)
            _LOGGER.debug("Current active items: %s", 
                         {k: {'name': v.get('name'), 'status': v.get('status')} 
                          for k, v in self._active_items.items()})

            # Remove domain prefix if present
            item_id = self._strip_domain(item_id)

            # Try to find the item by ID or name
            found_id = None
            if item_id in self._active_items:
                found_id = item_id
            else:
                # Try by name
                name_to_find = item_id.replace("_", " ").lower()
                for aid, item in self._active_items.items():
                    if (item.get('name', '').lower() == name_to_find or 
                        aid.lower() == name_to_find):
                        found_id = aid
                        break

            if not found_id:
                _LOGGER.error("Item %s not found in active items: %s", 
                             item_id,
                             [f"{k} ({v.get('name', '')}, {v.get('status', '')})" 
                              for k, v in self._active_items.items()])
                return

            existing_item = self._active_items[found_id]
            original_status = existing_item.get("status", "scheduled")
            item = dict(existing_item)
            changes = dict(changes)
            schedule_changed = False
            
            # Verify item type matches
            if item.get("is_alarm") != is_alarm:
                _LOGGER.error(
                    "Cannot edit %s as %s", 
                    "alarm" if is_alarm else "reminder",
                    "reminder" if is_alarm else "alarm"
                )
                return

            # Process changes
            current_scheduled = item.get("scheduled_time")
            if isinstance(current_scheduled, str):
                parsed_current = dt_util.parse_datetime(current_scheduled)
                if parsed_current is not None:
                    current_scheduled = parsed_current
                    item["scheduled_time"] = parsed_current

            if "time" in changes or "date" in changes:
                time_input = changes.get("time")
                if time_input is None:
                    time_input = (
                        current_scheduled.time()
                        if isinstance(current_scheduled, datetime)
                        else dt_util.now().time()
                    )
                elif isinstance(time_input, str):
                    parsed_time = dt_util.parse_time(time_input)
                    if parsed_time is None:
                        raise ValueError(f"Invalid time format: {time_input}")
                    time_input = parsed_time

                date_input = changes.get("date")
                if date_input is None:
                    date_input = (
                        current_scheduled.date()
                        if isinstance(current_scheduled, datetime)
                        else dt_util.now().date()
                    )
                elif isinstance(date_input, str):
                    parsed_date = dt_util.parse_date(date_input)
                    if parsed_date is None:
                        raise ValueError(f"Invalid date format: {date_input}")
                    date_input = parsed_date

                new_time = datetime.combine(date_input, time_input)
                new_time = dt_util.as_local(new_time)

                if new_time < dt_util.now() and "date" not in changes:
                    new_time = new_time + timedelta(days=1)

                item["scheduled_time"] = new_time
                schedule_changed = True

            # Update other fields if provided
            spotify_source_candidate = item.get(ATTR_SPOTIFY_SOURCE)
            if ATTR_SPOTIFY_SOURCE in changes:
                spotify_source_candidate = changes.pop(ATTR_SPOTIFY_SOURCE)

            if "media_player" in changes:
                item["media_player"] = self._normalize_media_player(changes["media_player"])
            if "media_players" in changes:
                item["media_player"] = self._normalize_media_player(changes["media_players"])

            if "sound_media" in changes or "sound_file" in changes:
                raw_sound = changes.get("sound_media") if "sound_media" in changes else changes.get("sound_file")
                descriptor = await self._prepare_sound_descriptor(raw_sound, is_alarm=is_alarm)
                item["sound_media"] = descriptor
                changes.pop("sound_media", None)
                changes.pop("sound_file", None)

            if "announce_time" in changes:
                item["announce_time"] = bool(changes["announce_time"])

            if is_alarm and "announce_name" in changes:
                item["announce_name"] = bool(changes["announce_name"])

            if "activation_entity" in changes:
                item["activation_entity"] = self._normalize_activation_entity(
                    changes["activation_entity"],
                    enforce_allowed=True,
                    item_name=found_id,
                )

            if ATTR_VOLUME in changes:
                volume_override = self._normalize_volume_override(changes.pop(ATTR_VOLUME))
                if volume_override is not None:
                    item[ATTR_VOLUME] = volume_override
                else:
                    item.pop(ATTR_VOLUME, None)

            if ATTR_USER_ID in changes:
                owner_metadata = await self._resolve_owner_metadata(changes.pop(ATTR_USER_ID))
                item.update(owner_metadata)
                changes.pop(ATTR_USER_NAME, None)

            incoming_name = changes.get("name", None)
            if incoming_name is not None:
                slug = self._slugify_name(incoming_name)
                if not slug:
                    slug = existing_item.get("name") or (
                        self._get_next_available_id("alarm") if is_alarm else self._get_next_available_id("reminder")
                    )
                if not is_alarm:
                    for other_id, other_item in self._active_items.items():
                        if other_id != found_id and other_item.get("name") == slug:
                            raise ValueError(f"Reminder name already exists: {incoming_name}")
                item["name"] = slug
            changes.pop("name", None)

            for field in ["message", "repeat", "notify_device", "enabled"]:
                if field in changes:
                    value = changes[field]
                    if field == "repeat" and isinstance(value, str):
                        value = value.lower()
                    item[field] = value
                    if field == "repeat":
                        schedule_changed = True

            if "repeat_days" in changes:
                rd = changes["repeat_days"]
                if rd is None:
                    rd = []
                elif not isinstance(rd, list):
                    rd = list(rd)
                item["repeat_days"] = [
                    str(day).strip().lower()
                    for day in rd
                    if day is not None and str(day).strip()
                ]
                schedule_changed = True

            media_player_target = item.get("media_player") or self.get_default_media_player()
            target_profile = (
                self.get_media_player_profile(media_player_target) if media_player_target else None
            )
            target_family = (target_profile or {}).get("family")
            if target_family != "spotify":
                spotify_source_candidate = None
            if media_player_target and isinstance(item.get("sound_media"), dict):
                descriptor = await self._ensure_media_player_media_compatibility(
                    media_player_target,
                    item["sound_media"],
                )
                item["sound_media"] = descriptor
                playback_id = self._select_media_identifier_for_player(
                    descriptor,
                    media_player_target,
                )
                if playback_id:
                    item["sound_file"] = playback_id

            validated_spotify_source = self._validate_spotify_player_usage(
                media_player_target,
                is_alarm=is_alarm,
                spotify_source=spotify_source_candidate,
            )
            if validated_spotify_source:
                item[ATTR_SPOTIFY_SOURCE] = validated_spotify_source
            else:
                item.pop(ATTR_SPOTIFY_SOURCE, None)

            item_enabled = item.get("enabled", True)
            if item_enabled and isinstance(item.get("scheduled_time"), datetime):
                adjusted_time = self._ensure_future_schedule_time(
                    item.get("scheduled_time"),
                    repeat=item.get("repeat", "once"),
                    repeat_days=item.get("repeat_days", []),
                    reference=dt_util.now(),
                )
                if adjusted_time is not None:
                    item["scheduled_time"] = adjusted_time
                    schedule_changed = True

            if isinstance(item.get("scheduled_time"), datetime) and (
                schedule_changed or not isinstance(item.get("scheduled_time_canonical"), datetime)
            ):
                item["scheduled_time_canonical"] = item["scheduled_time"]

            # Store updated item
            normalized = self._normalize_item_fields(item)
            new_enabled = normalized.get("enabled", True)

            if new_enabled:
                normalized["status"] = "scheduled"
            else:
                if original_status in ("scheduled", "active", "expired"):
                    normalized["status"] = "disabled"
                else:
                    normalized["status"] = original_status or "disabled"

            self._active_items[found_id] = normalized

            # Edited items that remain disabled should not keep any scheduled triggers
            self._cancel_scheduled_trigger(found_id)

            # Save to storage
            await self.storage.async_save(self._active_items)

            # Update entity state
            if new_enabled and normalized.get("scheduled_time"):
                self._schedule_trigger(found_id, normalized.get("scheduled_time"))
                if normalized.get("is_alarm"):
                    self._bump_last_alarm_time(normalized.get("scheduled_time"))

            self._write_item_state(found_id)

            # Refresh dashboard summary
            self._update_dashboard_state()

            # Force update of sensors
            self._broadcast_state_refresh()

            _LOGGER.info(
                "Successfully edited %s: %s", 
                "alarm" if is_alarm else "reminder",
                found_id
            )

        except ValueError as err:
            _LOGGER.error("Error editing item %s: %s", item_id, err)
            raise HomeAssistantError(str(err)) from err
        except Exception as err:
            _LOGGER.error("Error editing item %s: %s", item_id, err, exc_info=True)
            raise HomeAssistantError("Failed to edit item") from err

    async def duplicate_item(self, item_id: str, is_alarm: bool) -> str:
        """Duplicate an existing alarm or reminder."""
        raw_id = self._strip_domain(item_id)
        resolved_id = self._resolve_active_item_id(raw_id)
        candidate_id = resolved_id or raw_id

        existing_item = self._active_items.get(candidate_id)
        if existing_item is None:
            stored = await self.storage.async_load()
            if isinstance(stored, dict) and candidate_id in stored:
                existing_item = self._normalize_item_fields(stored[candidate_id])
                self._active_items[candidate_id] = existing_item

        if existing_item is None:
            raise HomeAssistantError(f"Item not found: {item_id}")

        if existing_item.get("is_alarm") != is_alarm:
            raise HomeAssistantError(
                f"Cannot duplicate {'alarm' if not is_alarm else 'reminder'} with the {'alarm' if is_alarm else 'reminder'} service"
            )

        duplicated_id = self._duplicate_name_for_item(existing_item, is_alarm=is_alarm)
        duplicated = dict(existing_item)
        duplicated["name"] = duplicated_id
        duplicated["entity_id"] = duplicated_id
        duplicated["unique_id"] = duplicated_id
        duplicated.pop("id", None)

        scheduled_time = duplicated.get("scheduled_time")
        if isinstance(scheduled_time, str):
            parsed_scheduled_time = dt_util.parse_datetime(scheduled_time)
            if parsed_scheduled_time is not None:
                scheduled_time = parsed_scheduled_time
                duplicated["scheduled_time"] = parsed_scheduled_time

        scheduled_time_canonical = duplicated.get("scheduled_time_canonical")
        if isinstance(scheduled_time_canonical, str):
            parsed_canonical = dt_util.parse_datetime(scheduled_time_canonical)
            if parsed_canonical is not None:
                duplicated["scheduled_time_canonical"] = parsed_canonical

        normalized = self._normalize_item_fields(duplicated)
        enabled = bool(normalized.get("enabled", True))

        if enabled:
            if isinstance(scheduled_time, datetime) and scheduled_time >= dt_util.now():
                normalized["status"] = "scheduled"
            else:
                normalized["status"] = "expired"
        else:
            normalized["status"] = "disabled"

        self._active_items[duplicated_id] = normalized
        await self.storage.async_save(self._active_items)

        self._cancel_scheduled_trigger(duplicated_id)
        if enabled and isinstance(normalized.get("scheduled_time"), datetime) and normalized["status"] == "scheduled":
            self._schedule_trigger(duplicated_id, normalized.get("scheduled_time"))
            if is_alarm:
                self._bump_last_alarm_time(normalized.get("scheduled_time"))

        self._write_item_state(duplicated_id)
        self._update_dashboard_state()
        self._broadcast_state_refresh(action="duplicated")
        return duplicated_id

    async def delete_item(self, item_id: str, is_alarm: bool) -> None:
        """Delete a specific item."""
        try:
            resolved_item_id = self._resolve_active_item_id(item_id)
            if not resolved_item_id:
                _LOGGER.warning("Item %s not found for deletion", item_id)
                return

            item_id = resolved_item_id
            item = self._active_items[item_id]
            
            # Verify item type matches
            if item["is_alarm"] != is_alarm:
                _LOGGER.error(
                    "Cannot delete %s as %s", 
                    "alarm" if is_alarm else "reminder",
                    "reminder" if is_alarm else "alarm"
                )
                return

            await self.stop_item(item_id, is_alarm, reason="deleted")

            entity_id = self._entity_id_for_item(item_id, item)

            # Remove from storage and active items
            await self.storage.async_delete_item(item_id)
            self._active_items.pop(item_id, None)

            # Remove entity
            self.hass.states.async_remove(entity_id)
            entity_registry = er.async_get(self.hass)
            switch_entity_id = entity_registry.async_get_entity_id(
                "switch", DOMAIN, f"{DOMAIN}_{item_id}"
            )
            if switch_entity_id:
                entity_registry.async_remove(switch_entity_id)

            self._fire_state_event(entity_id, action="removed")

            # Refresh dashboard view after removal
            self._update_dashboard_state()

            _LOGGER.info(
                "Successfully deleted %s: %s",
                "alarm" if is_alarm else "reminder",
                item_id
            )

        except Exception as err:
            _LOGGER.error("Error deleting item %s: %s", item_id, err, exc_info=True)

    async def delete_all_items(self, is_alarm: bool = None) -> None:
        """Delete all items. If is_alarm is None, deletes both alarms and reminders."""
        try:
            targets: list[tuple[str, bool]] = []
            for item_id, item in list(self._active_items.items()):
                if is_alarm is None or item.get("is_alarm") == is_alarm:
                    targets.append((item_id, item.get("is_alarm", False)))

            deleted_count = 0
            for item_id, is_alarm_item in targets:
                try:
                    await self.delete_item(item_id, is_alarm_item)
                    deleted_count += 1
                except Exception as err:
                    _LOGGER.error(
                        "Error deleting %s during bulk delete: %s",
                        item_id,
                        err,
                        exc_info=True,
                    )

            if deleted_count > 0:
                self._update_dashboard_state()
                _LOGGER.info(
                    "Successfully deleted %d %s",
                    deleted_count,
                    "alarms" if is_alarm else "reminders" if is_alarm is not None else "items"
                )
            else:
                _LOGGER.info("No items to delete")

        except Exception as err:
            _LOGGER.error("Error deleting all items: %s", err, exc_info=True)

    async def reschedule_item(self, item_id: str, changes: dict, is_alarm: bool) -> None:
        """Reschedule a stopped or completed item."""
        try:
            # Remove domain prefix if present
            item_id = self._strip_domain(item_id)
            
            _LOGGER.debug("Attempting to reschedule item %s with changes: %s", item_id, changes)
            _LOGGER.debug("Current active items: %s", self._active_items)
            
            if item_id not in self._active_items:
                # Try to find item in storage
                stored_items = await self.storage.async_load()
                if item_id in stored_items:
                    self._active_items[item_id] = self._normalize_item_fields(stored_items[item_id])
                    _LOGGER.debug("Restored item %s from storage", item_id)
                else:
                    _LOGGER.error("Item %s not found in storage or active items", item_id)
                    return
                
            item = self._active_items[item_id]
            original_status = item.get("status", "scheduled")

            if isinstance(item.get("scheduled_time"), str):
                parsed_sched = dt_util.parse_datetime(item.get("scheduled_time"))
                if parsed_sched is not None:
                    item["scheduled_time"] = parsed_sched

            schedule_changed = False

            # Verify item type matches
            if item["is_alarm"] != is_alarm:
                _LOGGER.error(
                    "Cannot reschedule %s as %s",
                    "alarm" if is_alarm else "reminder",
                    "reminder" if is_alarm else "alarm"
                )
                return

            self._cancel_scheduled_trigger(item_id)

            # Calculate new scheduled time
            now = dt_util.now()
            if "time" in changes or "date" in changes:
                time_input = changes.get("time", item["scheduled_time"].time())
                if isinstance(time_input, str):
                    parsed = dt_util.parse_time(time_input)
                    if parsed is None:
                        raise ValueError(f"Invalid time format: {time_input}")
                    time_input = parsed
                date_input = changes.get("date", item["scheduled_time"].date())
                if isinstance(date_input, str):
                    parsed_date = dt_util.parse_date(date_input)
                    if parsed_date is None:
                        raise ValueError(f"Invalid date format: {date_input}")
                    date_input = parsed_date
                new_time = datetime.combine(date_input, time_input)
                new_time = dt_util.as_local(new_time)
                
                # Validate future time
                if new_time < now:
                    if "date" not in changes:  # Only adjust if date wasn't explicitly set
                        new_time = new_time + timedelta(days=1)
                
                item["scheduled_time"] = new_time
                schedule_changed = True

            # Update other fields if provided
            if "media_player" in changes:
                item["media_player"] = self._normalize_media_player(changes["media_player"])
            if "media_players" in changes:
                item["media_player"] = self._normalize_media_player(changes["media_players"])

            if "announce_time" in changes:
                item["announce_time"] = bool(changes["announce_time"])

            if "activation_entity" in changes:
                item["activation_entity"] = self._normalize_activation_entity(
                    changes["activation_entity"],
                    enforce_allowed=True,
                    item_name=item_id,
                )

            if ATTR_VOLUME in changes:
                volume_override = self._normalize_volume_override(changes.pop(ATTR_VOLUME))
                if volume_override is not None:
                    item[ATTR_VOLUME] = volume_override
                else:
                    item.pop(ATTR_VOLUME, None)

            if "sound_media" in changes or "sound_file" in changes:
                raw_sound = changes.get("sound_media") if "sound_media" in changes else changes.get("sound_file")
                descriptor = await self._prepare_sound_descriptor(raw_sound, is_alarm=is_alarm)
                item["sound_media"] = descriptor
                changes.pop("sound_media", None)
                changes.pop("sound_file", None)

            incoming_name = changes.get("name", None)
            if incoming_name is not None:
                slug = self._slugify_name(incoming_name)
                if not slug:
                    slug = item.get("name") or (
                        self._get_next_available_id("alarm") if is_alarm else self._get_next_available_id("reminder")
                    )
                if not is_alarm:
                    for other_id, other_item in self._active_items.items():
                        if other_id != item_id and other_item.get("name") == slug:
                            raise ValueError(f"Reminder name already exists: {incoming_name}")
                item["name"] = slug
            changes.pop("name", None)

            for field in ["message", "repeat", "notify_device", "enabled"]:
                if field in changes:
                    value = changes[field]
                    if field == "repeat" and isinstance(value, str):
                        value = value.lower()
                    item[field] = value
                    if field == "repeat":
                        schedule_changed = True

            if "repeat_days" in changes:
                rd = changes["repeat_days"]
                if rd is None:
                    rd = []
                elif not isinstance(rd, list):
                    rd = list(rd)
                item["repeat_days"] = [
                    str(day).strip().lower()
                    for day in rd
                    if day is not None and str(day).strip()
                ]
                schedule_changed = True

            media_player_target = item.get("media_player") or self.get_default_media_player()
            if media_player_target and isinstance(item.get("sound_media"), dict):
                descriptor = await self._ensure_media_player_media_compatibility(
                    media_player_target,
                    item["sound_media"],
                )
                item["sound_media"] = descriptor
                playback_id = self._select_media_identifier_for_player(
                    descriptor,
                    media_player_target,
                )
                if playback_id:
                    item["sound_file"] = playback_id

            item_enabled = item.get("enabled", True)
            if item_enabled and isinstance(item.get("scheduled_time"), datetime):
                adjusted_time = self._ensure_future_schedule_time(
                    item.get("scheduled_time"),
                    repeat=item.get("repeat", "once"),
                    repeat_days=item.get("repeat_days", []),
                    reference=dt_util.now(),
                )
                if adjusted_time is not None:
                    item["scheduled_time"] = adjusted_time
                    schedule_changed = True

            # Update status based on enabled flag
            new_enabled = item.get("enabled", True)
            if new_enabled:
                item["status"] = "scheduled"
            else:
                if original_status in ("scheduled", "active", "expired"):
                    item["status"] = "disabled"
                else:
                    item["status"] = original_status or "disabled"
            if "last_stopped" in item:
                item["last_rescheduled_from"] = item["last_stopped"]
            
            # Create stop event if needed
            if item_id not in self._stop_events:
                self._stop_events[item_id] = asyncio.Event()
            
            # Save changes
            if isinstance(item.get("scheduled_time"), datetime) and (
                schedule_changed or not isinstance(item.get("scheduled_time_canonical"), datetime)
            ):
                item["scheduled_time_canonical"] = item["scheduled_time"]
            normalized = self._normalize_item_fields(item)
            self._active_items[item_id] = normalized
            await self.storage.async_save(self._active_items)

            scheduled_time = normalized.get("scheduled_time")
            if isinstance(scheduled_time, str):
                scheduled_time = dt_util.parse_datetime(scheduled_time)
            schedule_now = new_enabled and isinstance(scheduled_time, datetime)
            if schedule_now:
                if (
                    normalized.get("repeat", "once") == "once"
                    and scheduled_time <= dt_util.now()
                ):
                    await self._mark_item_expired(
                        item_id, reason="Rescheduled time already passed"
                    )
                    return
            
            # Update entity state
            self._write_item_state(item_id)

            # Schedule new trigger with task name if enabled
            if schedule_now:
                self._schedule_trigger(item_id, scheduled_time)
                if is_alarm:
                    self._bump_last_alarm_time(scheduled_time)

            _LOGGER.info(
                "Successfully rescheduled %s %s for %s",
                "alarm" if is_alarm else "reminder",
                item_id,
                scheduled_time.strftime("%Y-%m-%d %H:%M:%S") if schedule_now else normalized.get("scheduled_time")
            )

            self._update_dashboard_state()
            self._broadcast_state_refresh()

        except ValueError as err:
            _LOGGER.error("Error rescheduling item %s: %s", item_id, err)
            raise HomeAssistantError(str(err)) from err
        except Exception as err:
            _LOGGER.error("Error rescheduling item %s: %s", item_id, err, exc_info=True)
            raise HomeAssistantError("Failed to reschedule item") from err

    def _update_dashboard_state(self) -> None:
        """Update the dashboard sensor with full lists of alarms and reminders."""
        try:
            alarms = {}
            reminders = {}
            overall_state = "idle"
            for iid, item in self._active_items.items():
                serialized = self._serialize_item_state(item)
                summary = {
                    "name": serialized.get("name"),
                    "status": serialized.get("status"),
                    "scheduled_time": serialized.get("scheduled_time"),
                    "message": serialized.get("message"),
                    "is_alarm": bool(serialized.get("is_alarm")),
                    "sound_file": serialized.get("sound_file"),
                    "media_player": serialized.get("media_player"),
                    "repeat": serialized.get("repeat"),
                    "repeat_days": serialized.get("repeat_days", []),
                    "notify_device": serialized.get("notify_device"),
                    "announce_time": serialized.get("announce_time", True),
                    "activation_entity": serialized.get("activation_entity"),
                    ATTR_USER_ID: serialized.get(ATTR_USER_ID),
                    ATTR_USER_NAME: serialized.get(ATTR_USER_NAME),
                }
                if item.get("status") == "active":
                    overall_state = "active"
                if item.get("is_alarm"):
                    alarms[iid] = summary
                else:
                    reminders[iid] = summary

            attrs = {
                "alarms": alarms,
                "reminders": reminders,
                "alarm_count": len(alarms),
                "reminder_count": len(reminders),
                "last_updated": dt_util.now().isoformat(),
                "default_media_player": self._default_media_player,
                "default_alarm_sound": DEFAULT_ALARM_SOUND,
                "default_reminder_sound": DEFAULT_REMINDER_SOUND,
                "default_snooze_minutes": self.get_default_snooze_minutes(),
                "active_press_mode": self.get_active_press_mode(),
                "restrict_non_admin_ownership": self.get_restrict_non_admin_ownership(),
                "hidden_media_players": self.get_hidden_media_players(),
                "allowed_activation_entities": sorted(self._allowed_activation_entities)
                if self._allowed_activation_entities
                else [],
            }
            self.hass.states.async_set(DASHBOARD_ENTITY_ID, overall_state, attrs)
        except Exception as err:
            _LOGGER.error("Failed to update dashboard state: %s", err, exc_info=True)



