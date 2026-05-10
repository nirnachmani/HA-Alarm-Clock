"""The HA Alarm Clock integration."""
from __future__ import annotations

import logging
import voluptuous as vol
from typing import Union, List, Dict
from datetime import time, datetime

from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.const import ATTR_NAME, ATTR_ENTITY_ID  # Use HA's built-in constants
from homeassistant.helpers import device_registry as dr
from homeassistant.components import websocket_api

from .const import (
    DOMAIN,
    SERVICE_SET_ALARM,
    SERVICE_SET_REMINDER,
    SERVICE_STOP_ALARM,    
    SERVICE_SNOOZE_ALARM,  
    SERVICE_STOP_REMINDER,
    SERVICE_SNOOZE_REMINDER,
    SERVICE_STOP_ALL_ALARMS,  
    SERVICE_STOP_ALL_REMINDERS,  
    SERVICE_STOP_ALL,  
    SERVICE_EDIT_ALARM,  
    SERVICE_EDIT_REMINDER,
    SERVICE_DUPLICATE_ALARM,
    SERVICE_DUPLICATE_REMINDER,
    SERVICE_DELETE_ALARM, 
    SERVICE_DELETE_REMINDER,  
    SERVICE_DELETE_ALL_ALARMS,  
    SERVICE_DELETE_ALL_REMINDERS,  
    SERVICE_DELETE_ALL,  
    ATTR_DATETIME,
    ATTR_MESSAGE,
    ATTR_ALARM_ID,        
    ATTR_REMINDER_ID,
    ATTR_SNOOZE_MINUTES,
    ATTR_MEDIA_PLAYER,
    ATTR_NAME,
    ATTR_NOTIFY_DEVICE,  
    ATTR_NOTIFY_TITLE,      
    ATTR_SPOTIFY_SOURCE,
    ATTR_VOLUME,
    DEFAULT_SNOOZE_MINUTES,
    DEFAULT_NAME,
    CONF_MEDIA_PLAYER,
    CONF_HIDDEN_MEDIA_PLAYERS,
    CONF_ALLOWED_ACTIVATION_ENTITIES,
    CONF_ENABLE_LLM,
    CONF_ACTIVE_PRESS_MODE,
    CONF_DEFAULT_SNOOZE_MINUTES,
    DEFAULT_ENABLE_LLM,
    DEFAULT_HIDDEN_MEDIA_PLAYERS,
    ALARM_ENTITY_DOMAIN,
    REMINDER_ENTITY_DOMAIN,
)

from .coordinator import AlarmAndReminderCoordinator
from .media_player import MediaHandler
from .intents import async_setup_intents
from .llm_functions import async_setup_llm_api, async_cleanup_llm_api
# from .sensor import async_setup_entry as async_setup_sensor_entry
# sensor platform removed; scheduling moved to coordinator and switches

__all__ = ["AlarmAndReminderCoordinator"]

_LOGGER = logging.getLogger(__name__)

REPEAT_OPTIONS = [
    "once",
    "daily",
    "weekdays",
    "weekends",
    "custom",
]

REPEAT_DAY_OPTIONS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

SOUND_MEDIA_SCHEMA = vol.Schema(
    {
        vol.Required("media_content_id"): cv.string,
        vol.Optional("media_content_type"): cv.string,
    },
    extra=vol.ALLOW_EXTRA,
)

SOUND_INPUT_SCHEMA = vol.Any(SOUND_MEDIA_SCHEMA, cv.string)


def _normalize_option_entity_list(raw_value) -> list[str]:
    """Normalize an arbitrary options value into a flat entity-id list."""
    if raw_value in (None, ""):
        return []

    normalized: list[str] = []
    for entity in cv.ensure_list(raw_value):
        if not entity:
            continue
        try:
            normalized.append(cv.entity_id(str(entity)))
        except vol.Invalid:
            continue
    return sorted(set(normalized))


def _normalize_hidden_media_players_option(
    hass: HomeAssistant,
    raw_value,
    *,
    default_media_player: str | None,
) -> list[str]:
    """Normalize the hidden-media-player option against current state."""
    available_media_players = set(hass.states.async_entity_ids("media_player"))
    hidden_media_players: list[str] = []
    for entity_id in _normalize_option_entity_list(raw_value):
        if entity_id == default_media_player:
            continue
        if entity_id in available_media_players:
            hidden_media_players.append(entity_id)
    return hidden_media_players


def _resolve_media_player_from_call(call: ServiceCall) -> str | None:
    """Extract a single media player entity_id from a service call."""
    candidate = call.data.get(ATTR_MEDIA_PLAYER)
    target_entity: str | list | tuple | set | None = call.data.get(ATTR_ENTITY_ID)

    # Handle nested payloads like {"entity_id": ...} from automations/scripts
    if isinstance(candidate, dict):
        if candidate.get("entity_id"):
            candidate = candidate["entity_id"]
        elif candidate.get("entity_ids"):
            candidate = candidate["entity_ids"]

    service_target = getattr(call, "target", None)
    if not target_entity and service_target is not None:
        entity_ids = getattr(service_target, "entity_id", None)
        if entity_ids:
            target_entity = entity_ids

    # Some automation contexts store target entity ids under call.data["target"]
    if not target_entity:
        target_payload = call.data.get("target") if isinstance(call.data, dict) else None
        if isinstance(target_payload, dict):
            target_entity = target_payload.get("entity_id") or target_payload.get("entity_ids")

    if target_entity:
        if isinstance(target_entity, (list, tuple, set)):
            filtered = [str(item).strip() for item in target_entity if item]
            if len(filtered) > 1:
                raise vol.Invalid("Select a single media player entity as the target.")
            target_entity = filtered[0] if filtered else None
        else:
            target_entity = str(target_entity).strip()

        if target_entity:
            if candidate and str(candidate).strip() and str(candidate).strip() != target_entity:
                raise vol.Invalid(
                    "Specify the media player either via the media player target or the media_player field, not both."
                )
            candidate = target_entity

    if isinstance(candidate, (list, tuple, set)):
        candidate = next((str(item).strip() for item in candidate if item), None)

    if not candidate:
        # If other target selectors were used without an entity, warn so users pick an entity instead.
        if any(call.data.get(key) for key in ("area_id", "device_id", "floor_id", "label_id")):
            raise vol.Invalid(
                "Select a specific media player entity for this service instead of an area, device, floor, or label."
            )
        return None

    candidate = str(candidate).strip()
    if not candidate:
        return None

    if "." not in candidate:
        candidate = f"media_player.{candidate}"

    try:
        return cv.entity_id(candidate)
    except vol.Invalid as err:
        raise vol.Invalid(f"Invalid media player entity: {candidate}") from err


def _validate_target(call: ServiceCall) -> dict[str | None, str | None]:
    """Extract media-player target information from a service call."""
    _LOGGER.debug(
        "validate_target call.data=%s target=%s target.entity_id=%s",
        dict(call.data),
        getattr(call, "target", None),
        getattr(getattr(call, "target", None), "entity_id", None),
    )
    media_player = _resolve_media_player_from_call(call)
    _LOGGER.debug("Validated target media_player=%s", media_player)
    if media_player:
        return {"media_player": media_player}
    return {}


def _normalize_target_mutation(call: ServiceCall, data: dict) -> None:
    """Normalize optional target updates in mutable service payloads."""
    media_player = _resolve_media_player_from_call(call)

    if media_player:
        data[ATTR_MEDIA_PLAYER] = media_player
    elif ATTR_MEDIA_PLAYER in data and data[ATTR_MEDIA_PLAYER]:
        try:
            data[ATTR_MEDIA_PLAYER] = cv.entity_id(data[ATTR_MEDIA_PLAYER])
        except vol.Invalid as err:
            raise vol.Invalid(f"Invalid media player entity: {data[ATTR_MEDIA_PLAYER]}") from err
    else:
        data.pop(ATTR_MEDIA_PLAYER, None)

    # Remove entity-based selectors we don't persist directly.
    data.pop(ATTR_ENTITY_ID, None)
    for key in ("area_id", "device_id", "floor_id", "label_id"):
        data.pop(key, None)


def _validate_activation_entity(value):
    """Accept an entity ID or a target selector payload."""
    if value in (None, ""):
        return None

    candidate = value
    if isinstance(candidate, dict):
        candidate = candidate.get("entity_id") or candidate.get("entity")

    if isinstance(candidate, (list, tuple, set)):
        candidate = next((item for item in candidate if item), None)

    if candidate is None:
        raise vol.Invalid("Select an entity for activation_entity or leave it blank")

    candidate = str(candidate).strip()
    if not candidate:
        return None

    try:
        return cv.entity_id(candidate)
    except vol.Invalid as err:
        raise vol.Invalid(f"Invalid activation entity: {candidate}") from err


def _validate_repeat(value):
    """Normalize and validate repeat option."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized not in REPEAT_OPTIONS:
        raise vol.Invalid(f"Invalid repeat option: {value}")
    return normalized


def _validate_repeat_days(value):
    """Normalize and validate repeat_days input."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = [value]

    normalized: list[str] = []
    for item in items:
        if item is None:
            continue
        candidate = str(item).strip().lower()
        if not candidate:
            continue
        if candidate not in REPEAT_DAY_OPTIONS:
            raise vol.Invalid(f"Invalid repeat day: {item}")
        normalized.append(candidate)
    return normalized


def _validate_volume(value):
    """Normalize optional volume input to 0.0-1.0 range."""
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise vol.Invalid("Volume must be numeric") from None
    if number < 0:
        number = 0.0
    if number <= 1:
        return number
    if number <= 100:
        return min(1.0, number / 100.0)
    return 1.0

ALARM_ID_VALIDATOR = vol.Any(cv.entity_id, cv.string)
REMINDER_ID_VALIDATOR = vol.Any(cv.entity_id, cv.string)

SERVICE_RESCHEDULE_ALARM = "reschedule_alarm"
SERVICE_RESCHEDULE_REMINDER = "reschedule_reminder"

DEFAULT_ALARM_SOUND = "/media/local/Alarms/birds.mp3"
DEFAULT_REMINDER_SOUND = "/media/local/Alarms/ringtone.mp3"

async def _get_coordinator(hass: HomeAssistant) -> AlarmAndReminderCoordinator | None:
    """Get the coordinator from hass.data."""
    if DOMAIN in hass.data:
        if "coordinator" in hass.data[DOMAIN]:
            return hass.data[DOMAIN]["coordinator"]
        for data in hass.data[DOMAIN].values():
            if isinstance(data, dict) and "coordinator" in data:
                return data["coordinator"]
    _LOGGER.error("HA Alarm Clock coordinator not found")
    return None


async def _async_get_or_create_coordinator(hass: HomeAssistant) -> AlarmAndReminderCoordinator:
    """Return the singleton coordinator, creating it if needed."""
    domain_data = hass.data.setdefault(DOMAIN, {})

    coordinator = domain_data.get("coordinator")
    if coordinator:
        return coordinator

    for data in domain_data.values():
        if isinstance(data, dict) and "coordinator" in data:
            coordinator = data["coordinator"]
            domain_data["coordinator"] = coordinator
            return coordinator

    media_handler = MediaHandler(
        hass,
        DEFAULT_ALARM_SOUND,
        DEFAULT_REMINDER_SOUND,
    )
    coordinator = AlarmAndReminderCoordinator(hass, media_handler)
    domain_data["coordinator"] = coordinator
    return coordinator


PLATFORMS = ["switch"]

async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the HA Alarm Clock integration (minimal)."""
    # Only initialize the top-level data container here.
    hass.data.setdefault(DOMAIN, {})
    # return True

    try:
        # Initialize data structure
        hass.data.setdefault(DOMAIN, {})

        coordinator = await _async_get_or_create_coordinator(hass)

        # Initialize the DOMAIN data structure
        hass.data[DOMAIN].setdefault("entities", [])  # Initialize the entities list

    # Dynamic schema based on available media players
        ALARM_SERVICE_SCHEMA = vol.Schema({
            vol.Optional("time"): cv.time,
            vol.Optional("date"): cv.date,
            vol.Optional(ATTR_NAME): str,  # Optional name for alarms
            vol.Optional(ATTR_MESSAGE): cv.string,
            vol.Optional(ATTR_MEDIA_PLAYER): cv.entity_id,
            vol.Optional("announce_time", default=True): cv.boolean,
            vol.Optional("announce_name", default=True): cv.boolean,
            vol.Optional("activation_entity"): _validate_activation_entity,
            vol.Optional("repeat", default="once"): vol.In(REPEAT_OPTIONS),
            vol.Optional("repeat_days"): vol.All(
                cv.ensure_list,
                [vol.In(["mon", "tue", "wed", "thu", "fri", "sat", "sun"])]
            ),
            vol.Optional("sound_media"): SOUND_INPUT_SCHEMA,
            vol.Optional("sound_file"): cv.string,
            vol.Optional(ATTR_SPOTIFY_SOURCE): cv.string,
            vol.Optional(ATTR_NOTIFY_DEVICE): vol.Any(
                cv.string,  # Single device
                vol.All(cv.ensure_list, [cv.string])  # List of devices
            ),
            vol.Optional(ATTR_VOLUME): _validate_volume,
        })

        REMINDER_SERVICE_SCHEMA = vol.Schema({
            vol.Optional("time"): cv.time,
            vol.Required(ATTR_NAME): str,  # Required name for reminders
            vol.Optional("date"): cv.date,
            vol.Optional(ATTR_MESSAGE): cv.string,
            vol.Optional(ATTR_MEDIA_PLAYER): cv.entity_id,
            vol.Optional("announce_time", default=True): cv.boolean,
            vol.Optional("announce_name", default=True): cv.boolean,
            vol.Optional("activation_entity"): _validate_activation_entity,
            vol.Optional("repeat", default="once"): vol.In(REPEAT_OPTIONS),
            vol.Optional("repeat_days"): vol.All(
                cv.ensure_list,
                [vol.In(["mon", "tue", "wed", "thu", "fri", "sat", "sun"])]
            ),
            vol.Optional("sound_media"): SOUND_INPUT_SCHEMA,
            vol.Optional("sound_file"): cv.string,
            vol.Optional(ATTR_SPOTIFY_SOURCE): cv.string,
            vol.Optional(ATTR_NOTIFY_DEVICE): vol.Any(
                 cv.string,  # Single device
                vol.All(cv.ensure_list, [cv.string])  # List of devices
            ),
            vol.Optional(ATTR_VOLUME): _validate_volume,
        })

        # Store coordinator for future access
        hass.data[DOMAIN]["coordinator"] = coordinator

        async def async_schedule_alarm(call: ServiceCall):
            """Handle the alarm service call."""
            target = _validate_target(call)
            await coordinator.schedule_item(call, is_alarm=True, target=target)

        async def async_schedule_reminder(call: ServiceCall):
            """Handle the reminder service call."""
            target = _validate_target(call)
            await coordinator.schedule_item(call, is_alarm=False, target=target)

        # Register services with updated schema
        hass.services.async_register(
            DOMAIN,
            SERVICE_SET_ALARM,
            async_schedule_alarm,
            schema=ALARM_SERVICE_SCHEMA,
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_SET_REMINDER,
            async_schedule_reminder,
            schema=REMINDER_SERVICE_SCHEMA,
        )

        websocket_api.async_register_command(hass, websocket_resolve_media_metadata)

        # Register reminder-specific services
        async def async_stop_reminder(call: ServiceCall):
            """Handle stop reminder service call."""
            try:
                reminder_id = call.data.get(ATTR_REMINDER_ID)
                coordinator = await _get_coordinator(hass)
                if coordinator:
                    _LOGGER.debug("Found coordinator. Active items: %s", coordinator._active_items)
                    await coordinator.stop_item(reminder_id, is_alarm=False)
            except Exception as err:
                _LOGGER.error("Error stopping reminder: %s", err, exc_info=True)

        hass.services.async_register(
            DOMAIN,
            SERVICE_STOP_REMINDER,
            async_stop_reminder,
            schema=vol.Schema({
                vol.Required(ATTR_REMINDER_ID): REMINDER_ID_VALIDATOR,
            }),
        )

        async def async_stop_alarm(call: ServiceCall):
            """Handle stop alarm service call."""
            alarm_id = call.data.get("alarm_id")
            await coordinator.stop_item(alarm_id, is_alarm=True)

        # Register alarm control services
        hass.services.async_register(
            DOMAIN,
            "stop_alarm",
            async_stop_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
            }),
        )

        # Register new services
        hass.services.async_register(
            DOMAIN,
            "stop_all_alarms",
            async_stop_all_alarms,
            schema=vol.Schema({}),
        )

        hass.services.async_register(
            DOMAIN,
            "stop_all_reminders",
            async_stop_all_reminders,
            schema=vol.Schema({}),
        )

        hass.services.async_register(
            DOMAIN,
            "stop_all",
            async_stop_all,
            schema=vol.Schema({}),
        )

        async def async_edit_alarm(call: ServiceCall):
            """Handle edit alarm service call."""
            try:
                # Create a mutable copy of the data
                data = dict(call.data)
                _normalize_target_mutation(call, data)
                alarm_id = data.pop("alarm_id")
                
                coordinator = None
                for entry_id, data_entry in hass.data[DOMAIN].items():
                    if isinstance(data_entry, dict) and "coordinator" in data_entry:
                        coordinator = data_entry["coordinator"]
                        break
                
                if coordinator:
                    await coordinator.edit_item(alarm_id, data, is_alarm=True)
                
            except HomeAssistantError as err:
                _LOGGER.error("Error editing alarm: %s", err)
                raise
            except Exception as err:
                _LOGGER.error("Error editing alarm: %s", err, exc_info=True)
                raise HomeAssistantError("Failed to edit alarm") from err

        async def async_edit_reminder(call: ServiceCall):
            """Handle edit reminder service call."""
            try:
                # Create a mutable copy of the data
                data = dict(call.data)
                _normalize_target_mutation(call, data)
                reminder_id = data.pop("reminder_id")
                
                coordinator = None
                for entry_id, data_entry in hass.data[DOMAIN].items():
                    if isinstance(data_entry, dict) and "coordinator" in data_entry:
                        coordinator = data_entry["coordinator"]
                        break
                
                if coordinator:
                    await coordinator.edit_item(reminder_id, data, is_alarm=False)
                
            except HomeAssistantError as err:
                _LOGGER.error("Error editing reminder: %s", err)
                raise
            except Exception as err:
                _LOGGER.error("Error editing reminder: %s", err, exc_info=True)
                raise HomeAssistantError("Failed to edit reminder") from err

        async def async_duplicate_alarm(call: ServiceCall):
            """Handle duplicate alarm service call."""
            try:
                coordinator = await _get_coordinator(hass)
                if coordinator:
                    await coordinator.duplicate_item(call.data["alarm_id"], is_alarm=True)
            except HomeAssistantError as err:
                _LOGGER.error("Error duplicating alarm: %s", err)
                raise
            except Exception as err:
                _LOGGER.error("Error duplicating alarm: %s", err, exc_info=True)
                raise HomeAssistantError("Failed to duplicate alarm") from err

        async def async_duplicate_reminder(call: ServiceCall):
            """Handle duplicate reminder service call."""
            try:
                coordinator = await _get_coordinator(hass)
                if coordinator:
                    await coordinator.duplicate_item(call.data["reminder_id"], is_alarm=False)
            except HomeAssistantError as err:
                _LOGGER.error("Error duplicating reminder: %s", err)
                raise
            except Exception as err:
                _LOGGER.error("Error duplicating reminder: %s", err, exc_info=True)
                raise HomeAssistantError("Failed to duplicate reminder") from err

        # Register edit services
        hass.services.async_register(
            DOMAIN,
            SERVICE_EDIT_ALARM,
            async_edit_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
                vol.Optional("time"): cv.time,
                vol.Optional("date"): cv.date,
                vol.Optional("name"): cv.string,
                vol.Optional("message"): cv.string,
                vol.Optional("media_player"): cv.entity_id,
                vol.Optional("announce_time"): cv.boolean,
                vol.Optional("announce_name"): cv.boolean,
                vol.Optional("activation_entity"): _validate_activation_entity,
                vol.Optional("repeat"): _validate_repeat,
                vol.Optional("repeat_days"): _validate_repeat_days,
                vol.Optional(ATTR_SPOTIFY_SOURCE): cv.string,
                vol.Optional(ATTR_VOLUME): _validate_volume,
            }, extra=vol.ALLOW_EXTRA),
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_EDIT_REMINDER,
            async_edit_reminder,
            schema=vol.Schema({
                vol.Required("reminder_id"): REMINDER_ID_VALIDATOR,
                vol.Optional("time"): cv.time,
                vol.Optional("date"): cv.date,
                vol.Optional("name"): cv.string,
                vol.Optional("message"): cv.string,
                vol.Optional("media_player"): cv.entity_id,
                vol.Optional("announce_time"): cv.boolean,
                vol.Optional("announce_name"): cv.boolean,
                vol.Optional("activation_entity"): _validate_activation_entity,
                vol.Optional("repeat"): _validate_repeat,
                vol.Optional("repeat_days"): _validate_repeat_days,
                vol.Optional(ATTR_VOLUME): _validate_volume,
            }, extra=vol.ALLOW_EXTRA),
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DUPLICATE_ALARM,
            async_duplicate_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
            }),
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DUPLICATE_REMINDER,
            async_duplicate_reminder,
            schema=vol.Schema({
                vol.Required("reminder_id"): REMINDER_ID_VALIDATOR,
            }),
        )

        # Set up intents
        if DOMAIN not in hass.data:
            hass.data[DOMAIN] = {}
            await async_setup_intents(hass)  # Only setup intents once

        # Register delete services at the end of async_setup
        hass.services.async_register(
            DOMAIN,
            SERVICE_DELETE_ALARM,
            async_delete_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
            })
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DELETE_REMINDER,
            async_delete_reminder,
            schema=vol.Schema({
                vol.Required("reminder_id"): REMINDER_ID_VALIDATOR,
            })
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DELETE_ALL_ALARMS,
            async_delete_all_alarms,
            schema=vol.Schema({})
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DELETE_ALL_REMINDERS,
            async_delete_all_reminders,
            schema=vol.Schema({})
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_DELETE_ALL,
            async_delete_all,
            schema=vol.Schema({})
        )

        async def async_snooze_alarm(call: ServiceCall) -> None:
            """Handle snooze alarm service call."""
            try:
                alarm_id = call.data.get("alarm_id")
                minutes = call.data.get("minutes")
                coordinator = None
                
                for entry_id, data in hass.data[DOMAIN].items():
                    if isinstance(data, dict) and "coordinator" in data:
                        coordinator = data["coordinator"]
                        break
                
                if coordinator:
                    default_minutes = coordinator.get_default_snooze_minutes()
                    duration = minutes if minutes is not None else default_minutes
                    await coordinator.snooze_item(alarm_id, int(duration), is_alarm=True)
                else:
                    _LOGGER.error("No coordinator found")
                    
            except Exception as err:
                _LOGGER.error("Error snoozing alarm: %s", err, exc_info=True)

        async def async_snooze_reminder(call: ServiceCall) -> None:
            """Handle snooze reminder service call."""
            try:
                reminder_id = call.data.get("reminder_id")
                minutes = call.data.get("minutes")
                coordinator = None
                
                for entry_id, data in hass.data[DOMAIN].items():
                    if isinstance(data, dict) and "coordinator" in data:
                        coordinator = data["coordinator"]
                        break
                
                if coordinator:
                    default_minutes = coordinator.get_default_snooze_minutes()
                    duration = minutes if minutes is not None else default_minutes
                    await coordinator.snooze_item(reminder_id, int(duration), is_alarm=False)
                else:
                    _LOGGER.error("No coordinator found")
                    
            except Exception as err:
                _LOGGER.error("Error snoozing reminder: %s", err, exc_info=True)

        # Register snooze services
        hass.services.async_register(
            DOMAIN,
            SERVICE_SNOOZE_ALARM,
            async_snooze_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
                vol.Optional("minutes"): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=60)
                ),
            })
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_SNOOZE_REMINDER,
            async_snooze_reminder,
            schema=vol.Schema({
                vol.Required("reminder_id"): REMINDER_ID_VALIDATOR,
                vol.Optional("minutes"): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=60)
                ),
            })
        )

        async def async_reschedule_alarm(call: ServiceCall) -> None:
            """Handle reschedule alarm service call."""
            try:
                alarm_id = call.data.get("alarm_id")
                changes = {k: v for k, v in call.data.items() if k != "alarm_id"}
                _normalize_target_mutation(call, changes)
                
                coordinator = None
                for entry_id, data in hass.data[DOMAIN].items():
                    if isinstance(data, dict) and "coordinator" in data:
                        coordinator = data["coordinator"]
                        break
                
                if coordinator:
                    await coordinator.reschedule_item(alarm_id, changes, is_alarm=True)
                else:
                    _LOGGER.error("No coordinator found")
                    
            except Exception as err:
                _LOGGER.error("Error rescheduling alarm: %s", err, exc_info=True)

        async def async_reschedule_reminder(call: ServiceCall) -> None:
            """Handle reschedule reminder service call."""
            try:
                reminder_id = call.data.get("reminder_id")
                changes = {k: v for k, v in call.data.items() if k != "reminder_id"}
                _normalize_target_mutation(call, changes)
                
                coordinator = None
                for entry_id, data in hass.data[DOMAIN].items():
                    if isinstance(data, dict) and "coordinator" in data:
                        coordinator = data["coordinator"]
                        break
                
                if coordinator:
                    await coordinator.reschedule_item(reminder_id, changes, is_alarm=False)
                else:
                    _LOGGER.error("No coordinator found")
                    
            except Exception as err:
                _LOGGER.error("Error rescheduling reminder: %s", err, exc_info=True)

        # Register new services
        hass.services.async_register(
            DOMAIN,
            SERVICE_RESCHEDULE_ALARM,
            async_reschedule_alarm,
            schema=vol.Schema({
                vol.Required("alarm_id"): ALARM_ID_VALIDATOR,
                vol.Optional("time"): cv.time,
                vol.Optional("date"): cv.date,
                vol.Optional("message"): cv.string,
                vol.Optional("media_player"): cv.entity_id,
                vol.Optional("announce_time"): cv.boolean,
                vol.Optional("activation_entity"): _validate_activation_entity,
                vol.Optional("repeat"): _validate_repeat,
                vol.Optional("repeat_days"): _validate_repeat_days,
                vol.Optional(ATTR_VOLUME): _validate_volume,
            }, extra=vol.ALLOW_EXTRA),
        )

        hass.services.async_register(
            DOMAIN,
            SERVICE_RESCHEDULE_REMINDER,
            async_reschedule_reminder,
            schema=vol.Schema({
                vol.Required("reminder_id"): REMINDER_ID_VALIDATOR,
                vol.Optional("time"): cv.time,
                vol.Optional("date"): cv.date,
                vol.Optional("message"): cv.string,
                vol.Optional("media_player"): cv.entity_id,
                vol.Optional("announce_time"): cv.boolean,
                vol.Optional("activation_entity"): _validate_activation_entity,
                vol.Optional("repeat"): _validate_repeat,
                vol.Optional("repeat_days"): _validate_repeat_days,
                vol.Optional(ATTR_VOLUME): _validate_volume,
            }, extra=vol.ALLOW_EXTRA),
        )

        return True

    except Exception as err:
        _LOGGER.error("Error setting up integration: %s", err, exc_info=True)
        return False

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a config entry: create/store coordinator and forward platforms."""
    try:
        hass.data.setdefault(DOMAIN, {})
        # Ensure per-entry container
        hass.data[DOMAIN].setdefault(entry.entry_id, {})
        entry_store = hass.data[DOMAIN][entry.entry_id]

        default_media_player = entry.options.get(CONF_MEDIA_PLAYER)
        stored_hidden_media_players = _normalize_option_entity_list(
            entry.options.get(
                CONF_HIDDEN_MEDIA_PLAYERS,
                DEFAULT_HIDDEN_MEDIA_PLAYERS,
            )
        )
        cleaned_hidden_media_players = _normalize_hidden_media_players_option(
            hass,
            entry.options.get(
                CONF_HIDDEN_MEDIA_PLAYERS,
                DEFAULT_HIDDEN_MEDIA_PLAYERS,
            ),
            default_media_player=default_media_player,
        )
        if stored_hidden_media_players != cleaned_hidden_media_players:
            updated_options = dict(entry.options)
            updated_options[CONF_HIDDEN_MEDIA_PLAYERS] = cleaned_hidden_media_players
            hass.config_entries.async_update_entry(entry, options=updated_options)

        entry.async_on_unload(entry.add_update_listener(update_listener))

        # Create or reuse the shared coordinator (tests patch AlarmAndReminderCoordinator)
        coordinator = await _async_get_or_create_coordinator(hass)

        coordinator.set_default_media_player(default_media_player)
        coordinator.set_hidden_media_players(cleaned_hidden_media_players)
        allowed_option = (
            entry.options.get(CONF_ALLOWED_ACTIVATION_ENTITIES)
            if CONF_ALLOWED_ACTIVATION_ENTITIES in entry.options
            else None
        )
        coordinator.set_allowed_activation_entities(allowed_option)
        coordinator.set_default_snooze_minutes(
            entry.options.get(CONF_DEFAULT_SNOOZE_MINUTES)
        )
        coordinator.set_active_press_mode(
            entry.options.get(CONF_ACTIVE_PRESS_MODE)
        )
        # Publish updated defaults/allow-list to the dashboard sensor immediately
        coordinator._update_dashboard_state()

        # Attach stable id and create device so switches group under one device
        # Use a fixed shared device identifier so all entries use the same device
        coordinator.id = DOMAIN  # stable identifier shared across entries
        device_registry = dr.async_get(hass)
        device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,  # Link device to the actual ConfigEntry
            identifiers={(DOMAIN, coordinator.id)},
            name=DEFAULT_NAME,
            model="HA Alarm Clock",
            sw_version="0.0.0",
            manufacturer="@nirnachmani",
        )


        # Store coordinator and entities list for this entry
        entry_store["coordinator"] = coordinator
        entry_store.setdefault("entities", [])

        # Let coordinator restore saved items if it supports it
        if hasattr(coordinator, "async_load_items"):
            await coordinator.async_load_items()

        # --- Service handlers (ensure services.yaml remains for UI metadata) ---
        def _extract_target(call: ServiceCall) -> tuple[str | None, bool | None]:
            """Return (item_id, is_alarm) inferred from call data/target."""
            raw_target: str | None = None
            is_alarm: bool | None = None

            _LOGGER.debug("Extract target raw call data=%s target=%s", call.data, getattr(call, "target", None))

            entity_data = call.data.get("entity_id")
            if entity_data:
                raw_target = entity_data[0] if isinstance(entity_data, (list, tuple, set)) else entity_data

            if "alarm_id" in call.data:
                raw_target = call.data["alarm_id"]
                is_alarm = True
            elif "reminder_id" in call.data:
                raw_target = call.data["reminder_id"]
                is_alarm = False
            else:
                entity_ids = None
                target_info = getattr(call, "target", None)
                _LOGGER.debug("Extract target target_info=%s (%s)", target_info, type(target_info))
                if target_info:
                    if isinstance(target_info, dict):
                        entity_ids = target_info.get("entity_id")
                    else:
                        entity_ids = getattr(target_info, "entity_id", None)
                        if not entity_ids:
                            entity_ids = getattr(target_info, "entity_ids", None)
                if entity_ids:
                    entity = entity_ids[0] if isinstance(entity_ids, (list, tuple, set)) else entity_ids
                    raw_target = entity
                    if isinstance(entity, str):
                        if entity.startswith(f"{ALARM_ENTITY_DOMAIN}."):
                            is_alarm = True
                        elif entity.startswith(f"{REMINDER_ENTITY_DOMAIN}."):
                            is_alarm = False

            if isinstance(raw_target, str):
                raw_target = coordinator._strip_domain(raw_target)

            if raw_target and is_alarm is None:
                item = coordinator._active_items.get(raw_target)
                if item is not None:
                    is_alarm = bool(item.get("is_alarm"))

            return raw_target, is_alarm

        async def _handle_set_alarm(call: ServiceCall) -> None:
            target = _validate_target(call)
            await coordinator.schedule_item(call, True, target)

        async def _handle_set_reminder(call: ServiceCall) -> None:
            target = _validate_target(call)
            await coordinator.schedule_item(call, False, target)

        async def _handle_stop(call: ServiceCall) -> None:
            item_id, is_alarm = _extract_target(call)
            _LOGGER.debug("Service stop resolved target: id=%s is_alarm=%s", item_id, is_alarm)
            if item_id is not None and is_alarm is not None:
                await coordinator.stop_item(item_id, is_alarm)

        async def _handle_snooze(call: ServiceCall) -> None:
            minutes = call.data.get("minutes")
            item_id, is_alarm = _extract_target(call)
            _LOGGER.debug("Service snooze resolved target: id=%s is_alarm=%s", item_id, is_alarm)
            if item_id is not None and is_alarm is not None:
                default_minutes = coordinator.get_default_snooze_minutes()
                duration = minutes if minutes is not None else default_minutes
                await coordinator.snooze_item(item_id, int(duration), is_alarm)

        async def _handle_delete(call: ServiceCall) -> None:
            item_id, is_alarm = _extract_target(call)
            _LOGGER.debug("Service delete resolved target: id=%s is_alarm=%s", item_id, is_alarm)
            if item_id is not None and is_alarm is not None:
                await coordinator.delete_item(item_id, is_alarm)

        # Register services under domain (these names match services.yaml)
        hass.services.async_register(
            DOMAIN, "set_alarm", _handle_set_alarm)
        hass.services.async_register(
            DOMAIN, "set_reminder", _handle_set_reminder)
        hass.services.async_register(
            DOMAIN, "stop", _handle_stop)
        hass.services.async_register(
            DOMAIN, "snooze", _handle_snooze)
        hass.services.async_register(
            DOMAIN, "delete", _handle_delete)
        # ...register other services (reschedule, edit, stop_all, etc.) similarly...
        # -----------------------------------------------------------------------

        enable_llm = entry.options.get(CONF_ENABLE_LLM, DEFAULT_ENABLE_LLM)
        if enable_llm:
            try:
                await async_setup_llm_api(hass)
                _LOGGER.info("LLM API setup completed for HA Alarm Clock")
            except Exception as llm_err:
                _LOGGER.warning("Failed to setup LLM API (non-critical): %s", llm_err)

        # Forward platforms and finish setup
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        return True

    except Exception as err:
        _LOGGER.error("Error setting up config entry: %s", err, exc_info=True)
        return False

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        try:
            await async_cleanup_llm_api(hass)
            _LOGGER.info("LLM API cleanup completed")
        except Exception as llm_err:
            _LOGGER.debug("Error cleaning up LLM API: %s", llm_err)        
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok

async def update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Update listener."""
    await hass.config_entries.async_reload(entry.entry_id)

async def async_stop_all_alarms(call: ServiceCall):
    """Handle stop all alarms service call."""
    try:
        hass = call.hass
        coordinator = None
        _LOGGER.debug(
            "Service handler async_stop_all_alarms (post-setup duplicate) invoked: data=%s",
            call.data,
        )
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.stop_all_items(is_alarm=True)
    except Exception as err:
        _LOGGER.error("Error stopping all alarms: %s", err)

async def async_stop_all_reminders(call: ServiceCall):
    """Handle stop all reminders service call."""
    try:
        hass = call.hass
        coordinator = None
        _LOGGER.debug(
            "Service handler async_stop_all_reminders (post-setup duplicate) invoked: data=%s",
            call.data,
        )
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.stop_all_items(is_alarm=False)
    except Exception as err:
        _LOGGER.error("Error stopping all reminders: %s", err)

async def async_stop_all(call: ServiceCall):
    """Handle stop all service call."""
    try:
        hass = call.hass
        coordinator = None
        _LOGGER.debug(
            "Service handler async_stop_all (post-setup duplicate) invoked: data=%s",
            call.data,
        )
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.stop_all_items()
    except Exception as err:
        _LOGGER.error("Error stopping all items: %s", err)

async def async_delete_alarm(call: ServiceCall) -> None:
    """Handle delete alarm service call."""
    try:
        hass = call.hass
        alarm_id = call.data.get("alarm_id")
        coordinator = None
        
        # Look for coordinator in hass.data instead of call.data
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.delete_item(alarm_id, is_alarm=True)
        else:
            _LOGGER.error("No coordinator found")
            
    except Exception as err:
        _LOGGER.error("Error deleting alarm: %s", err, exc_info=True)

async def async_delete_reminder(call: ServiceCall) -> None:
    """Handle delete reminder service call."""
    try:
        hass = call.hass
        reminder_id = call.data.get("reminder_id")
        coordinator = None
        
        # Look for coordinator in hass.data instead of call.data
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.delete_item(reminder_id, is_alarm=False)
        else:
            _LOGGER.error("No coordinator found")
            
    except Exception as err:
        _LOGGER.error("Error deleting reminder: %s", err, exc_info=True)

async def async_delete_all_alarms(call: ServiceCall) -> None:
    """Handle delete all alarms service call."""
    try:
        hass = call.hass
        coordinator = None
        # Look for coordinator in hass.data instead of call.data
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.delete_all_items(is_alarm=True)
        else:
            _LOGGER.error("No coordinator found")
            
    except Exception as err:
        _LOGGER.error("Error deleting all alarms: %s", err, exc_info=True)

async def async_delete_all_reminders(call: ServiceCall) -> None:
    """Handle delete all reminders service call."""
    try:
        hass = call.hass
        coordinator = None
        # Look for coordinator in hass.data instead of call.data
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.delete_all_items(is_alarm=False)
        else:
            _LOGGER.error("No coordinator found")
            
    except Exception as err:
        _LOGGER.error("Error deleting all reminders: %s", err, exc_info=True)

async def async_delete_all(call: ServiceCall) -> None:
    """Handle delete all service call."""
    try:
        hass = call.hass
        coordinator = None
        # Look for coordinator in hass.data instead of call.data
        for entry_id, data in hass.data[DOMAIN].items():
            if isinstance(data, dict) and "coordinator" in data:
                coordinator = data["coordinator"]
                break
        
        if coordinator:
            await coordinator.delete_all_items()
        else:
            _LOGGER.error("No coordinator found")
            
    except Exception as err:
        _LOGGER.error("Error deleting all items: %s", err, exc_info=True)


async def _async_handle_resolve_media_ws(hass, connection, msg):
    """Resolve additional media metadata for the companion cards."""
    coordinator = await _async_get_or_create_coordinator(hass)
    if coordinator is None:
        connection.send_error(msg["id"], "not_ready", "Coordinator unavailable")
        return

    try:
        result = await coordinator.async_resolve_media_metadata(
            msg["media_content_id"],
            msg.get("media_content_type"),
            msg.get("provider"),
        )
    except HomeAssistantError as err:
        connection.send_error(msg["id"], "resolve_failed", str(err))
        return


@websocket_api.websocket_command(
    {
        vol.Required("type"): f"{DOMAIN}/resolve_media",
        vol.Required("media_content_id"): cv.string,
        vol.Optional("media_content_type"): cv.string,
        vol.Optional("provider"): cv.string,
    }
)
@websocket_api.async_response
async def websocket_resolve_media_metadata(hass, connection, msg):
    """Resolve media metadata for HA Alarm Clock cards."""
    await _async_handle_resolve_media_ws(hass, connection, msg)


    connection.send_result(msg["id"], result)
