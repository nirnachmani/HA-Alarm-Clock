"""LLM Tools for reminder management."""
import logging
import re
import unicodedata
from datetime import datetime, time
from typing import Any, Iterable

import voluptuous as vol
from homeassistant.core import HomeAssistant
from homeassistant.helpers import llm
from homeassistant.util.json import JsonObjectType
from homeassistant.util import dt as dt_util

from .const import DASHBOARD_ENTITY_ID, DOMAIN
from .llm_media_player import resolve_room_media_player_candidates

_LOGGER = logging.getLogger(__name__)


def _slugify_label(value: str | None) -> str:
    """Mirror coordinator slugging so lookups match stored names."""
    if not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_only.lower()
    replaced = re.sub(r"[^a-z0-9]+", "_", lowered)
    collapsed = re.sub(r"_+", "_", replaced)
    return collapsed.strip("_")


def _normalize_terms(value: str | None) -> set[str]:
    """Return a set of comparable variants for a name/id."""
    terms: set[str] = set()
    if value is None:
        return terms
    text = str(value).strip()
    if not text:
        return terms

    lower = text.lower()
    terms.add(lower)

    humanized = lower.replace("_", " ").strip()
    if humanized:
        terms.add(humanized)

    slug = _slugify_label(text)
    if slug:
        terms.add(slug)
        slug_humanized = slug.replace("_", " ").strip()
        if slug_humanized:
            terms.add(slug_humanized)

    return terms


def _humanize_label(value: str | None) -> str:
    """Convert a slug/identifier into a user-facing label."""
    if not value:
        return ""
    text = str(value).strip().replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    return " ".join(word.capitalize() for word in text.split(" "))


def _terms_match(search_terms: set[str], candidate_terms: set[str]) -> bool:
    """Check if any normalized variant overlaps between sets."""
    for needle in search_terms:
        if not needle:
            continue
        for candidate in candidate_terms:
            if not candidate:
                continue
            if needle == candidate or needle in candidate or candidate in needle:
                return True
    return False


def _get_allowed_activation_entities(hass: HomeAssistant) -> set[str]:
    """Return the configured activation-entity allow list from the dashboard state."""
    allowed: set[str] = set()
    state = hass.states.get(DASHBOARD_ENTITY_ID)
    if not state:
        return allowed
    attr_value: Any = state.attributes.get("allowed_activation_entities")
    if isinstance(attr_value, str):
        allowed.add(attr_value)
    elif isinstance(attr_value, Iterable):
        for entry in attr_value:
            if isinstance(entry, str) and entry:
                allowed.add(entry)
    return allowed


class SetReminderTool(llm.Tool):
    """Tool for setting a reminder."""

    name = "set_reminder"
    description = "Set a new reminder at a specific time with a task description. Use this when the user wants to be reminded about something."
    response_instruction = """
    Confirm to the user that the reminder has been set with the time and task.
    Keep your response concise and friendly, in plain text without formatting.
    """

    parameters = vol.Schema(
        {
            vol.Required(
                "time",
                description="Time for the reminder in HH:MM (24-hour). Example: 07:30 or 18:45.",
            ): str,
            vol.Required(
                "name",
                description="Descriptive task name for the reminder. Example: 'take medicine'.",
            ): str,
            vol.Optional(
                "date",
                description="Optional date for the reminder (YYYY-MM-DD). Defaults to today when omitted.",
            ): str,
            vol.Optional(
                "repeat",
                description="Repeat pattern: once, daily, weekdays, weekends, or custom.",
            ): vol.In(["once", "daily", "weekdays", "weekends", "custom"]),
            vol.Optional(
                "repeat_days",
                description="If repeat is custom, provide days using mon/tue/wed/thu/fri/sat/sun.",
            ): [str],
            vol.Optional(
                "message",
                description="Optional additional message to announce when the reminder rings.",
            ): str,
            vol.Optional(
                "media_player",
                description="Specific media player entity_id to use for this reminder.",
            ): str,
            vol.Optional(
                "sound_media",
                description="Media selector payload to play when the reminder fires.",
            ): dict,
            vol.Optional(
                "sound_file",
                description="Direct media path/URL to play when the reminder fires.",
            ): str,
            vol.Optional(
                "announce_time",
                description="Whether to announce the current time in the spoken message.",
            ): bool,
            vol.Optional(
                "notify_device",
                description="Mobile app notify device ID (e.g., mobile_app_pixel7).",
            ): vol.Any(str, [str]),
            vol.Optional(
                "activation_entity",
                description="Entity to activate when the reminder fires (must be in allowed list).",
            ): str,
            vol.Optional(
                "volume",
                description="Optional playback volume (0-1 normalized or 0-100%).",
            ): vol.Any(int, float, str),
        }
    )

    def wrap_response(self, response: dict) -> dict:
        response.setdefault("instruction", self.response_instruction)
        return response

    def _validate_time(self, time_str: str) -> tuple[bool, str]:
        """Validate time format and return (is_valid, error_message)."""
        pattern = r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$"
        if not re.match(pattern, time_str):
            return False, "Time must be in HH:MM format (24-hour). Example: 07:30 or 14:00"
        return True, ""

    def _validate_repeat_days(self, days: list[str] | None) -> tuple[bool, str]:
        """Validate repeat days."""
        if not days:
            return True, ""
        valid_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        for day in days:
            if day.lower() not in valid_days:
                return (
                    False,
                    f"Invalid day: {day}. Use: mon, tue, wed, thu, fri, sat, sun",
                )
        return True, ""

    async def async_call(
        self,
        hass: HomeAssistant,
        tool_input: llm.ToolInput,
        llm_context: llm.LLMContext,
    ) -> JsonObjectType:
        """Call the tool to set a reminder."""
        time_str = tool_input.tool_args["time"]
        name = tool_input.tool_args["name"]
        repeat_days = tool_input.tool_args.get("repeat_days")
        message = tool_input.tool_args.get("message", "")
        date_str = tool_input.tool_args.get("date")
        repeat_value = tool_input.tool_args.get("repeat")
        media_player = tool_input.tool_args.get("media_player")
        sound_media = tool_input.tool_args.get("sound_media")
        sound_file = tool_input.tool_args.get("sound_file")
        announce_time = tool_input.tool_args.get("announce_time")
        notify_device = tool_input.tool_args.get("notify_device")
        activation_entity = tool_input.tool_args.get("activation_entity")
        volume_value = tool_input.tool_args.get("volume")

        _LOGGER.info("Setting reminder '%s' at %s", name, time_str)

        # Validate time
        is_valid, error_msg = self._validate_time(time_str)
        if not is_valid:
            return {"error": error_msg}

        # Validate repeat days
        is_valid, error_msg = self._validate_repeat_days(repeat_days)
        if not is_valid:
            return {"error": error_msg}

        try:
            # Parse time
            hour, minute = map(int, time_str.split(':'))
            time_obj = time(hour, minute)

            # Optional activation entity validation
            if activation_entity:
                allowed_entities = _get_allowed_activation_entities(hass)
                if allowed_entities and activation_entity not in allowed_entities:
                    allowed_fmt = ", ".join(sorted(allowed_entities))
                    return {
                        "error": f"Activation entity '{activation_entity}' is not allowed. Choose one of: {allowed_fmt}"
                    }

            # Get coordinator
            coordinator = None
            for entry_id, data in hass.data.get(DOMAIN, {}).items():
                if isinstance(data, dict) and "coordinator" in data:
                    coordinator = data["coordinator"]
                    break

            if not coordinator:
                return {"error": "Reminder system coordinator not found"}

            auto_selected_media_player_name: str | None = None
            if not media_player:
                area_resolution = resolve_room_media_player_candidates(
                    hass,
                    conversation_device_id=getattr(tool_input, "device_id", None),
                )
                selected_player = area_resolution.get("selected")
                if selected_player:
                    media_player = selected_player["entity_id"]
                    auto_selected_media_player_name = selected_player["name"]

            # Create service call data
            service_data: dict[str, Any] = {
                "time": time_obj,
                "name": name,
                "message": message or name,
            }

            if date_str:
                service_data["date"] = date_str

            # Repeat handling mirrors service schema expectations
            if repeat_value:
                service_data["repeat"] = repeat_value
            elif repeat_days:
                service_data["repeat"] = "custom"

            if repeat_days:
                if service_data.get("repeat", "custom") == "custom":
                    service_data["repeat_days"] = repeat_days

            # Create a mock ServiceCall-like object
            class MockServiceCall:
                def __init__(self, data, context):
                    self.data = data
                    self.context = context

            if sound_media:
                service_data["sound_media"] = sound_media
            elif sound_file:
                service_data["sound_file"] = sound_file

            if announce_time is not None:
                service_data["announce_time"] = bool(announce_time)

            if notify_device:
                service_data["notify_device"] = notify_device

            if activation_entity:
                service_data["activation_entity"] = activation_entity

            if volume_value is not None:
                try:
                    service_data["volume"] = float(volume_value)
                except (TypeError, ValueError):
                    return {"error": "Volume must be numeric"}

            call = MockServiceCall(service_data, llm_context.context)
            target = {}
            if media_player:
                target["media_player"] = media_player

            # Schedule the reminder using the coordinator
            await coordinator.schedule_item(call, is_alarm=False, target=target)

            response_msg = f"Reminder '{name}' set for {time_str}"
            if repeat_days:
                response_msg += f" on {', '.join(repeat_days)}"
            if auto_selected_media_player_name:
                response_msg += f" using {auto_selected_media_player_name}"

            return self.wrap_response({
                "success": True,
                "message": response_msg,
                "time": time_str,
                "name": name,
            })

        except Exception as e:
            _LOGGER.error("Error setting reminder: %s", e, exc_info=True)
            return {"error": f"Failed to set reminder: {str(e)}"}


class ListRemindersTool(llm.Tool):
    """Tool for listing all reminders."""

    name = "list_reminders"
    description = "List all currently set reminders. Use this when the user asks what reminders are set or wants to see their reminders."
    response_instruction = """
    Present the list of reminders to the user in a clear, conversational way.
    Include the time and task name for each reminder.
    If there are no reminders, let the user know in a friendly way.
    Keep your response concise and in plain text without formatting.
    """

    parameters = vol.Schema({})

    def wrap_response(self, response: dict) -> dict:
        response["instruction"] = self.response_instruction
        return response

    async def async_call(
        self,
        hass: HomeAssistant,
        tool_input: llm.ToolInput,
        llm_context: llm.LLMContext,
    ) -> JsonObjectType:
        """Call the tool to list reminders."""
        _LOGGER.info("Listing all reminders")

        try:
            # Get coordinator
            coordinator = None
            for entry_id, data in hass.data.get(DOMAIN, {}).items():
                if isinstance(data, dict) and "coordinator" in data:
                    coordinator = data["coordinator"]
                    break

            if not coordinator:
                return {"error": "Reminder system coordinator not found"}

            # Get all reminders from active items
            reminders = []
            for item_id, item in coordinator._active_items.items():
                if not item.get("is_alarm") and item.get("status") in ["scheduled", "active"]:
                    raw_name = item.get("name", item_id)
                    display_name = _humanize_label(raw_name) or _humanize_label(item_id) or item_id
                    reminder_info = {
                        "id": item_id,
                        "name": display_name,
                        "slug": raw_name,
                        "status": item.get("status"),
                    }

                    # Format scheduled time
                    sched_time = item.get("scheduled_time")
                    if isinstance(sched_time, datetime):
                        reminder_info["time"] = sched_time.strftime("%H:%M")
                        reminder_info["date"] = sched_time.strftime("%Y-%m-%d")
                    elif isinstance(sched_time, str):
                        parsed = dt_util.parse_datetime(sched_time)
                        if parsed:
                            reminder_info["time"] = parsed.strftime("%H:%M")
                            reminder_info["date"] = parsed.strftime("%Y-%m-%d")

                    if item.get("repeat_days"):
                        reminder_info["repeat_days"] = item["repeat_days"]

                    if item.get("message"):
                        reminder_info["message"] = item["message"]

                    reminders.append(reminder_info)

            if not reminders:
                return self.wrap_response(
                    {"reminders": [], "message": "No reminders are currently set"}
                )

            return self.wrap_response(
                {
                    "reminders": reminders,
                    "count": len(reminders),
                    "message": f"You have {len(reminders)} reminder{'s' if len(reminders) != 1 else ''} set",
                }
            )

        except Exception as e:
            _LOGGER.error("Error listing reminders: %s", e, exc_info=True)
            return {"error": f"Failed to list reminders: {str(e)}"}


class DeleteReminderTool(llm.Tool):
    """Tool for deleting reminders."""

    name = "delete_reminder"
    description = "Delete one or more reminders. Use this when the user wants to cancel, remove, or delete a reminder. Can delete by reminder name or all reminders."
    response_instruction = """
    Confirm to the user which reminder(s) were deleted.
    Keep your response concise and friendly, in plain text without formatting.
    """

    parameters = vol.Schema(
        {
            vol.Optional(
                "name",
                description="Delete reminder(s) by name or partial name match. Example: 'medicine' will delete reminders with 'medicine' in the name.",
            ): str,
            vol.Optional(
                "delete_all",
                description="Set to true to delete all reminders. Use when user says 'delete all reminders' or 'clear all reminders'.",
            ): bool,
        }
    )

    def wrap_response(self, response: dict) -> dict:
        response["instruction"] = self.response_instruction
        return response

    async def async_call(
        self,
        hass: HomeAssistant,
        tool_input: llm.ToolInput,
        llm_context: llm.LLMContext,
    ) -> JsonObjectType:
        """Call the tool to delete reminder(s)."""
        name = tool_input.tool_args.get("name")
        delete_all = tool_input.tool_args.get("delete_all", False)

        _LOGGER.info("Deleting reminder: name=%s, delete_all=%s", name, delete_all)

        try:
            # Get coordinator
            coordinator = None
            for entry_id, data in hass.data.get(DOMAIN, {}).items():
                if isinstance(data, dict) and "coordinator" in data:
                    coordinator = data["coordinator"]
                    break

            if not coordinator:
                return {"error": "Reminder system coordinator not found"}

            if delete_all:
                # Count reminders before deletion so confirmation is accurate
                reminder_count = sum(
                    1 for item in coordinator._active_items.values() if not item.get("is_alarm")
                )
                if reminder_count == 0:
                    return {
                        "error": "No reminders are currently set, so nothing was deleted",
                    }

                await coordinator.delete_all_items(is_alarm=False)
                return self.wrap_response(
                    {
                        "success": True,
                        "deleted_count": reminder_count,
                        "message": f"Deleted all {reminder_count} reminder{'s' if reminder_count != 1 else ''}",
                    }
                )

            if name:
                # Find reminders matching name (normalize like coordinator)
                deleted_count = 0
                search_terms = _normalize_terms(name)
                items_to_delete: list[str] = []

                for item_id, item in coordinator._active_items.items():
                    if item.get("is_alarm"):
                        continue
                    item_terms = _normalize_terms(item.get("name"))
                    item_terms.update(_normalize_terms(item_id))

                    if _terms_match(search_terms, item_terms):
                        items_to_delete.append(item_id)

                for item_id in items_to_delete:
                    await coordinator.delete_item(item_id, is_alarm=False)
                    deleted_count += 1

                if deleted_count > 0:
                    return self.wrap_response(
                        {
                            "success": True,
                            "deleted_count": deleted_count,
                            "message": f"Deleted {deleted_count} reminder{'s' if deleted_count != 1 else ''} matching '{name}'",
                        }
                    )
                return {"error": f"No reminders found matching '{name}'"}

            return {
                "error": "Please specify a name or set delete_all to true to delete reminders"
            }

        except Exception as e:
            _LOGGER.error("Error deleting reminder: %s", e, exc_info=True)
            return {"error": f"Failed to delete reminder: {str(e)}"}
