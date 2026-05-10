"""LLM function implementations for HA Alarm Clock services."""

import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import llm

from .alarm_tools import DeleteAlarmTool, ListAlarmsTool, SetAlarmTool
from .reminder_tools import DeleteReminderTool, ListRemindersTool, SetReminderTool
from .alarm_control_tools import SnoozeAlarmTool, StopAlarmTool, SnoozeReminderTool, StopReminderTool
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

ALARM_REMINDER_API_NAME = "HA Alarm Clock Assistant"


def get_coordinator(hass: HomeAssistant):
    """Get the coordinator from hass.data - shared helper function."""
    for entry_id, data in hass.data.get(DOMAIN, {}).items():
        if isinstance(data, dict) and "coordinator" in data:
            return data["coordinator"]
    return None

ALARM_REMINDER_SERVICES_PROMPT = """
You have access to alarm and reminder management tools to help users manage their alarms and reminders.

For Alarms:
- When a user asks to set an alarm, use the set_alarm tool
- When a user asks what alarms are set or scheduled, use the list_alarms tool
- When a user asks to delete or cancel an alarm, use the delete_alarm tool
- When a user asks to stop or dismiss a ringing alarm, use the stop_alarm tool
- When a user asks to snooze a ringing alarm, use the snooze_alarm tool

For Reminders:
- When a user asks to set a reminder, use the set_reminder tool
- When a user asks what reminders are set or scheduled, use the list_reminders tool
- When a user asks to delete or cancel a reminder, use the delete_reminder tool
- When a user asks to stop or dismiss a ringing reminder, use the stop_reminder tool
- When a user asks to snooze a ringing reminder, use the snooze_reminder tool

If a tool response says clarification is needed, ask the user a brief follow-up question and wait for the answer instead of guessing.

Be helpful and conversational when confirming actions or listing items.
""".strip()


class AlarmReminderAPI(llm.API):
    """Alarm and Reminder management API for LLM integration."""

    def __init__(self, hass: HomeAssistant, name: str) -> None:
        """Initialize the API."""
        super().__init__(hass=hass, id=DOMAIN, name=name)

    async def async_get_api_instance(
        self, llm_context: llm.LLMContext
    ) -> llm.APIInstance:
        """Get API instance."""
        tools = [
            SetAlarmTool(),
            ListAlarmsTool(),
            DeleteAlarmTool(),
            StopAlarmTool(),
            SnoozeAlarmTool(),
            SetReminderTool(),
            ListRemindersTool(),
            DeleteReminderTool(),
            StopReminderTool(),
            SnoozeReminderTool(),
        ]

        return llm.APIInstance(
            api=self,
            api_prompt=ALARM_REMINDER_SERVICES_PROMPT,
            llm_context=llm_context,
            tools=tools,
        )


async def async_setup_llm_api(hass: HomeAssistant) -> None:
    """Set up LLM API for alarm and reminder services."""
    # Check if already set up
    if DOMAIN in hass.data and "llm_api" in hass.data[DOMAIN]:
        _LOGGER.debug("LLM API already registered")
        return

    hass.data.setdefault(DOMAIN, {})

    # Create and register the API
    alarm_reminder_api = AlarmReminderAPI(hass, ALARM_REMINDER_API_NAME)
    hass.data[DOMAIN]["llm_api"] = alarm_reminder_api

    try:
        unregister_func = llm.async_register_api(hass, alarm_reminder_api)
        hass.data[DOMAIN]["llm_api_unregister"] = unregister_func
        _LOGGER.info("HA Alarm Clock LLM API registered successfully")
    except Exception as e:
        _LOGGER.error("Failed to register LLM API: %s", e, exc_info=True)
        raise


async def async_cleanup_llm_api(hass: HomeAssistant) -> None:
    """Clean up LLM API."""
    if DOMAIN not in hass.data:
        return

    # Unregister API if we have the unregister function
    unreg_func = hass.data[DOMAIN].get("llm_api_unregister")
    if unreg_func:
        try:
            unreg_func()
            _LOGGER.info("HA Alarm Clock LLM API unregistered")
        except Exception as e:
            _LOGGER.debug("Error unregistering LLM API: %s", e)

    # Clean up stored data
    hass.data[DOMAIN].pop("llm_api", None)
    hass.data[DOMAIN].pop("llm_api_unregister", None)
