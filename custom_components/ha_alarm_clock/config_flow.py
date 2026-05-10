"""Config flow for the HA Alarm Clock integration."""
from __future__ import annotations

from typing import Any
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.helpers import config_validation as cv, selector
from homeassistant.data_entry_flow import FlowResult

from .const import (
    DOMAIN,
    CONF_ALARM_SOUND,
    CONF_REMINDER_SOUND,
    CONF_MEDIA_PLAYER,
    CONF_HIDDEN_MEDIA_PLAYERS,
    CONF_ALLOWED_ACTIVATION_ENTITIES,
    CONF_ENABLE_LLM,
    CONF_DEFAULT_SNOOZE_MINUTES,
    CONF_ACTIVE_PRESS_MODE,
    ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE,
    ACTIVE_PRESS_MODE_SHORT_SNOOZE_LONG_STOP,
    DEFAULT_ALARM_SOUND,
    DEFAULT_REMINDER_SOUND,
    DEFAULT_MEDIA_PLAYER,
    DEFAULT_HIDDEN_MEDIA_PLAYERS,
    DEFAULT_NAME,
    DEFAULT_ENABLE_LLM,
    DEFAULT_ALLOWED_ACTIVATION_ENTITIES,
    DEFAULT_SNOOZE_MINUTES,
    DEFAULT_ACTIVE_PRESS_MODE,
)

@config_entries.HANDLERS.register(DOMAIN)
class HAAlarmClockConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for HA Alarm Clock."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        if user_input is not None:
            return self.async_create_entry(title=DEFAULT_NAME, data={})

        return self.async_show_form(step_id="user")

    @staticmethod
    def async_get_options_flow(_config_entry):
        """Get the options flow."""
        return OptionsFlowHandler()

class OptionsFlowHandler(config_entries.OptionsFlow):
    def _media_player_entity_ids(self) -> list[str]:
        """Return currently available media players."""
        return sorted(self.hass.states.async_entity_ids("media_player"))

    def _normalize_hidden_media_players(
        self,
        value: Any,
        *,
        available_media_players: set[str],
        default_media_player: str | None,
    ) -> list[str]:
        """Normalize hidden media player options."""
        if value in (None, ""):
            return []

        hidden: set[str] = set()
        for entity in cv.ensure_list(value):
            if not entity:
                continue
            normalized = cv.entity_id(str(entity))
            if default_media_player and normalized == default_media_player:
                raise vol.Invalid("default_media_player_hidden")
            if normalized in available_media_players:
                hidden.add(normalized)
        return sorted(hidden)

    async def async_step_init(self, user_input=None):
        """Manage the options."""
        # Get list of media players plus "none" option
        media_players = ["none"]
        media_player_entities = self._media_player_entity_ids()
        media_player_entity_set = set(media_player_entities)
        media_players.extend(media_player_entities)
        media_player_choices = list(media_players)
        if "" not in media_player_choices:
            media_player_choices.append("")
        media_player_choices_with_none = media_player_choices + [None]
        configured_default_player = self.config_entry.options.get(
            CONF_MEDIA_PLAYER,
            DEFAULT_MEDIA_PLAYER,
        )
        excluded_hidden_candidates = [configured_default_player] if configured_default_player else []

        stored_allowed = self.config_entry.options.get(
            CONF_ALLOWED_ACTIVATION_ENTITIES,
            DEFAULT_ALLOWED_ACTIVATION_ENTITIES,
        )
        if stored_allowed in (None, ""):
            default_allowed_entities: list[str] = []
        elif isinstance(stored_allowed, (list, tuple, set)):
            default_allowed_entities = [str(entity) for entity in stored_allowed if entity]
        else:
            default_allowed_entities = [str(stored_allowed)]

        try:
            default_hidden_media_players = self._normalize_hidden_media_players(
                self.config_entry.options.get(
                    CONF_HIDDEN_MEDIA_PLAYERS,
                    DEFAULT_HIDDEN_MEDIA_PLAYERS,
                ),
                available_media_players=media_player_entity_set,
                default_media_player=configured_default_player,
            )
        except vol.Invalid:
            default_hidden_media_players = []

        data_schema = vol.Schema({
            vol.Optional(
                CONF_ALARM_SOUND,
                default=self.config_entry.options.get(
                    CONF_ALARM_SOUND, DEFAULT_ALARM_SOUND
                ),
            ): str,
            vol.Optional(
                CONF_REMINDER_SOUND,
                default=self.config_entry.options.get(
                    CONF_REMINDER_SOUND, DEFAULT_REMINDER_SOUND
                ),
            ): str,
            vol.Optional(
                CONF_MEDIA_PLAYER,
                default=self.config_entry.options.get(CONF_MEDIA_PLAYER, "none")
            ): vol.In(media_player_choices_with_none),
            vol.Optional(
                CONF_HIDDEN_MEDIA_PLAYERS,
                default=default_hidden_media_players,
            ): selector.selector(
                {
                    "entity": {
                        "multiple": True,
                        "filter": [{"domain": "media_player"}],
                        "exclude_entities": excluded_hidden_candidates,
                    }
                }
            ),
            vol.Optional(
                CONF_ALLOWED_ACTIVATION_ENTITIES,
                default=default_allowed_entities,
            ): selector.EntitySelector(
                selector.EntitySelectorConfig(multiple=True)
            ),
            vol.Optional(
                CONF_ENABLE_LLM,
                default=self.config_entry.options.get(CONF_ENABLE_LLM, DEFAULT_ENABLE_LLM),
            ): bool,
            vol.Optional(
                CONF_DEFAULT_SNOOZE_MINUTES,
                default=self.config_entry.options.get(
                    CONF_DEFAULT_SNOOZE_MINUTES, DEFAULT_SNOOZE_MINUTES
                ),
            ): vol.All(vol.Coerce(int), vol.Range(min=1, max=180)),
            vol.Optional(
                CONF_ACTIVE_PRESS_MODE,
                default=self.config_entry.options.get(
                    CONF_ACTIVE_PRESS_MODE, DEFAULT_ACTIVE_PRESS_MODE
                ),
            ): vol.In(
                [
                    ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE,
                    ACTIVE_PRESS_MODE_SHORT_SNOOZE_LONG_STOP,
                ]
            ),
        })

        errors: dict[str, str] = {}

        if user_input is not None:
            data = dict(user_input)
            selected_player = data.get(CONF_MEDIA_PLAYER)
            if not selected_player or selected_player == "none":
                data[CONF_MEDIA_PLAYER] = None

            try:
                data[CONF_HIDDEN_MEDIA_PLAYERS] = self._normalize_hidden_media_players(
                    data.get(CONF_HIDDEN_MEDIA_PLAYERS),
                    available_media_players=media_player_entity_set,
                    default_media_player=data.get(CONF_MEDIA_PLAYER),
                )
            except vol.Invalid:
                errors[CONF_HIDDEN_MEDIA_PLAYERS] = "default_media_player_hidden"

            allowed_entities_input = data.get(CONF_ALLOWED_ACTIVATION_ENTITIES)
            if allowed_entities_input is None:
                data[CONF_ALLOWED_ACTIVATION_ENTITIES] = []
            else:
                try:
                    normalized_entities: list[str] = []
                    for entity in cv.ensure_list(allowed_entities_input):
                        if not entity:
                            continue
                        normalized_entities.append(cv.entity_id(entity))
                except vol.Invalid:
                    errors["base"] = "invalid_activation_entities"
                else:
                    data[CONF_ALLOWED_ACTIVATION_ENTITIES] = normalized_entities

            if not errors:
                return self.async_create_entry(title="", data=data)

        return self.async_show_form(
            step_id="init",
            data_schema=data_schema,
            errors=errors,
        )
