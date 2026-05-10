"""Helpers for resolving media players during LLM-originated requests."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .const import DASHBOARD_ENTITY_ID


def _get_hidden_media_players(hass: HomeAssistant) -> set[str]:
    """Return media players hidden by integration configuration."""
    state = hass.states.get(DASHBOARD_ENTITY_ID)
    if not state:
        return set()

    attr_value: Any = state.attributes.get("hidden_media_players")
    if isinstance(attr_value, str):
        return {attr_value}
    if isinstance(attr_value, (list, tuple, set)):
        return {str(entry) for entry in attr_value if entry}
    return set()


def _get_default_media_player(hass: HomeAssistant) -> str | None:
    """Return the configured default media player from dashboard state."""
    state = hass.states.get(DASHBOARD_ENTITY_ID)
    if not state:
        return None

    attr_value = state.attributes.get("default_media_player")
    if isinstance(attr_value, str) and attr_value.strip():
        return attr_value.strip()
    return None


def _media_player_area_id(
    entity_id: str,
    *,
    entity_registry: er.EntityRegistry,
    device_registry: dr.DeviceRegistry,
) -> str | None:
    """Resolve the effective area for a media player entity."""
    entity_entry = entity_registry.async_get(entity_id)
    if entity_entry is None:
        return None

    if entity_entry.area_id:
        return entity_entry.area_id

    if entity_entry.device_id:
        device_entry = device_registry.async_get(entity_entry.device_id)
        if device_entry is not None:
            return device_entry.area_id

    return None


def _friendly_entity_name(hass: HomeAssistant, entity_id: str) -> str:
    """Return the best available display name for an entity."""
    state = hass.states.get(entity_id)
    if state:
        friendly_name = state.attributes.get("friendly_name")
        if isinstance(friendly_name, str) and friendly_name.strip():
            return friendly_name.strip()
    return entity_id


def resolve_room_media_player_candidates(
    hass: HomeAssistant,
    *,
    conversation_device_id: str | None,
) -> dict[str, Any]:
    """Resolve visible media players in the room of the originating device."""
    result: dict[str, Any] = {
        "area_id": None,
        "area_name": None,
        "candidates": [],
        "selected": None,
    }

    if not conversation_device_id:
        return result

    device_registry = dr.async_get(hass)
    origin_device = device_registry.async_get(conversation_device_id)
    if origin_device is None or not origin_device.area_id:
        return result

    area_id = origin_device.area_id
    area_registry = ar.async_get(hass)
    area_entry = area_registry.async_get_area(area_id)
    entity_registry = er.async_get(hass)
    hidden_media_players = _get_hidden_media_players(hass)

    candidates: list[dict[str, str]] = []
    for entity_id in hass.states.async_entity_ids("media_player"):
        if entity_id in hidden_media_players:
            continue
        if (
            _media_player_area_id(
                entity_id,
                entity_registry=entity_registry,
                device_registry=device_registry,
            )
            != area_id
        ):
            continue
        candidates.append(
            {
                "entity_id": entity_id,
                "name": _friendly_entity_name(hass, entity_id),
            }
        )

    candidates.sort(key=lambda item: (item["name"].lower(), item["entity_id"]))

    default_media_player = _get_default_media_player(hass)
    selected = None
    if default_media_player:
        selected = next(
            (
                candidate
                for candidate in candidates
                if candidate["entity_id"] == default_media_player
            ),
            None,
        )
    if selected is None and candidates:
        selected = candidates[0]

    result["area_id"] = area_id
    result["area_name"] = area_entry.name if area_entry else area_id
    result["candidates"] = candidates
    result["selected"] = selected
    return result