"""Home Assistant config-entry setup for Pixie Plus Local."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryError, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .pixie_inventory import DeviceRecord, PixieInventory
from .pixie_runtime import CloudParams, PixieAuthError, PixieAuthHandler, PixieRuntimeData
from .pixie_value_profiles import hardware_list

LOGGER = logging.getLogger(__name__)

DOMAIN = "pixie_plus_local"
MANUFACTURER = "SAL - Pixie Plus"
INTEGRATION_TITLE = "Pixie Plus Local"
PLATFORMS: tuple[str, ...] = ("light", "switch", "cover")

CONF_HOME_ID = "home_id"
CONF_HOME_NAME = "home_name"
CONF_USER_ID = "user_id"
CONF_MESHNET = "meshnet"
CONF_MESHNET2 = "meshnet2"
CONF_NETID = "netid"
CONF_INVENTORY_MODE = "inventory_mode"
CONF_PIXIE_USERNAME = "pixie_username"
CONF_PIXIE_PASSWORD = "pixie_password"

INVENTORY_MODE_LOCAL_53216 = "local_53216"
INVENTORY_MODE_CLOUD_FALLBACK = "cloud_fallback"

COORDINATOR_UPDATE_INTERVAL = timedelta(seconds=10)
INVENTORY_STORE_VERSION = 1


def _inventory_store(hass: HomeAssistant, entry: ConfigEntry) -> Store:
    return Store(hass, INVENTORY_STORE_VERSION, f"{DOMAIN}_{entry.entry_id}_inventory")


async def _async_load_inventory_snapshot(hass: HomeAssistant, entry: ConfigEntry) -> PixieInventory | None:
    payload = await _inventory_store(hass, entry).async_load()
    if not isinstance(payload, dict):
        LOGGER.debug("No stored Pixie inventory snapshot found for entry %s", entry.entry_id)
        return None
    snapshot = payload.get("inventory")
    if not isinstance(snapshot, dict):
        LOGGER.debug("Stored Pixie inventory snapshot is missing inventory data for entry %s", entry.entry_id)
        return None
    try:
        inventory = PixieInventory.from_dict(snapshot)
        LOGGER.debug(
            "Restored Pixie inventory snapshot for entry %s: home=%s devices=%s",
            entry.entry_id,
            inventory.home_id,
            len(inventory.devices_by_id),
        )
        return inventory
    except Exception as err:
        LOGGER.warning("Could not restore Pixie inventory snapshot: %s", err)
        return None


async def _async_save_inventory_snapshot(hass: HomeAssistant, entry: ConfigEntry, inventory: PixieInventory | None) -> None:
    if inventory is None:
        return
    await _inventory_store(hass, entry).async_save({"inventory": inventory.to_dict()})
    LOGGER.debug(
        "Saved Pixie inventory snapshot for entry %s: home=%s devices=%s",
        entry.entry_id,
        inventory.home_id,
        len(inventory.devices_by_id),
    )


def _entry_inventory_mode(entry: ConfigEntry) -> str:
    mode = str(entry.data.get(CONF_INVENTORY_MODE) or INVENTORY_MODE_LOCAL_53216)
    resolved_mode = mode if mode in (INVENTORY_MODE_LOCAL_53216, INVENTORY_MODE_CLOUD_FALLBACK) else INVENTORY_MODE_LOCAL_53216
    if resolved_mode != mode:
        LOGGER.debug("Unknown Pixie inventory mode '%s', defaulting to %s", mode, resolved_mode)
    return resolved_mode


def _entry_username(entry: ConfigEntry) -> str:
    return str(entry.data.get(CONF_PIXIE_USERNAME) or "")


def _entry_password(entry: ConfigEntry) -> str:
    return str(entry.data.get(CONF_PIXIE_PASSWORD) or "")


async def _async_update_entry_runtime_data(
    hass: HomeAssistant,
    entry: ConfigEntry,
    cloud_params: CloudParams,
    *,
    inventory_mode: str,
    username: str,
    password: str,
) -> None:
    data = dict(entry.data)
    data.update(
        {
            CONF_HOME_ID: cloud_params.home_id,
            CONF_HOME_NAME: cloud_params.home_name,
            CONF_USER_ID: cloud_params.user_id,
            CONF_MESHNET: cloud_params.meshnet,
            CONF_MESHNET2: cloud_params.meshnet2,
            CONF_NETID: cloud_params.netid,
            CONF_INVENTORY_MODE: inventory_mode,
        }
    )
    if inventory_mode == INVENTORY_MODE_CLOUD_FALLBACK:
        data[CONF_PIXIE_USERNAME] = username
        data[CONF_PIXIE_PASSWORD] = password
    else:
        data.pop(CONF_PIXIE_USERNAME, None)
        data.pop(CONF_PIXIE_PASSWORD, None)
    hass.config_entries.async_update_entry(entry, data=data)
    LOGGER.debug(
        "Updated Pixie config entry %s for inventory mode %s%s",
        entry.entry_id,
        inventory_mode,
        " with stored credentials" if inventory_mode == INVENTORY_MODE_CLOUD_FALLBACK else " without stored credentials",
    )


@dataclass(frozen=True)
class PixieEndpoint:
    """Represents one Home Assistant entity endpoint."""

    device_id: int
    endpoint_key: str
    command_target: str
    entity_unique_id: str
    device_identifier: str
    device_name: str | None
    via_device_identifier: str | None
    entity_name: str | None = None
    entity_translation_key: str | None = None
    device_translation_key: str | None = None


def gateway_device_identifier(inventory: PixieInventory) -> str:
    """Return the stable gateway device identifier."""
    gateway = inventory.gateway
    if gateway is not None:
        if gateway.gateway_id:
            return f"gateway:{gateway.gateway_id}"
        if gateway.gateway_mac:
            return f"gateway:{gateway.gateway_mac}"
    return f"gateway:home:{inventory.home_id}"


def physical_device_identifier(record: DeviceRecord) -> str:
    """Return the stable identifier for one physical device."""
    if record.mac:
        return f"device:{record.mac}"
    return f"device:id:{record.id}"


def child_device_identifier(record: DeviceRecord, endpoint_key: str) -> str:
    """Return the stable identifier for one child endpoint device."""
    return f"{physical_device_identifier(record)}:{endpoint_key}"


def endpoint_unique_identifier(record: DeviceRecord, endpoint_key: str) -> str:
    """Return the stable unique identifier for one entity endpoint."""
    if endpoint_key == "main":
        return physical_device_identifier(record)
    return child_device_identifier(record, endpoint_key)


async def async_register_device_topology(
    hass: HomeAssistant,
    entry: ConfigEntry,
    inventory: PixieInventory | None,
    *,
    domain: str,
) -> None:
    """Register the gateway and physical devices in the device registry."""
    if inventory is None:
        return

    device_registry = dr.async_get(hass)
    gateway_identifier = gateway_device_identifier(inventory)
    gateway = inventory.gateway
    gateway_kwargs = {
        "config_entry_id": entry.entry_id,
        "identifiers": {(domain, gateway_identifier)},
        "manufacturer": MANUFACTURER,
        "name": gateway.model_name or "Pixie Gateway" if gateway else "Pixie Gateway",
        "model": gateway.model_name if gateway else "Pixie Gateway",
        "model_id": gateway.model_no if gateway else None,
    }
    device_registry.async_get_or_create(**gateway_kwargs)

    for record in inventory.devices_by_id.values():
        if record.model_no == "0102":
            continue

        kwargs = {
            "config_entry_id": entry.entry_id,
            "identifiers": {(domain, physical_device_identifier(record))},
            "manufacturer": MANUFACTURER,
            "name": record.name,
            "model": hardware_list.get(record.model_no, record.model_no),
            "model_id": record.model_no,
            "via_device": (domain, gateway_identifier),
        }
        if record.version is not None:
            kwargs["sw_version"] = str(record.version)
        device_registry.async_get_or_create(**kwargs)


class PixiePlusCoordinatorEntity(CoordinatorEntity[PixieInventory]):
    """Shared base entity for Pixie Plus Local platforms."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, runtime_data, endpoint: PixieEndpoint, *, domain: str) -> None:
        """Initialize the shared base entity."""
        super().__init__(runtime_data.coordinator)
        self.runtime_data = runtime_data
        self.endpoint = endpoint
        self.domain = domain
        self._attr_unique_id = endpoint.entity_unique_id
        self._attr_name = endpoint.entity_name
        self._attr_translation_key = endpoint.entity_translation_key

    @property
    def record(self) -> DeviceRecord:
        """Return the live device record from the shared inventory."""
        return self.coordinator.data.devices_by_id[self.endpoint.device_id]

    @property
    def available(self) -> bool:
        """Return whether the entity is currently available."""
        runtime_session = self.runtime_data.pixie_runtime.runtime_session
        if runtime_session is None or not runtime_session.is_alive():
            return False
        return self.record.runtime.presence == "online"

    @property
    def device_info(self):
        """Return the device registry info for this entity's device."""
        record = self.record
        info = {
            "identifiers": {(self.domain, self.endpoint.device_identifier)},
            "manufacturer": MANUFACTURER,
            "model": hardware_list.get(record.model_no, record.model_no),
            "model_id": record.model_no,
        }
        if self.endpoint.device_name is not None:
            info["name"] = self.endpoint.device_name
        if self.endpoint.device_translation_key is not None:
            info["translation_key"] = self.endpoint.device_translation_key
        if self.endpoint.via_device_identifier is not None:
            info["via_device"] = (self.domain, self.endpoint.via_device_identifier)
        if record.version is not None:
            info["sw_version"] = str(record.version)
        return info


class PixiePlusRuntimeCoordinator(DataUpdateCoordinator[PixieInventory]):
    """Expose the in-memory Pixie runtime inventory to HA entities."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        pixie_runtime: PixieRuntimeData,
    ) -> None:
        super().__init__(
            hass,
            LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=COORDINATOR_UPDATE_INTERVAL,
            always_update=True,
        )
        self.pixie_runtime = pixie_runtime
        self.runtime_manager: PixiePlusConfigEntryRuntimeData | None = None

    async def _async_update_data(self) -> PixieInventory:
        """Return the current runtime inventory snapshot."""
        if self.runtime_manager is not None:
            try:
                await self.runtime_manager.async_ensure_runtime(self.hass, reason="coordinator_refresh")
            except Exception as err:
                raise UpdateFailed(f"Pixie runtime unavailable: {err}") from err

        inventory = self.pixie_runtime.inventory
        if inventory is None:
            raise UpdateFailed("Pixie runtime inventory is not initialized")

        runtime_session = self.pixie_runtime.runtime_session
        if runtime_session is not None and not runtime_session.is_alive() and runtime_session.error is not None:
            raise UpdateFailed(f"Pixie gateway runtime stopped: {runtime_session.error}") from runtime_session.error

        return inventory


@dataclass
class PixiePlusConfigEntryRuntimeData:
    """Objects stored in ConfigEntry.runtime_data."""

    handler: PixieAuthHandler
    cloud_params: CloudParams
    pixie_runtime: PixieRuntimeData
    coordinator: PixiePlusRuntimeCoordinator
    entry: ConfigEntry
    restart_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @staticmethod
    def _describe_runtime_session(runtime_session) -> str:
        """Return a compact runtime-session status string for logs."""
        if runtime_session is None:
            return "missing"

        summary = runtime_session.health_summary()
        parts = [
            f"alive={summary['alive']}",
            f"primed={summary['primed']}",
            f"closed={summary['connection_closed']}",
            f"hb_failures={summary['consecutive_heartbeat_failures']}",
        ]
        if summary["error"]:
            parts.append(f"error={summary['error']}")
        return ", ".join(parts)

    def push_inventory_update_from_thread(self, inventory: PixieInventory) -> None:
        """Push a runtime inventory update to HA from the TCP worker thread."""
        self.pixie_runtime.inventory = inventory
        self.coordinator.hass.loop.call_soon_threadsafe(
            self.coordinator.async_set_updated_data,
            inventory,
        )
        self.coordinator.hass.loop.call_soon_threadsafe(
            self.coordinator.hass.async_create_task,
            _async_save_inventory_snapshot(self.coordinator.hass, self.entry, inventory),
        )

    async def async_ensure_runtime(self, hass: HomeAssistant, *, reason: str):
        """Ensure there is one healthy live runtime session for this config entry."""
        runtime_session = self.pixie_runtime.runtime_session
        if runtime_session is not None and runtime_session.is_alive() and not runtime_session.needs_restart():
            return runtime_session

        async with self.restart_lock:
            runtime_session = self.pixie_runtime.runtime_session
            if runtime_session is not None and runtime_session.is_alive() and not runtime_session.needs_restart():
                return runtime_session

            if runtime_session is not None:
                LOGGER.warning(
                    "Restarting Pixie runtime (%s): %s",
                    reason,
                    self._describe_runtime_session(runtime_session),
                )
                await hass.async_add_executor_job(runtime_session.stop_and_join, 5.0)
            else:
                LOGGER.info("Starting Pixie runtime (%s)", reason)

            restart_handler = PixieAuthHandler()
            restart_handler.inventory = self.pixie_runtime.inventory
            restart_handler.gateway_identity = self.pixie_runtime.inventory.gateway if self.pixie_runtime.inventory else None
            restart_handler.set_inventory_update_callback(self.push_inventory_update_from_thread)

            username = _entry_username(self.entry)
            password = _entry_password(self.entry)
            inventory_mode = _entry_inventory_mode(self.entry)

            try:
                restarted_runtime = await restart_handler.async_bootstrap_gateway(
                    self.cloud_params,
                    username=username,
                    password=password,
                    keep_control_alive=True,
                    wait_for_shutdown=False,
                    hydrate_inventory=False,
                )
            except Exception:
                restart_session = restart_handler.runtime_session
                if restart_session is not None:
                    await hass.async_add_executor_job(restart_session.stop_and_join, 5.0)
                raise

            if restarted_runtime.runtime_session is None:
                raise ConfigEntryError("Pixie runtime restart completed without a live session")

            self.handler = restart_handler
            self.pixie_runtime.handler = restart_handler
            self.pixie_runtime.runtime_session = restarted_runtime.runtime_session
            self.pixie_runtime.inventory_mode = inventory_mode
            if restarted_runtime.inventory is not None:
                self.pixie_runtime.inventory = restarted_runtime.inventory

            LOGGER.info(
                "Pixie runtime ready after %s: %s",
                reason,
                self._describe_runtime_session(self.pixie_runtime.runtime_session),
            )
            return self.pixie_runtime.runtime_session

    async def async_shutdown(self, hass: HomeAssistant) -> None:
        """Stop the long-lived gateway runtime session."""
        async with self.restart_lock:
            runtime_session = self.pixie_runtime.runtime_session
            if runtime_session is None:
                return

            await hass.async_add_executor_job(runtime_session.stop_and_join, 5.0)

    async def async_send_local_command(self, hass: HomeAssistant, **kwargs) -> None:
        """Send a local command using the single shared 41578 runtime session."""
        runtime_session = await self.async_ensure_runtime(hass, reason="command_send")
        try:
            await hass.async_add_executor_job(runtime_session.send_command, dict(kwargs))
            self.coordinator.async_set_updated_data(self.pixie_runtime.inventory)
            return
        except Exception as err:
            runtime_unhealthy = (
                not runtime_session.is_alive()
                or runtime_session.needs_restart()
                or runtime_session.connection_closed_at is not None
            )
            if runtime_unhealthy:
                LOGGER.warning("Live Pixie runtime command failed; restarting shared runtime: %s", err)
                recovered_session = await self.async_ensure_runtime(
                    hass,
                    reason="command_send_recovery",
                )
                await hass.async_add_executor_job(recovered_session.send_command, dict(kwargs))
                self.coordinator.async_set_updated_data(self.pixie_runtime.inventory)
                return

            LOGGER.warning("Live Pixie runtime command failed on shared runtime: %s", err)
            raise


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the Pixie Plus Local integration."""
    return True


def _cloud_params_from_entry(entry: ConfigEntry) -> CloudParams:
    """Build bootstrap cloud parameters from persisted config-entry data."""
    missing = [
        key
        for key in (CONF_HOME_ID, CONF_USER_ID, CONF_MESHNET, CONF_MESHNET2, CONF_NETID)
        if not entry.data.get(key)
    ]
    if missing:
        raise ConfigEntryError(
            "Config entry is missing required Pixie runtime fields: " + ", ".join(sorted(missing))
        )

    return CloudParams(
        home_id=str(entry.data[CONF_HOME_ID]),
        home_name=str(entry.data.get(CONF_HOME_NAME) or entry.title or INTEGRATION_TITLE),
        user_id=str(entry.data[CONF_USER_ID]),
        meshnet=str(entry.data[CONF_MESHNET]),
        meshnet2=str(entry.data[CONF_MESHNET2]),
        netid=str(entry.data[CONF_NETID]),
    )


async def _async_build_runtime_data(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> PixiePlusConfigEntryRuntimeData:
    """Bootstrap the Pixie local runtime and its HA coordinator."""
    cloud_params = _cloud_params_from_entry(entry)
    inventory_mode = _entry_inventory_mode(entry)
    persisted_inventory = await _async_load_inventory_snapshot(hass, entry)
    username = _entry_username(entry)
    password = _entry_password(entry)

    LOGGER.debug(
        "Bootstrapping Pixie entry %s in %s mode%s",
        entry.entry_id,
        inventory_mode,
        " with stored inventory snapshot available" if persisted_inventory is not None else " with no stored inventory snapshot",
    )

    handler = PixieAuthHandler()
    coordinator: PixiePlusRuntimeCoordinator | None = None

    async def _shutdown_runtime() -> None:
        runtime_session = handler.runtime_session
        if runtime_session is not None:
            await hass.async_add_executor_job(runtime_session.stop_and_join, 5.0)

    try:
        if inventory_mode == INVENTORY_MODE_CLOUD_FALLBACK:
            LOGGER.warning(
                "Pixie Plus Local is using cloud-assisted inventory mode because direct local inventory was unavailable during setup"
            )
            if not username or not password:
                raise ConfigEntryError("Cloud fallback mode requires stored Pixie credentials")
            try:
                LOGGER.debug("Refreshing Pixie inventory from cloud for entry %s", entry.entry_id)
                refreshed_cloud_params = await handler.async_fetch_cloud_params(
                    username,
                    password,
                    include_inventory_seed=True,
                )
                cloud_params = refreshed_cloud_params
                await _async_update_entry_runtime_data(
                    hass,
                    entry,
                    cloud_params,
                    inventory_mode=INVENTORY_MODE_CLOUD_FALLBACK,
                    username=username,
                    password=password,
                )
            except Exception as err:
                if persisted_inventory is None:
                    raise ConfigEntryNotReady(f"Pixie cloud refresh failed and no stored inventory is available: {err}") from err
                LOGGER.warning("Pixie cloud refresh failed; using stored inventory snapshot: %s", err)
                handler.inventory = persisted_inventory
                handler.gateway_identity = persisted_inventory.gateway

            pixie_runtime = await handler.async_bootstrap_gateway(
                cloud_params,
                username=username,
                password=password,
                keep_control_alive=True,
                wait_for_shutdown=False,
                hydrate_inventory=False,
            )
            pixie_runtime.inventory_mode = INVENTORY_MODE_CLOUD_FALLBACK
            if pixie_runtime.inventory is None:
                pixie_runtime.inventory = handler.inventory
            LOGGER.debug("Pixie entry %s using cloud-assisted inventory mode", entry.entry_id)
        else:
            LOGGER.debug("Trying direct local Pixie inventory startup for entry %s", entry.entry_id)
            pixie_runtime = await handler.async_bootstrap_gateway(
                cloud_params,
                username="",
                password="",
                keep_control_alive=True,
                wait_for_shutdown=False,
            )
            pixie_runtime.inventory_mode = INVENTORY_MODE_LOCAL_53216
            if pixie_runtime.inventory is None:
                if persisted_inventory is None:
                    raise ConfigEntryNotReady("Pixie startup inventory unavailable and no stored inventory snapshot exists")
                LOGGER.warning("Direct local Pixie inventory startup failed; using stored inventory snapshot")
                await _shutdown_runtime()
                handler = PixieAuthHandler()
                handler.inventory = persisted_inventory
                handler.gateway_identity = persisted_inventory.gateway
                pixie_runtime = await handler.async_bootstrap_gateway(
                    cloud_params,
                    username="",
                    password="",
                    keep_control_alive=True,
                    wait_for_shutdown=False,
                    hydrate_inventory=False,
                )
                pixie_runtime.inventory = persisted_inventory
                pixie_runtime.inventory_mode = INVENTORY_MODE_LOCAL_53216
            else:
                LOGGER.debug("Direct local Pixie inventory startup succeeded for entry %s", entry.entry_id)

        coordinator = PixiePlusRuntimeCoordinator(hass, entry, pixie_runtime)
        handler.set_inventory_update_callback(
            lambda inventory: hass.loop.call_soon_threadsafe(coordinator.async_set_updated_data, inventory)
        )
        await coordinator.async_config_entry_first_refresh()
        await _async_save_inventory_snapshot(hass, entry, pixie_runtime.inventory)
    except PixieAuthError as err:
        await _shutdown_runtime()
        raise ConfigEntryNotReady(str(err)) from err
    except Exception:
        await _shutdown_runtime()
        raise

    runtime_data = PixiePlusConfigEntryRuntimeData(
        handler=handler,
        cloud_params=cloud_params,
        pixie_runtime=pixie_runtime,
        coordinator=coordinator,
        entry=entry,
    )
    coordinator.runtime_manager = runtime_data
    return runtime_data


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Pixie Plus Local from a config entry."""
    runtime_data = await _async_build_runtime_data(hass, entry)
    desired_title = (
        runtime_data.pixie_runtime.inventory.home_name
        if runtime_data.pixie_runtime.inventory and runtime_data.pixie_runtime.inventory.home_name
        else runtime_data.cloud_params.home_name
    ) or INTEGRATION_TITLE
    if entry.title != desired_title:
        hass.config_entries.async_update_entry(entry, title=desired_title)
    entry.runtime_data = runtime_data
    await async_register_device_topology(hass, entry, runtime_data.pixie_runtime.inventory, domain=DOMAIN)

    if PLATFORMS:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Pixie Plus Local config entry."""
    unload_ok = True
    if PLATFORMS:
        unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if not unload_ok:
        return False

    runtime_data: PixiePlusConfigEntryRuntimeData = entry.runtime_data
    await runtime_data.async_shutdown(hass)
    return True