#!/usr/bin/env python3
"""
Pixie Plus Autonomous Authentication Handler (Auto-Discover + Key Extraction)

Fully autonomous flow:
1. Broadcast UDP discovery to find available hubs on LAN
2. Capture handshake via MITM proxy to extract session key
3. Store credentials + key for subsequent commands without manual hub IP

Based on Android app analysis:
- q0.b:UDP broadcast discovers gateways on port 41580
- After discovery, TCP connects to port 41578 for control
"""

import json
import logging
import os
import socket
import threading
import time
import base64
import queue
import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Tuple, Callable

# Network constants
UDP_DISCOVERY_PORT = 41580
TCP_CONTROL_PORT = 41578
BROADCAST_ADDRESS = "255.255.255.255"
RUNTIME_IDLE_TIMEOUT_SECONDS = 45.0
RUNTIME_MAX_CONSECUTIVE_HEARTBEAT_FAILURES = 3
RUNTIME_COMMAND_BASE_TIMEOUT_SECONDS = 10.0
RUNTIME_COMMAND_PER_AHEAD_SECONDS = 2.0
RUNTIME_COMMAND_MAX_TIMEOUT_SECONDS = 60.0
RUNTIME_COMMAND_MIN_GAP_SECONDS = 0.25
from datetime import datetime, timezone
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from .pixie_protocol import PixieEnvelope, PixieMessage, PixieCrypto
from .pixie_inventory import GatewayIdentity, PixieInventory
from .pixie_value_profiles import (
    decode_value_byte,
    get_all_effect_names,
    get_model_effect_names,
    resolve_cover_command_position,
)

LOGGER = logging.getLogger(__name__)


# ============================================================================
# ENVELOPE ENCODING/DECODING (moved to pixie_protocol.py)
# ============================================================================

# Removed: encode_envelope, decode_envelope - now in pixie_protocol.PixieEnvelope


class PixieHub:
    """Represents a discovered Pixie hub."""

    def __init__(self, host: str, port: int = 41580):
        self.host = host
        self.port = port
        self.is_valid = False

    def __repr__(self):
        return f"Hub({self.host}:{self.port})"


class PixieAuthError(Exception):
    """Base exception for Pixie authentication errors"""
    pass


@dataclass(frozen=True)
class CloudParams:
    """Persistable cloud-derived parameters required for local gateway access."""

    home_id: str
    home_name: str
    user_id: str
    meshnet: str
    meshnet2: str
    netid: str


@dataclass
class PixieRuntimeSession:
    """Owns the long-lived 41578 control thread and its readiness state."""

    handler: "PixieAuthHandler"
    host: str
    port: int
    keep_control_alive: bool
    stop_event: threading.Event = field(default_factory=threading.Event)
    ready_event: threading.Event = field(default_factory=threading.Event)
    ready_state: Dict[str, Any] = field(default_factory=dict)
    control_result: Dict[str, Any] = field(default_factory=lambda: {"result": None, "error": None})
    command_kwargs: Dict[str, Any] = field(default_factory=dict)
    command_queue: "queue.Queue[Dict[str, Any]]" = field(default_factory=queue.Queue)
    last_inbound_at: Optional[float] = None
    last_heartbeat_sent_at: Optional[float] = None
    last_heartbeat_reply_at: Optional[float] = None
    primed_at: Optional[float] = None
    connection_closed_at: Optional[float] = None
    consecutive_heartbeat_failures: int = 0
    thread: Optional[threading.Thread] = None
    command_state_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    command_sequence: int = 0
    active_command_id: Optional[int] = None
    active_command_started_at: Optional[float] = None
    last_command_sent_at: Optional[float] = None

    def _update_health_state(self, **kwargs: Any) -> None:
        self.ready_state.update(kwargs)

    def start(self) -> None:
        if self.thread is not None:
            return

        self._update_health_state(started_at=time.time(), stop_requested=False)

        self.thread = threading.Thread(
            target=self._run,
            name="pixie-41578-control-main",
            daemon=False,
        )
        self.thread.start()

    def _run(self) -> None:
        try:
            self.control_result["result"] = self.handler._perform_handshake_capture(
                self.host,
                self.port,
                runtime_session=self,
                control_ready_event=self.ready_event,
                control_ready_state=self.ready_state,
                stop_event=self.stop_event,
                keep_control_alive=self.keep_control_alive,
                command_request_queue=self.command_queue,
                **self.command_kwargs,
            )
        except Exception as exc:
            self.control_result["error"] = exc
            self._update_health_state(last_error=str(exc))

    def wait_until_primed(self, timeout: float) -> bool:
        return self.ready_event.wait(timeout=timeout)

    def is_alive(self) -> bool:
        return bool(self.thread and self.thread.is_alive())

    def join(self, timeout: Optional[float] = None) -> None:
        if self.thread is not None:
            self.thread.join(timeout=timeout)

    def stop(self) -> None:
        self.stop_event.set()
        self._update_health_state(stop_requested=True)

    def stop_and_join(self, timeout: float = 5.0) -> None:
        self.stop()
        self.join(timeout=timeout)

    def mark_inbound_traffic(self, when: Optional[float] = None) -> None:
        ts = time.time() if when is None else float(when)
        self.last_inbound_at = ts
        self._update_health_state(last_inbound_at=ts)

    def mark_primed(self, when: Optional[float] = None) -> None:
        if self.primed_at is not None:
            return
        ts = time.time() if when is None else float(when)
        self.primed_at = ts
        self._update_health_state(primed_at=ts)

    def mark_heartbeat_sent(self, when: Optional[float] = None) -> None:
        ts = time.time() if when is None else float(when)
        self.last_heartbeat_sent_at = ts
        self._update_health_state(last_heartbeat_sent_at=ts)

    def mark_heartbeat_reply(self, when: Optional[float] = None) -> None:
        ts = time.time() if when is None else float(when)
        self.last_heartbeat_reply_at = ts
        self.consecutive_heartbeat_failures = 0
        self._update_health_state(
            last_heartbeat_reply_at=ts,
            consecutive_heartbeat_failures=0,
        )

    def mark_heartbeat_failure(self) -> None:
        self.consecutive_heartbeat_failures += 1
        self._update_health_state(
            consecutive_heartbeat_failures=self.consecutive_heartbeat_failures,
        )

    def mark_connection_closed(self, when: Optional[float] = None) -> None:
        if self.connection_closed_at is not None:
            return
        ts = time.time() if when is None else float(when)
        self.connection_closed_at = ts
        self._update_health_state(connection_closed=True, connection_closed_at=ts)

    def health_summary(self) -> Dict[str, Any]:
        return {
            "alive": self.is_alive(),
            "primed": self.primed_at is not None,
            "connection_closed": self.connection_closed_at is not None,
            "consecutive_heartbeat_failures": self.consecutive_heartbeat_failures,
            "last_inbound_at": self.last_inbound_at,
            "last_heartbeat_sent_at": self.last_heartbeat_sent_at,
            "last_heartbeat_reply_at": self.last_heartbeat_reply_at,
            "error": str(self.error) if self.error is not None else None,
        }

    def needs_restart(
        self,
        *,
        now: Optional[float] = None,
        idle_timeout: float = RUNTIME_IDLE_TIMEOUT_SECONDS,
        max_heartbeat_failures: int = RUNTIME_MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
    ) -> bool:
        if self.stop_event.is_set():
            return False

        if self.error is not None or self.connection_closed_at is not None:
            return True

        if self.thread is not None and not self.thread.is_alive():
            return True

        if self.primed_at is None:
            return False

        if self.consecutive_heartbeat_failures < max_heartbeat_failures:
            return False

        ts_now = time.time() if now is None else float(now)
        last_activity = self.last_inbound_at or self.last_heartbeat_reply_at or self.primed_at
        if last_activity is None:
            return False

        return (ts_now - last_activity) >= idle_timeout

    def reserve_command_slot(self) -> tuple[int, int]:
        """Reserve a command slot and report how many commands are already ahead."""
        with self.command_state_lock:
            self.command_sequence += 1
            queued = self.command_queue.qsize()
            in_flight = 1 if self.active_command_id is not None else 0
            return self.command_sequence, queued + in_flight

    def mark_command_started(self, command_id: int) -> None:
        """Mark a queued command as actively executing on the live session."""
        with self.command_state_lock:
            self.active_command_id = command_id
            self.active_command_started_at = time.time()

    def mark_command_finished(self, command_id: int) -> None:
        """Clear the active command marker once execution completes."""
        with self.command_state_lock:
            if self.active_command_id == command_id:
                self.active_command_id = None
                self.active_command_started_at = None

    def command_backlog_snapshot(self) -> Dict[str, Any]:
        """Return current queue depth and active-command state for logging."""
        with self.command_state_lock:
            return {
                "queued": self.command_queue.qsize(),
                "active_command_id": self.active_command_id,
                "active_for": (
                    None
                    if self.active_command_started_at is None
                    else max(0.0, time.time() - self.active_command_started_at)
                ),
            }

    def throttle_before_command_send(self, min_gap: float = RUNTIME_COMMAND_MIN_GAP_SECONDS) -> None:
        """Enforce a minimum gap between queued command sends."""
        with self.command_state_lock:
            last_sent_at = self.last_command_sent_at

        if last_sent_at is None:
            return

        remaining = min_gap - (time.time() - last_sent_at)
        if remaining > 0:
            time.sleep(remaining)

    def mark_command_sent(self, when: Optional[float] = None) -> None:
        """Record when a queued command was written to the TCP socket."""
        ts = time.time() if when is None else float(when)
        with self.command_state_lock:
            self.last_command_sent_at = ts

    def send_command(self, command_kwargs: Dict[str, Any], timeout: float = RUNTIME_COMMAND_BASE_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Send a local command via the existing control session."""
        if not self.is_alive():
            raise RuntimeError("Pixie control session is not running")
        if self.needs_restart():
            raise RuntimeError(f"Pixie control session is unhealthy: {self.health_summary()}")
        if not self.wait_until_primed(timeout=min(timeout, 5.0)):
            raise TimeoutError(f"Pixie control session is not primed (state={self.ready_state})")

        command_id, commands_ahead = self.reserve_command_slot()
        effective_timeout = min(
            RUNTIME_COMMAND_MAX_TIMEOUT_SECONDS,
            max(timeout, RUNTIME_COMMAND_BASE_TIMEOUT_SECONDS) + (commands_ahead * RUNTIME_COMMAND_PER_AHEAD_SECONDS),
        )
        response_queue: "queue.Queue[Tuple[str, Any]]" = queue.Queue(maxsize=1)
        self.command_queue.put({
            "command_id": command_id,
            "kwargs": dict(command_kwargs),
            "response_queue": response_queue,
        })
        try:
            status, payload = response_queue.get(timeout=effective_timeout)
        except queue.Empty as exc:
            backlog = self.command_backlog_snapshot()
            raise TimeoutError(
                "Timed out waiting for live Pixie command completion "
                f"(command_id={command_id}, ahead={commands_ahead}, timeout={effective_timeout:.1f}s, "
                f"queued_now={backlog['queued']}, active_command_id={backlog['active_command_id']}, "
                f"active_for={backlog['active_for']})"
            ) from exc

        if status == "error":
            if isinstance(payload, Exception):
                raise payload
            raise RuntimeError(str(payload))

        return payload if isinstance(payload, dict) else {"status": payload}

    @property
    def error(self) -> Optional[Exception]:
        return self.control_result.get("error")

    @property
    def result(self) -> Optional[Dict[str, Any]]:
        return self.control_result.get("result")


@dataclass
class PixieRuntimeData:
    """Live runtime objects intended to back the future HA integration layer."""

    handler: "PixieAuthHandler"
    runtime_session: Optional[PixieRuntimeSession]
    inventory: Optional[PixieInventory]
    inventory_mode: str


# ============================================================================
# API CONFIGURATION
# ============================================================================

def listen_for_responses(sock: socket.socket, timeout: int = 10) -> Tuple[List[PixieHub], Optional[Dict[str, Any]]]:
    """
    Listen for UDP responses and parse envelope format (matches Java q0/b.java).

    Args:
        sock: Open UDP socket
        timeout: Maximum time to listen

    Returns:
        Tuple of (list of discovered hubs, decoded envelope dict from first valid response)
    """
    hubs_found = []
    first_decoded = None
    seen_ips = set()
    printed_identifier = False

    try:
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            sock.settimeout(1.0)
            try:
                data, addr = sock.recvfrom(65536)
                src_ip, src_port = addr

                # Try to parse as JSON response (matches Java q0/b.java F method)
                try:
                    response = json.loads(data.decode('utf-8', errors='ignore'))

                    gateway_type = response.get('type', '')
                    gateway_meshnet = response.get('meshNet', '')
                    gateway_meshnet2 = response.get('meshNet2', '')
                    gateway_from = response.get('from', '')

                    if not printed_identifier:
                        LOGGER.debug(
                            "Hub broadcast: type=%s meshNet=%s meshNet2=%s from=%s",
                            gateway_type,
                            gateway_meshnet,
                            gateway_meshnet2,
                            gateway_from,
                        )
                        printed_identifier = True

                    if first_decoded is None:
                        first_decoded = response

                    # Check for gateway response (matches Java validation)
                    gateway_type = response.get('type', '')
                    gateway_meshnet = response.get('meshNet', '')
                    gateway_meshnet2 = response.get('meshNet2', '')

                    if gateway_type == 'GW' and (gateway_meshnet or gateway_meshnet2):
                        if src_ip in seen_ips:
                            LOGGER.debug("Duplicate gateway advert from %s skipped", src_ip)
                            continue
                        hub = PixieHub(src_ip, UDP_DISCOVERY_PORT)
                        hub.is_valid = True
                        hubs_found.append(hub)
                        seen_ips.add(src_ip)
                        LOGGER.debug("Valid gateway discovered at %s:%s", src_ip, src_port)
                        break

                except json.JSONDecodeError:
                    # Fallback: try to decode as raw envelope
                    decoded = PixieEnvelope.decode(data, None)  # No key for broadcast
                    if decoded:
                        gateway_type = decoded.get('type', '')
                        gateway_meshnet = decoded.get('meshNet', '')
                        gateway_meshnet2 = decoded.get('meshNet2', '')
                        gateway_from = decoded.get('from', '')

                        if not printed_identifier:
                            LOGGER.debug(
                                "Hub broadcast: type=%s meshNet=%s meshNet2=%s from=%s",
                                gateway_type,
                                gateway_meshnet,
                                gateway_meshnet2,
                                gateway_from,
                            )
                            printed_identifier = True

                        if first_decoded is None:
                            first_decoded = decoded

                        if gateway_type == 'GW' and (gateway_meshnet or gateway_meshnet2):
                            if src_ip in seen_ips:
                                LOGGER.debug("Duplicate gateway advert from %s skipped", src_ip)
                                continue
                            hub = PixieHub(src_ip, UDP_DISCOVERY_PORT)
                            hub.is_valid = True
                            hubs_found.append(hub)
                            seen_ips.add(src_ip)
                            LOGGER.debug("Valid gateway discovered at %s:%s", src_ip, src_port)
                            break
                    else:
                        LOGGER.debug("Could not decode UDP response from %s:%s (len=%s)", src_ip, src_port, len(data))

            except socket.timeout:
                continue
            except Exception as e:
                LOGGER.debug("Error processing UDP response: %s", e)

    except Exception as e:
        LOGGER.warning("Gateway discovery receive loop failed: %s", e)
    finally:
        sock.close()

    return hubs_found, first_decoded


# ============================================================================
# API CONFIGURATION
# ============================================================================

# Real Pixie Plus Cloud API endpoints (from pixiepluslogin.py)
API_URL = {
    "login": "https://www.pixie.app/p0/pixieCloud/login",
    "user_query": "https://www.pixie.app/p0/pixieCloud/functions/userQuery",
    "home": "https://www.pixie.app/p0/pixieCloud/classes/Home",
    "livegroup": "https://www.pixie.app/p0/pixieCloud/classes/LiveGroup",
}

# Pixie Plus Constants (from Android app)
APPLICATION_ID = "6426f04c206c108275ede71b9fd09ac8"
CLIENT_KEY = "35779bd411c751ff87577cd762118dad"

# Network defaults (from Android code)
UDP_DISCOVERY_PORT = 41580
TCP_CONTROL_PORT = 41578
TCP_SYNC_PORT = 53216


class PixieAuthHandler:
    """
    Handles complete autonomous authentication flow for Pixie Plus.

    Fully automatic process:
    1. Scan LAN via UDP broadcast to discover hubs (no IP required from user)
    2. Set up MITM proxy on discovered hub port
    3. Capture handshake traffic during TCP connection
    4. Extract netID, meshNet from login response
    5. Extract session key using netID as decryption seed
    6. Store credentials + key for all future commands
    """

    def __init__(self, credentials_path: Optional[str] = None, verbose: bool = False):
        self.verbose = verbose
        self.suppress_heartbeat_logs = False
        self.netid_seed = None
        self.dump_structures = False
        self.dump_dir = "debug_dumps"
        self.meshnet = None
        self.meshnet2 = None
        self.home_id: Optional[str] = None
        self.home_name: Optional[str] = None
        self.user_id: Optional[str] = None
        self.session_token: Optional[str] = None
        self.session_key_hex = None
        self.current_hub: Optional[Dict[str, Any]] = None
        self.inventory: Optional[PixieInventory] = None
        self.gateway_identity: Optional[GatewayIdentity] = None
        self.runtime_session: Optional[PixieRuntimeSession] = None
        self.stored_username: Optional[str] = None
        self.stored_password: Optional[str] = None
        self.inventory_mode: str = "local_53216"
        self._command_counter = 0x10  # Start at 0x10 to match app UI: 10, 11, 12, 13...
        self._cached_cloud_home_obj: Optional[Dict[str, Any]] = None
        self._pending_bulk_ble_updates: List[Dict[str, Any]] = []
        self._pending_bulk_lock = threading.Lock()
        self._inventory_update_callback: Optional[Callable[[PixieInventory], None]] = None
        self._awaiting_initial_gwdata_bulk = False

    def _debug_enabled(self) -> bool:
        return self.verbose or LOGGER.isEnabledFor(logging.DEBUG)

    def _log_debug(self, message: str, *args: Any) -> None:
        LOGGER.debug(message, *args)

    def _log_info(self, message: str, *args: Any) -> None:
        LOGGER.info(message, *args)

    def _log_warning(self, message: str, *args: Any) -> None:
        LOGGER.warning(message, *args)

    def _log_error(self, message: str, *args: Any) -> None:
        LOGGER.error(message, *args)

    def _log_exception(self, message: str, *args: Any) -> None:
        LOGGER.exception(message, *args)

    def _log_multiline_debug(self, header: str, lines: List[str]) -> None:
        if not self._debug_enabled():
            return
        if lines:
            LOGGER.debug("%s\n%s", header, "\n".join(lines))
        else:
            LOGGER.debug("%s", header)

    def set_inventory_update_callback(
        self,
        callback: Optional[Callable[[PixieInventory], None]],
    ) -> None:
        """Register a callback invoked after runtime inventory changes."""
        self._inventory_update_callback = callback

    def _notify_inventory_updated(self) -> None:
        """Notify the integration layer that runtime inventory changed."""
        if self.inventory is None or self._inventory_update_callback is None:
            return
        try:
            self._inventory_update_callback(self.inventory)
        except Exception as exc:
            self._log_debug("Inventory update callback failed: %s", exc)

    def _dump_structure_json(self, filename: str, payload: Any) -> None:
        """Write optional debug structure JSON files for offline shape comparison."""
        if not self.dump_structures:
            return
        try:
            os.makedirs(self.dump_dir, exist_ok=True)
            out_path = os.path.join(self.dump_dir, filename)
            with open(out_path, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, ensure_ascii=False, indent=2)
            self._log_debug("Dumped structure JSON: %s", out_path)
        except Exception as e:
            self._log_warning("Could not dump structure file %s: %s", filename, e)

    def _queue_bulk_ble_records(self, records: List[Dict[str, Any]], source: str, *, full_snapshot: bool) -> None:
        """Queue bulk bleData records for later application once inventory exists."""
        if not records:
            return
        with self._pending_bulk_lock:
            self._pending_bulk_ble_updates.append({
                "source": source,
                "records": records,
                "full_snapshot": full_snapshot,
            })

    def _drain_bulk_ble_records(self) -> List[Dict[str, Any]]:
        """Drain queued bulk bleData record batches."""
        with self._pending_bulk_lock:
            batches = list(self._pending_bulk_ble_updates)
            self._pending_bulk_ble_updates.clear()
        return batches

    @staticmethod
    def _decode_bulk_br(value: int) -> Dict[str, Any]:
        """Decode GwData bulk br field into single or dual-channel state."""
        if value >= 12 and (value & ~0x03) != 0 and value <= 127:
            upper = value & ~0x03
            if upper in (12, 16):
                return {
                    "type": "multi",
                    "raw": value,
                    "ch1": bool(value & 0x01),
                    "ch2": bool(value & 0x02),
                }
        return {
            "type": "single",
            "raw": value,
            "pct": value,
        }

    def _apply_bulk_ble_records_to_inventory(self, records: List[Dict[str, Any]], source: str, *, full_snapshot: bool) -> int:
        """Apply bulk bleData records to inventory runtime using minimal state fields.

        Only presence, brightness (scalar models), bitmask state via runtime.r,
        derived is_on, and runtime.last_source are updated.
        """
        if not self.inventory or not records:
            return 0
        applied = self.inventory.apply_gwdata_bulk(records, source, full_snapshot=full_snapshot)
        if applied > 0:
            self._notify_inventory_updated()
        return applied

    def _apply_cloud_params(self, cloud_params: CloudParams) -> None:
        """Apply cloud-derived parameters to the handler's in-memory context."""
        self.home_id = cloud_params.home_id
        self.home_name = cloud_params.home_name
        self.user_id = cloud_params.user_id
        self.meshnet = cloud_params.meshnet
        self.meshnet2 = cloud_params.meshnet2
        self.netid_seed = cloud_params.netid

    def _current_gateway_identity(self) -> Optional[GatewayIdentity]:
        """Return the current parsed gateway identity from inventory if available."""
        if self.inventory and self.inventory.gateway:
            self.gateway_identity = self.inventory.gateway
        return self.gateway_identity

    def _build_auth_result_snapshot(self, hub_ip: str, hub_port: int) -> Dict[str, Any]:
        """Build the legacy auth result shape from current in-memory handler state."""
        return {
            "status": "success",
            "config": {
                "netid": self.netid_seed,
                "meshnet": self.meshnet,
                "meshnet2": self.meshnet2,
            },
            "session_key_hex": self.session_key_hex,
            "hub_ip": hub_ip,
            "hub_port": hub_port,
        }

    def _resolve_gateway_ip(self, gateway_ip: Optional[str]) -> Optional[str]:
        """Resolve a gateway IP either from the caller or via UDP discovery."""
        if gateway_ip:
            self._log_debug("Using explicit gateway IP: %s", gateway_ip)
            return gateway_ip

        self._log_debug("Scanning LAN for Pixie gateways")
        discovered_hubs = self.scan_lan_for_hubs()

        if not discovered_hubs:
            self._log_warning("No gateways discovered via UDP broadcast")
            return None

        if len(discovered_hubs) == 1 and discovered_hubs[0].is_valid:
            resolved_host = discovered_hubs[0].host
            self._log_debug("Auto-selected gateway: %s", resolved_host)
            return resolved_host

        self._log_warning("Multiple gateways discovered; unable to choose automatically")
        return None

    def _start_runtime_session(
        self,
        hub_ip: str,
        *,
        stop_event: Optional[threading.Event],
        keep_control_alive: bool,
        command_device_id: Optional[int],
        command_state: Optional[bool],
        command_brightness: Optional[int],
        command_color_rgb: Optional[Tuple[int, int, int]],
        command_white: bool,
        command_effect: Optional[str],
        command_target: Optional[str],
        command_cover_action: Optional[str],
        command_cover_action_map: Optional[Dict[str, int]],
        command_cover_tilt_action_map: Optional[Dict[str, int]],
    ) -> PixieRuntimeSession:
        """Start the long-lived 41578 runtime session."""
        runtime_session = PixieRuntimeSession(
            handler=self,
            host=hub_ip,
            port=TCP_CONTROL_PORT,
            keep_control_alive=keep_control_alive,
            stop_event=stop_event or threading.Event(),
            command_kwargs={
                "command_device_id": command_device_id,
                "command_state": command_state,
                "command_brightness": command_brightness,
                "command_color_rgb": command_color_rgb,
                "command_white": command_white,
                "command_effect": command_effect,
                "command_target": command_target,
                "command_cover_action": command_cover_action,
                "command_cover_action_map": command_cover_action_map,
                "command_cover_tilt_action_map": command_cover_tilt_action_map,
            },
        )
        runtime_session.start()
        self.runtime_session = runtime_session
        return runtime_session

    def _hydrate_local_inventory(
        self,
        runtime_session: PixieRuntimeSession,
        *,
        hub_ip: str,
        sync_timeout: float,
        cloud_home_cached: Optional[Dict[str, Any]],
    ) -> bool:
        """Build the initial inventory from 53216 plus runtime GwData when available."""
        self._log_debug("Hydrating startup inventory from %s:%s", hub_ip, TCP_SYNC_PORT)
        inventory_loaded = False
        hub_payload: Optional[Dict[str, Any]] = None
        net_id_int = int(str(self.netid_seed)) if self.netid_seed not in (None, "", "unknown") else None

        if net_id_int is not None:
            try:
                self._log_debug("Attempting one-shot %s inventory request", TCP_SYNC_PORT)
                sync_result = self._sync_inventory_53216_once(
                    hub_ip=hub_ip,
                    net_id_int=net_id_int,
                    mesh_net2_int=int(self.meshnet2),
                    timeout=sync_timeout,
                )
                payload = self._extract_53216_inventory_payload(sync_result.get("data"))
                if payload:
                    if not payload.get("objectId") and self.home_id:
                        payload["objectId"] = self.home_id
                    if not payload.get("name") and self.home_name not in (None, "", "unknown"):
                        payload["name"] = self.home_name
                    if payload.get("netID") is None and self.netid_seed not in (None, "", "unknown"):
                        payload["netID"] = self.netid_seed
                    if payload.get("meshNet") is None and self.meshnet not in (None, "", "unknown"):
                        payload["meshNet"] = self.meshnet
                    if payload.get("meshNet2") is None and self.meshnet2 not in (None, "", "unknown"):
                        payload["meshNet2"] = self.meshnet2
                    hub_payload = payload
                    self._log_debug("Captured gateway identity payload from port %s", TCP_SYNC_PORT)
                else:
                    self._log_warning("Port %s returned no usable startup inventory payload", TCP_SYNC_PORT)
            except Exception as sync_err:
                self._log_warning("Gateway %s startup inventory failed: %s", TCP_SYNC_PORT, sync_err)
        else:
            self._log_warning("netID unavailable; skipping %s startup inventory", TCP_SYNC_PORT)

        if hub_payload:
            self._log_debug("Waiting up to %ss for GwData bulk state", max(0.5, float(sync_timeout)))
            gw_deadline = time.time() + max(0.5, float(sync_timeout))
            while time.time() < gw_deadline:
                if runtime_session.ready_state.get("saw_bulk_bledata"):
                    break
                if runtime_session.error is not None:
                    break
                time.sleep(0.1)

            if runtime_session.ready_state.get("saw_bulk_bledata"):
                inventory_user_id = str(self.user_id) if self.user_id not in (None, "", "unknown") else "unknown"
                self._set_inventory_from_home_object(hub_payload, inventory_user_id, source="hub_53216")
                pending_batches = self._drain_bulk_ble_records()
                total_applied = 0
                updated_ids = set()
                for batch in pending_batches:
                    batch_source = str(batch.get("source") or "hub_gwdata")
                    batch_full_snapshot = bool(batch.get("full_snapshot", False))
                    records = batch.get("records") if isinstance(batch.get("records"), list) else []
                    total_applied += self._apply_bulk_ble_records_to_inventory(
                        records,
                        source=batch_source,
                        full_snapshot=batch_full_snapshot,
                    )
                    for rec_data in records:
                        try:
                            rec_id = int(rec_data.get("id"))
                        except Exception:
                            continue
                        if rec_id in self.inventory.devices_by_id:
                            updated_ids.add(rec_id)
                self._log_debug("Applied %s GwData bulk runtime updates", total_applied)

                all_ids = set(self.inventory.devices_by_id.keys()) if self.inventory else set()
                missing_ids = sorted(all_ids - updated_ids)
                if missing_ids:
                    self._log_debug(
                        "GwData bulk discrepancy: updated=%s inventory=%s missing=%s",
                        len(updated_ids),
                        len(all_ids),
                        len(missing_ids),
                    )
                    for miss_id in missing_ids:
                        miss_rec = self.inventory.devices_by_id.get(miss_id)
                        if not miss_rec:
                            continue
                        self._log_debug(
                            "GwData missing inventory device: id=%s model=%s name=%s",
                            miss_rec.id,
                            miss_rec.model_no,
                            miss_rec.name,
                        )
                else:
                    self._log_debug("GwData bulk covered all inventory devices")

                if self.verbose:
                    post_debug_dump = self.inventory.debug_lines_verbose()
                    self._log_multiline_debug("Final startup inventory snapshot after runtime hydration", post_debug_dump)
                else:
                    post_debug_dump = self.inventory.debug_lines()
                    self._log_multiline_debug("Final startup inventory summary", post_debug_dump)
                inventory_loaded = True
                self._log_debug("Startup inventory source: hub %s + GwData bulk", TCP_SYNC_PORT)
            else:
                self._log_warning("GwData bulk not ready before timeout; using cloud fallback snapshot")

        self._awaiting_initial_gwdata_bulk = False

        if inventory_loaded:
            self.inventory_mode = "local_53216"
            return True

        self._log_debug("Falling back to Home API inventory snapshot")
        home_obj = cloud_home_cached
        if home_obj is None:
            home_obj = self._fetch_home_object(
                homeid=str(self.home_id) if self.home_id else None,
                sessiontoken=str(self.session_token) if self.session_token else None,
            )
        if home_obj:
            inventory_user_id = str(self.user_id) if self.user_id not in (None, "", "unknown") else "unknown"
            fallback_source = "cloud_fallback_cached" if home_obj is cloud_home_cached else "cloud_fallback"
            self._set_inventory_from_home_object(home_obj, inventory_user_id, source=fallback_source)
            self.inventory_mode = "cloud_fallback"
            self._log_debug("Startup inventory source: %s", fallback_source)
            return True

        self._log_warning("Home API fallback inventory unavailable")
        return False

    def fetch_cloud_params(
        self,
        username: str,
        password: str,
        *,
        include_inventory_seed: bool = True,
    ) -> CloudParams:
        """Fetch cloud-derived parameters required to access the local gateway."""
        config = self._fetch_login_data(username, password, include_inventory_seed=include_inventory_seed)
        cloud_params = CloudParams(
            home_id=str(config.get("homeid") or "unknown"),
            home_name=str(config.get("home_name") or "unknown"),
            user_id=str(config.get("userid") or "unknown"),
            meshnet=str(config.get("meshnet") or "unknown"),
            meshnet2=str(config.get("meshnet2") or "unknown"),
            netid=str(config.get("netid") or "unknown"),
        )
        self._apply_cloud_params(cloud_params)
        self.session_token = config.get("sessiontoken")
        return cloud_params

    async def async_fetch_cloud_params(
        self,
        username: str,
        password: str,
        *,
        include_inventory_seed: bool = True,
    ) -> CloudParams:
        """Async wrapper for cloud parameter retrieval."""
        return await asyncio.to_thread(
            self.fetch_cloud_params,
            username,
            password,
            include_inventory_seed=include_inventory_seed,
        )

    def bootstrap_gateway(
        self,
        cloud_params: CloudParams,
        *,
        username: str,
        password: str,
        gateway_ip: Optional[str] = None,
        sync_timeout: float = 5.0,
        command_device_id: Optional[int] = None,
        command_state: Optional[bool] = None,
        command_brightness: Optional[int] = None,
        command_color_rgb: Optional[Tuple[int, int, int]] = None,
        command_white: bool = False,
        command_effect: Optional[str] = None,
        command_target: Optional[str] = None,
        command_cover_action: Optional[str] = None,
        command_cover_action_map: Optional[Dict[str, int]] = None,
        command_cover_tilt_action_map: Optional[Dict[str, int]] = None,
        stop_event: Optional[threading.Event] = None,
        keep_control_alive: bool = False,
        wait_for_shutdown: bool = False,
        hydrate_inventory: bool = True,
    ) -> PixieRuntimeData:
        """Bootstrap local gateway access using already-fetched cloud parameters."""
        self._apply_cloud_params(cloud_params)
        self.stored_username = username
        self.stored_password = password

        auth_result = self.discover_and_connect(
            username=username,
            password=password,
            hub_ip=gateway_ip,
            login_required=False,
            sync_timeout=sync_timeout,
            command_device_id=command_device_id,
            command_state=command_state,
            command_brightness=command_brightness,
            command_color_rgb=command_color_rgb,
            command_white=command_white,
            command_effect=command_effect,
            command_target=command_target,
            command_cover_action=command_cover_action,
            command_cover_action_map=command_cover_action_map,
            command_cover_tilt_action_map=command_cover_tilt_action_map,
            stop_event=stop_event,
            keep_control_alive=keep_control_alive,
            wait_for_shutdown=wait_for_shutdown,
            hydrate_inventory=hydrate_inventory,
        )

        self._current_gateway_identity()
        return PixieRuntimeData(
            handler=self,
            runtime_session=self.runtime_session,
            inventory=self.inventory,
            inventory_mode=self.inventory_mode,
        )

    async def async_bootstrap_gateway(self, cloud_params: CloudParams, **kwargs: Any) -> PixieRuntimeData:
        """Async wrapper for local gateway bootstrap."""
        return await asyncio.to_thread(self.bootstrap_gateway, cloud_params, **kwargs)

    def scan_lan_for_hubs(self, broadcast_address: str = "255.255.255.255",
                          timeout: int = 10) -> List[PixieHub]:
        """
        Scan local network for Pixie hubs via UDP broadcast.

        Matches q0.b logic from Android app - discovers gateways automatically.

        Args:
            broadcast_address: IPv4 multicast/broadcast address (default: 255.255.255.255)
            timeout: Seconds to wait for responses

        Returns:
            List of discovered Hub objects
        """

        self._log_debug("Scanning LAN for Pixie hubs via UDP broadcast")

        hubs_found: List[PixieHub] = []
        sock: Optional[socket.socket] = None

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", UDP_DISCOVERY_PORT))
            sock.settimeout(timeout)

            self._log_debug(
                "Listening for UDP broadcasts from hubs on port %s (passive mode)",
                UDP_DISCOVERY_PORT,
            )

            hubs_found, decoded = listen_for_responses(sock, timeout=timeout)

            if decoded:
                meshnet = decoded.get("meshNet")
                if meshnet:
                    self.meshnet = meshnet
                    self._log_debug("Updated meshNet from UDP response: %s", meshnet)
                meshnet2 = decoded.get("meshNet2")
                if meshnet2:
                    self.meshnet2 = meshnet2
                    self._log_debug("Updated meshNet2 from UDP response: %s", meshnet2)
                if meshnet2 and not self.meshnet:
                    self.meshnet = meshnet2
                    self._log_debug("Updated meshNet from meshNet2 fallback: %s", meshnet2)

        except Exception as exc:
            self._log_warning("Discovery scan error: %s", exc)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

        self._log_debug("Discovery scan finished with %s hub(s)", len(hubs_found))
        return hubs_found

    def discover_and_connect(
        self,
        username: str,
        password: str,
        hub_ip: Optional[str] = None,
        login_required: bool = True,
        sync_timeout: float = 5.0,
        command_device_id: Optional[int] = None,
        command_state: Optional[bool] = None,
        command_brightness: Optional[int] = None,
        command_color_rgb: Optional[Tuple[int, int, int]] = None,
        command_white: bool = False,
        command_effect: Optional[str] = None,
        command_target: Optional[str] = None,
        command_cover_action: Optional[str] = None,
        command_cover_action_map: Optional[Dict[str, int]] = None,
        command_cover_tilt_action_map: Optional[Dict[str, int]] = None,
        stop_event: Optional[threading.Event] = None,
        keep_control_alive: bool = True,
        wait_for_shutdown: bool = True,
        hydrate_inventory: bool = True,
    ) -> Dict[str, Any]:
        """
        Full autonomous discovery + connection flow.

        Args:
            username: User's email/username
            password: User's password
            hub_ip: Optional override for specific hub IP (for testing)
            command_device_id: Optional device ID to command after handshake
            command_state: Optional target on/off state for local command
            command_brightness: Optional brightness 0-100 for brightness command
            command_color_rgb: Optional RGB tuple for color command
            command_white: Whether color command was requested via --white
            command_effect: Optional effect name command
            command_target: Optional target endpoint for on/off command (relay|usb|left|right|both)
            command_cover_action: Optional cover action command

        Returns:
            Complete auth result with extracted credentials and session key
        """

        self._log_debug("Starting Pixie gateway bootstrap flow")

        with self._pending_bulk_lock:
            self._pending_bulk_ble_updates.clear()
        self._awaiting_initial_gwdata_bulk = True

        # Step 1: Fetch metadata from cloud API only when required.
        needs_login = login_required or not all([self.netid_seed, self.meshnet])
        config: Dict[str, Any] = {
            "netid": self.netid_seed,
            "meshnet": self.meshnet,
            "meshnet2": self.meshnet2,
            "homeid": self.home_id,
            "home_name": self.home_name,
            "userid": self.user_id,
            "sessiontoken": self.session_token,
        }
        if needs_login:
            self._log_debug("Fetching metadata from Pixie cloud API")
            config = self._fetch_login_data(username, password, include_inventory_seed=False)
        else:
            self._log_debug("Using cached login metadata")

        cloud_home_cached = self._cached_cloud_home_obj if isinstance(self._cached_cloud_home_obj, dict) else None
        if cloud_home_cached:
            self._log_debug("Cloud Home snapshot cached for startup fallback")
        else:
            self._log_debug("No cached cloud Home snapshot available")

        # Store metadata for hub discovery/53216/control orchestration
        self.meshnet = config.get('meshnet')
        self.meshnet2 = config.get('meshnet2')
        self.netid_seed = config.get('netid')
        self.home_id = config.get('homeid')
        self.home_name = config.get('home_name')
        self.user_id = config.get('userid')
        self.session_token = config.get('sessiontoken')

        # also store credentials for the step 3 save path
        self.stored_username = username
        self.stored_password = password

        self._log_debug("Bootstrap metadata: meshNet=%s meshNet2=%s netID=%s", self.meshnet, self.meshnet2, self.netid_seed)

        hub_ip = self._resolve_gateway_ip(hub_ip)
        if not hub_ip:
            return None

        # Step 3: Start 41578 control loop in background and keep it alive.
        self._log_debug("Starting TCP control channel on %s:%s", hub_ip, TCP_CONTROL_PORT)
        runtime_session = self._start_runtime_session(
            hub_ip,
            stop_event=stop_event,
            keep_control_alive=keep_control_alive,
            command_device_id=command_device_id,
            command_state=command_state,
            command_brightness=command_brightness,
            command_color_rgb=command_color_rgb,
            command_white=command_white,
            command_effect=command_effect,
            command_target=command_target,
            command_cover_action=command_cover_action,
            command_cover_action_map=command_cover_action_map,
            command_cover_tilt_action_map=command_cover_tilt_action_map,
        )

        priming_timeout = 5.0
        primed = runtime_session.wait_until_primed(timeout=priming_timeout)
        if primed:
            self._log_debug("41578 primed; starting one-shot %s inventory hydration", TCP_SYNC_PORT)
        else:
            self._log_warning("41578 priming timeout; continuing startup inventory with state=%s", runtime_session.ready_state)

        if hydrate_inventory:
            self._hydrate_local_inventory(
                runtime_session,
                hub_ip=hub_ip,
                sync_timeout=sync_timeout,
                cloud_home_cached=cloud_home_cached,
            )

        if keep_control_alive and wait_for_shutdown:
            self._log_debug("Control channel remains active on port %s awaiting shutdown", TCP_CONTROL_PORT)
            try:
                while runtime_session.is_alive():
                    runtime_session.join(timeout=0.5)
            except KeyboardInterrupt:
                self._log_info("Stop requested while waiting for control channel shutdown")
                runtime_session.stop_and_join(timeout=3.0)
                result = runtime_session.result
                if result:
                    return result
                raise PixieAuthError("Stopped by user")
        elif not keep_control_alive:
            runtime_session.stop_and_join(timeout=5.0)

        if runtime_session.error is not None:
            raise PixieAuthError(f"Control channel failed: {runtime_session.error}")

        if wait_for_shutdown or not keep_control_alive:
            auth_result = runtime_session.result
            if not auth_result:
                raise PixieAuthError("Handshake capture failed - ensure hub is reachable")
            return auth_result

        auth_snapshot = self._build_auth_result_snapshot(hub_ip, TCP_CONTROL_PORT)
        if not auth_snapshot.get("session_key_hex"):
            raise PixieAuthError("Handshake did not yield a session key before bootstrap completion")
        return auth_snapshot

    @staticmethod
    def _derive_sync_53216_key(unix_seconds: int, net_id: int) -> bytes:
        """Derive the 16-byte AES key used by the 53216 EA/EB exchange."""
        xor_val = int(net_id) ^ int(unix_seconds)
        combined = "Pixie" + format(xor_val & 0xFFFFFFFFFFFFFFFF, "x")
        arr = bytearray(16)
        for i, ch in enumerate(combined[:16]):
            arr[i] = ord(ch)
        return bytes(arr)

    @staticmethod
    def _build_sync_53216_ea(unix_seconds: int, net_id: int, nonce: int) -> bytes:
        """Build EA wire message: ea + len + nonce + base64(0x01 + AES_CBC(payload))."""
        iv = b"0" * 16
        key = PixieAuthHandler._derive_sync_53216_key(unix_seconds, net_id)
        plaintext = b'{"get":{"selected":127}}'
        ciphertext = AES.new(key, AES.MODE_CBC, iv).encrypt(pad(plaintext, 16))
        payload_b64 = base64.b64encode(bytes([0x01]) + ciphertext).decode("ascii")
        wire = f"ea{len(payload_b64):08x}{nonce:08x}{payload_b64}".encode("ascii")
        return wire

    @staticmethod
    def _derive_sync_53216_nonce(unix_seconds: int, mesh_net2: int) -> int:
        """Derive 53216 nonce from timestamp and meshNet2.

        nonce_high_byte = (meshNet2 >> 16) & 0xFF
        xor_const       = meshNet2 & 0xFFFFFF
        nonce_low24     = (unix_seconds & 0xFFFFFF) ^ xor_const
        nonce           = (nonce_high_byte << 24) | nonce_low24
        """
        xor_const = int(mesh_net2) & 0xFFFFFF
        nonce_high = (int(mesh_net2) >> 16) & 0xFF
        ts_low24 = int(unix_seconds) & 0xFFFFFF
        nonce_low24 = ts_low24 ^ xor_const
        return ((nonce_high << 24) | (nonce_low24 & 0xFFFFFF)) & 0xFFFFFFFF

    @staticmethod
    def _read_sync_53216_eb_frames(
        sock: socket.socket,
        timeout: float = 5.0,
    ) -> Tuple[bytes, List[str], List[Dict[str, Any]]]:
        """Read EB bytes from 53216 and return raw bytes, payload chunks, and parse metadata."""
        def _parse_frames(raw_ascii: str) -> Tuple[List[str], List[Dict[str, Any]], bool]:
            payload_parts: List[str] = []
            frame_infos: List[Dict[str, Any]] = []
            idx = 0
            saw_incomplete = False

            while idx < len(raw_ascii):
                marker = raw_ascii.find("eb", idx)
                if marker < 0:
                    break
                if marker + 10 > len(raw_ascii):
                    frame_infos.append({
                        "marker": marker,
                        "incomplete_header": True,
                        "available": len(raw_ascii) - marker,
                        "required": 10,
                    })
                    saw_incomplete = True
                    break

                length_hex = raw_ascii[marker + 2:marker + 10]
                try:
                    payload_len = int(length_hex, 16)
                except ValueError:
                    idx = marker + 2
                    continue

                payload_start = marker + 10
                payload_end = payload_start + payload_len
                if payload_end > len(raw_ascii):
                    frame_infos.append({
                        "marker": marker,
                        "length_hex": length_hex,
                        "payload_len": payload_len,
                        "payload_start": payload_start,
                        "payload_end": payload_end,
                        "in_bounds": False,
                        "missing_bytes": payload_end - len(raw_ascii),
                    })
                    saw_incomplete = True
                    break

                payload = raw_ascii[payload_start:payload_end]
                payload_parts.append(payload)
                frame_infos.append({
                    "marker": marker,
                    "length_hex": length_hex,
                    "payload_len": payload_len,
                    "payload_start": payload_start,
                    "payload_end": payload_end,
                    "in_bounds": True,
                    "payload_preview": payload[:48],
                })
                idx = payload_end

            return payload_parts, frame_infos, saw_incomplete

        chunks: List[bytes] = []
        per_recv_timeout = min(max(float(timeout), 0.1), 1.0)
        sock.settimeout(per_recv_timeout)
        deadline = time.time() + max(timeout * 4.0, 10.0)

        payload_parts: List[str] = []
        frame_infos: List[Dict[str, Any]] = []
        saw_incomplete = False

        while time.time() < deadline:
            try:
                packet = sock.recv(4096)
                if not packet:
                    break
                chunks.append(packet)
            except socket.timeout:
                # Keep waiting if headers advertise more bytes than currently buffered.
                if saw_incomplete:
                    continue
                if payload_parts:
                    break
                continue

            raw_ascii = b"".join(chunks).decode("ascii", errors="ignore")
            payload_parts, frame_infos, saw_incomplete = _parse_frames(raw_ascii)
            if payload_parts and not saw_incomplete:
                break

        raw = b"".join(chunks)
        if not raw:
            raise PixieAuthError("No data received from port 53216")

        raw_ascii = raw.decode("ascii", errors="ignore")
        payload_parts, frame_infos, saw_incomplete = _parse_frames(raw_ascii)

        if saw_incomplete:
            missing = None
            if frame_infos:
                last = frame_infos[-1]
                missing = last.get("missing_bytes") or max(0, int(last.get("required", 0)) - int(last.get("available", 0)))
            raise PixieAuthError(
                "Incomplete EB frame from 53216 response"
                + (f" (missing~{missing} bytes)" if isinstance(missing, int) else "")
            )

        if not payload_parts:
            raise PixieAuthError("Could not parse EB frame header(s) from 53216 response")

        return raw, payload_parts, frame_infos

    @staticmethod
    def _decrypt_sync_53216_eb_payload(unix_seconds: int, net_id: int, eb_payload_b64: str) -> Tuple[Any, str]:
        """Decrypt EB payload and return parsed JSON object + decode mode.

        The primary path intentionally matches decrypt_test3.py exactly:
        base64-decode -> AES-CBC decrypt -> unpad(or rstrip(0x00)) -> base64-decode -> JSON.
        """
        iv = b"0" * 16
        key = PixieAuthHandler._derive_sync_53216_key(unix_seconds, net_id)

        cleaned = "".join(eb_payload_b64.split())
        enc = base64.b64decode(cleaned + "=" * ((-len(cleaned)) % 4))
        enc_raw_len = len(enc)
        if len(enc) % 16 != 0:
            enc = enc[: len(enc) - (len(enc) % 16)]
        if not enc:
            raise PixieAuthError("EB ciphertext is empty after block alignment")

        pt = AES.new(key, AES.MODE_CBC, iv).decrypt(enc)
        try:
            pt = unpad(pt, 16)
        except Exception:
            pt = pt.rstrip(b"\x00")

        # Strict decrypt_test3 behavior: decrypted bytes are base64-wrapped JSON.
        try:
            json_bytes = base64.b64decode(pt)
            return json.loads(json_bytes.decode("utf-8")), "json_b64_wrapped_exact"
        except Exception:
            pass

        # Fallback A: direct UTF-8 JSON.
        try:
            text = pt.decode("utf-8")
            if text.lstrip().startswith("{"):
                return json.loads(text), "json_direct"
        except Exception:
            pass

        # Fallback B: base64 with optional padding recovery.
        try:
            json_bytes = base64.b64decode(pt + b"=" * ((-len(pt)) % 4))
            return json.loads(json_bytes.decode("utf-8")), "json_b64_wrapped"
        except Exception:
            pass

        # Fallback C: leading status byte + base64-wrapped JSON.
        if len(pt) > 1:
            try:
                json_bytes = base64.b64decode(pt[1:] + b"=" * ((-len(pt[1:])) % 4))
                return json.loads(json_bytes.decode("utf-8")), "json_b64_wrapped_skip1"
            except Exception:
                pass

        # Provide small diagnostic preview for troubleshooting.
        preview_hex = pt[:32].hex()
        preview_ascii = "".join(chr(b) if 32 <= b < 127 else "." for b in pt[:64])
        key_hex = key.hex()
        raise PixieAuthError(
            "Decrypted payload not recognized as JSON "
            f"(key={key_hex}, eb_raw_len={enc_raw_len}, eb_ct_len={len(enc)}, "
            f"pt_len={len(pt)}, hex={preview_hex}, ascii={preview_ascii})"
        )

    @staticmethod
    def _is_valid_53216_inventory_payload(obj: Any) -> bool:
        """Return True only for strong inventory-like payloads (avoid false positives like `26`)."""
        if not isinstance(obj, dict):
            return False

        # Canonical shape from decrypt_test3/decrypt_test_EB_result:
        # {"result":"success", "data": {"deviceList": [...]}}
        if "data" in obj and isinstance(obj.get("data"), dict):
            data_obj = obj.get("data")
            if isinstance(data_obj.get("deviceList"), list):
                return True

        # Relaxed fallback: payload itself may be the data object.
        if isinstance(obj.get("deviceList"), list):
            return True

        return False

    @staticmethod
    def _extract_53216_inventory_payload(obj: Any) -> Optional[Dict[str, Any]]:
        """Extract inventory object from decrypted 53216 JSON response."""
        if not isinstance(obj, dict):
            return None

        data_obj = obj.get("data")
        if isinstance(data_obj, dict) and isinstance(data_obj.get("deviceList"), list):
            return dict(data_obj)

        if isinstance(obj.get("deviceList"), list):
            return dict(obj)

        return None

    def _set_inventory_from_home_object(
        self,
        home_obj: Dict[str, Any],
        user_id: str,
        source: str,
        *,
        show_devices: Optional[bool] = None,
    ) -> None:
        """Build and assign normalized inventory from a Home-like object payload."""
        self.inventory = PixieInventory.from_home_object(
            home_obj,
            user_id=str(user_id or "unknown"),
            source=source,
        )
        self.gateway_identity = self.inventory.gateway
        self._log_debug("Built inventory from %s", source)
        show = self.verbose if show_devices is None else bool(show_devices)
        if show:
            debug_dump = (
                self.inventory.debug_lines_verbose()
                if self.verbose
                else self.inventory.debug_lines()
            )
            self._log_multiline_debug(f"Inventory dump for {source}", debug_dump)
        else:
            self._log_debug(
                "Inventory summary: home=%s devices=%s netID=%s",
                self.inventory.home_id,
                len(self.inventory.devices_by_id),
                self.inventory.net_id,
            )

    def _fetch_home_object(self, homeid: Optional[str], sessiontoken: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch Home object from cloud for metadata fallback only."""
        if not homeid or homeid == "unknown" or not sessiontoken or sessiontoken == "unknown":
            return None

        try:
            import httpx

            headers = {
                "x-parse-session-token": sessiontoken,
                "x-parse-application-id": APPLICATION_ID,
                "x-parse-client-key": CLIENT_KEY,
            }
            body = {
                "where": json.dumps({"objectId": homeid}),
            }

            response = httpx.get(API_URL["home"], params=body, headers=headers)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and isinstance(data.get("results"), list) and data["results"]:
                home_obj = data["results"][0]
                self._dump_structure_json("cloud_home_object.json", home_obj)
                if isinstance(home_obj, dict):
                    self._dump_structure_json("cloud_home_onlineList.json", home_obj.get("onlineList") or {})
                    self._dump_structure_json("cloud_home_deviceList.json", home_obj.get("deviceList") or [])
                    self._dump_structure_json("cloud_home_groupList.json", home_obj.get("groupList") or [])
                    self._dump_structure_json("cloud_home_sceneList.json", home_obj.get("sceneList") or [])
                return home_obj
        except Exception as e:
            self._log_debug("Could not fetch Home fallback object: %s", e)

        return None

    def _sync_inventory_53216_once(
        self,
        hub_ip: str,
        net_id_int: int,
        mesh_net2_int: int,
        timeout: float = 5.0,
        force_ts: Optional[int] = None,
        force_nonce: Optional[int] = None,
        force_ea_b64: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run one-shot EA/EB exchange on port 53216 and return decrypted inventory JSON."""
        debug_mode = self._debug_enabled()
        ts_float = time.time()
        ts = int(force_ts) if force_ts is not None else int(ts_float)
        nonce = int(force_nonce) if force_nonce is not None else self._derive_sync_53216_nonce(ts, mesh_net2_int)
        key_hex = self._derive_sync_53216_key(ts, net_id_int).hex()
        if force_ea_b64:
            ea_payload_b64 = "".join(str(force_ea_b64).split())
            ea_wire = f"ea{len(ea_payload_b64):08x}{nonce:08x}{ea_payload_b64}".encode("ascii")
        else:
            ea_wire = self._build_sync_53216_ea(ts, net_id_int, nonce)
        ea_wire_ascii = ea_wire.decode("ascii", errors="replace")

        if debug_mode:
            debug_lines = [
                f"unix_seconds_float: {ts_float:.6f}",
                f"unix_seconds: {ts}",
                f"utc_time: {datetime.fromtimestamp(ts, timezone.utc).isoformat()}",
                f"nonce: 0x{nonce:08x}",
                "ts_source: forced" if force_ts is not None else "ts_source: current_time",
                "nonce_source: forced" if force_nonce is not None else "nonce_source: derived_from_ts",
                "ea_payload_source: forced" if force_ea_b64 else "ea_payload_source: generated",
                f"key_hex(ts): {key_hex}",
                f"EA bytes: {len(ea_wire)}",
                f"EA wire: {ea_wire_ascii}",
            ]
            self._log_multiline_debug("53216 EA request parameters", debug_lines)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((hub_ip, TCP_SYNC_PORT))
            send_ts_float = time.time()
            sock.sendall(ea_wire)
            recv_start_ts_float = time.time()
            eb_raw, eb_parts, eb_frame_infos = self._read_sync_53216_eb_frames(sock, timeout=timeout)
            recv_end_ts_float = time.time()

        send_ts = int(send_ts_float)
        recv_start_ts = int(recv_start_ts_float)
        recv_end_ts = int(recv_end_ts_float)
        if debug_mode:
            frame_lines = [
                f"send_time_float: {send_ts_float:.6f} (sec={send_ts})",
                f"recv_start_float: {recv_start_ts_float:.6f} (sec={recv_start_ts})",
                f"recv_end_float: {recv_end_ts_float:.6f} (sec={recv_end_ts})",
                f"ts_delta_send_minus_build: {send_ts - ts}",
                f"EB frame parts: {len(eb_parts)}",
                f"EB raw bytes: {len(eb_raw)}",
                f"EB raw ascii: {eb_raw.decode('ascii', errors='replace')}",
                f"EB raw hex: {eb_raw.hex()}",
            ]
            frame_lines.extend(
                "EB frame {idx}: marker={marker} len_hex={length_hex} len={payload_len} in_bounds={in_bounds}".format(
                    idx=i,
                    marker=info.get("marker"),
                    length_hex=info.get("length_hex"),
                    payload_len=info.get("payload_len"),
                    in_bounds=info.get("in_bounds"),
                )
                for i, info in enumerate(eb_frame_infos)
            )
            self._log_multiline_debug("53216 EB response trace", frame_lines)

        attempts: List[Tuple[str, str]] = []
        if len(eb_parts) > 1:
            attempts.append(("concat", "".join(eb_parts)))
        for i, part in enumerate(eb_parts):
            attempts.append((f"part[{i}]", part))

        if debug_mode:
            for label, payload_b64 in attempts:
                self._log_debug("53216 EB payload %s b64 (%s): %s", label, len(payload_b64), payload_b64)

        debug_bundle = {
            "hub_ip": hub_ip,
            "netid": net_id_int,
            "ts_build_float": ts_float,
            "ts_build_int": ts,
            "ts_send_float": send_ts_float,
            "ts_send_int": send_ts,
            "ts_recv_start_float": recv_start_ts_float,
            "ts_recv_start_int": recv_start_ts,
            "ts_recv_end_float": recv_end_ts_float,
            "ts_recv_end_int": recv_end_ts,
            "nonce_hex": f"0x{nonce:08x}",
            "force_ts": force_ts,
            "force_nonce": force_nonce,
            "force_ea_b64": force_ea_b64,
            "key_hex": key_hex,
            "ea_wire": ea_wire_ascii,
            "eb_raw_ascii": eb_raw.decode("ascii", errors="replace"),
            "eb_raw_hex": eb_raw.hex(),
            "eb_frame_infos": eb_frame_infos,
            "eb_payloads": [{"label": label, "b64": payload_b64} for label, payload_b64 in attempts],
        }

        if debug_mode:
            metadata_lines = [
                f"hub_ip: {hub_ip}",
                f"netid: {net_id_int}",
                f"key_hex: {key_hex}",
                f"ts_build: {ts}",
                f"ts_send: {send_ts}",
                f"ts_recv_start: {recv_start_ts}",
                f"ts_recv_end: {recv_end_ts}",
                f"nonce_hex: 0x{nonce:08x}",
                f"payload_sources: {', '.join(label for label, _ in attempts)}",
            ]
            self._log_multiline_debug("53216 decrypt attempt metadata", metadata_lines)

        last_err: Optional[Exception] = None
        for label, payload_b64 in attempts:
            try:
                data, mode = self._decrypt_sync_53216_eb_payload(ts, net_id_int, payload_b64)
                if self._is_valid_53216_inventory_payload(data):
                    if debug_mode:
                        self._log_debug("53216 decrypt succeeded with source=%s mode=%s", label, mode)
                    self._dump_structure_json("hub_53216_decrypted_root.json", data)
                    payload_obj = self._extract_53216_inventory_payload(data)
                    if payload_obj is not None:
                        self._dump_structure_json("hub_53216_inventory_payload.json", payload_obj)
                        self._dump_structure_json("hub_53216_deviceList.json", payload_obj.get("deviceList") or [])
                        self._dump_structure_json("hub_53216_groupList.json", payload_obj.get("groupList") or [])
                        self._dump_structure_json("hub_53216_sceneList.json", payload_obj.get("sceneList") or [])
                    if debug_mode:
                        with open("sync53216_debug_last.json", "w", encoding="utf-8") as fp:
                            json.dump(debug_bundle, fp, ensure_ascii=False, indent=2)
                        self._log_debug("Wrote 53216 debug bundle: sync53216_debug_last.json")
                    return {
                        "status": "success",
                        "hub_ip": hub_ip,
                        "netid": str(net_id_int),
                        "unix_seconds": ts,
                        "nonce_hex": f"0x{nonce:08x}",
                        "eb_source": label,
                        "decode_mode": mode,
                        "data": data,
                    }

                raise PixieAuthError(
                    "Decrypted payload does not match expected inventory schema "
                    f"(source={label}, mode={mode}, type={type(data).__name__}, value={data!r})"
                )
            except Exception as exc:
                last_err = exc

        candidate_ts: List[Tuple[str, int]] = []
        seen_ts: set[int] = set()

        def _add_ts(label: str, ts_val: int) -> None:
            if ts_val in seen_ts:
                return
            seen_ts.add(ts_val)
            candidate_ts.append((label, ts_val))

        _add_ts("build", ts)
        _add_ts("send", send_ts)
        _add_ts("recv_start", recv_start_ts)
        _add_ts("recv_end", recv_end_ts)
        for offset in (-2, -1, 1, 2):
            _add_ts(f"build_{offset:+d}", ts + offset)
        candidate_results: List[Dict[str, Any]] = []
        for ts_label, ts_val in candidate_ts:
            for label, payload_b64 in attempts:
                rec: Dict[str, Any] = {
                    "ts_label": ts_label,
                    "ts": ts_val,
                    "source": label,
                    "ok": False,
                }
                try:
                    data, mode = self._decrypt_sync_53216_eb_payload(ts_val, net_id_int, payload_b64)
                    rec["ok"] = True
                    rec["mode"] = mode
                    rec["is_inventory_shape"] = self._is_valid_53216_inventory_payload(data)
                    rec["data_type"] = type(data).__name__
                    if isinstance(data, dict):
                        rec["dict_keys"] = list(data.keys())[:8]
                except Exception as exc:
                    rec["error"] = str(exc)
                candidate_results.append(rec)

        debug_bundle["candidate_decryptions"] = candidate_results
        hit_count = sum(1 for x in candidate_results if x.get("ok"))
        if debug_mode:
            self._log_debug("53216 candidate decryptions: %s/%s successful JSON parses", hit_count, len(candidate_results))

            candidate_lines = []
            for rec in candidate_results:
                base = (
                    f"ts_label={rec.get('ts_label')} ts={rec.get('ts')} "
                    f"source={rec.get('source')} ok={rec.get('ok')}"
                )
                if rec.get("ok"):
                    extra = (
                        f" mode={rec.get('mode')} inventory_shape={rec.get('is_inventory_shape')}"
                        f" data_type={rec.get('data_type')}"
                    )
                    if rec.get("dict_keys"):
                        extra += f" dict_keys={rec.get('dict_keys')}"
                    candidate_lines.append(base + extra)
                else:
                    candidate_lines.append(base + f" error={rec.get('error')}")
            self._log_multiline_debug("53216 candidate decryption matrix", candidate_lines)

        if debug_mode:
            with open("sync53216_debug_last.json", "w", encoding="utf-8") as fp:
                json.dump(debug_bundle, fp, ensure_ascii=False, indent=2)
            self._log_debug("Wrote 53216 debug bundle: sync53216_debug_last.json")
        raise PixieAuthError(f"Failed to decrypt EB payload from 53216: {last_err}")

    def _find_meshnet_record(self, api_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find the best record in LiveGroup response with meshNet/meshNet2/netID."""

        def _rec_search(item):
            if isinstance(item, dict):
                if item.get('meshNet') is not None or item.get('meshNet2') is not None or item.get('netID') is not None:
                    return item
                if item.get('MeshNet') is not None or item.get('MeshNet2') is not None:
                    return item
                # Search nested objects and arrays
                for k, v in item.items():
                    found = _rec_search(v)
                    if found is not None:
                        return found
            elif isinstance(item, list):
                for subitem in item:
                    found = _rec_search(subitem)
                    if found is not None:
                        return found
            elif isinstance(item, str):
                try:
                    parsed = json.loads(item)
                    found = _rec_search(parsed)
                    if found is not None:
                        return found
                except json.JSONDecodeError:
                    pass
                # Try to decode as hex envelope
                try:
                    import binascii
                    bytes_data = binascii.unhexlify(item)
                    decoded = PixieEnvelope.decode(bytes_data, None)  # No key for search
                    if decoded:
                        found = _rec_search(decoded)
                        if found is not None:
                            return found
                except:
                    pass
            return None

        # Top-level search
        found = _rec_search(api_data)
        if found is not None:
            return found

        # If this is not enough, inspect results as fallback
        results = api_data.get('results')
        if isinstance(results, list):
            for index, entry in enumerate(results):
                if not isinstance(entry, dict):
                    continue
                found = _rec_search(entry)
                if found is not None:
                    return found

        return None


    def _fetch_login_data(self, username: str, password: str, include_inventory_seed: bool = True) -> Dict[str, Any]:
        """
        Fetch netID, meshNet from Pixie Plus cloud API.
        
        Based on pixiepluslogin.py login() and livegroup_get_objectID() functions.
        """
        self._log_debug("Calling cloud API endpoints: %s and %s", API_URL['login'], API_URL['livegroup'])

        # Default values
        netid = self.netid_seed or "unknown"
        meshnet = "unknown"
        meshnet2 = "unknown"  # Initialize meshnet2 to prevent NameError
        homeid = "unknown"
        home_name = "unknown"
        userid = "unknown"
        sessiontoken = "unknown"

        try:
            import httpx

            # Call login API
            headers = {
                "x-parse-application-id": APPLICATION_ID,
                "x-parse-installation-id": "cli-installation",
                "x-parse-client-key": CLIENT_KEY,
                "x-parse-revocable-session": "1",
            }

            body = {"username": username, "password": password}

            response = httpx.post(API_URL["login"], json=body, headers=headers)

            if response.status_code == 403:
                self._log_warning("Cloud login failed: invalid credentials (403 Unauthorized)")
                return {
                    'netid': netid,
                    'meshnet': meshnet,
                    'homeid': homeid,
                    'userid': userid,
                    'sessiontoken': sessiontoken
                }

            response.raise_for_status()
            data = response.json()

            userid = data.get('objectId', userid)
            home_info = data.get('curHome', {})
            homeid = home_info.get('objectId', homeid)
            home_name = home_info.get('name', home_name)
            sessiontoken = data.get('sessionToken', sessiontoken)

            # Assume meshNet is the homeid for discovery
            meshnet = homeid

            self._log_debug(
                "Cloud login succeeded: user=%s home=%s sessionToken=%s meshNet=%s",
                userid,
                homeid,
                '***' if sessiontoken else None,
                meshnet,
            )

            # Try to get meshNet from LiveGroup
            try:
                headers = {
                    "x-parse-session-token": sessiontoken,
                    "x-parse-application-id": APPLICATION_ID,
                    "x-parse-client-key": CLIENT_KEY,
                }

                body = {
                    "where": json.dumps({"GroupID": {"$regex": homeid + "$", "$options": "i"}}),
                    "limit": 2,
                }

                response = httpx.get(API_URL["livegroup"], params=body, headers=headers)
                response.raise_for_status()
                data = response.json()

                record = self._find_meshnet_record(data)
                if record is not None:
                    meshnet_value = record.get('meshNet') or record.get('MeshNet')
                    meshnet2_value = record.get('meshNet2') or record.get('MeshNet2')

                    if meshnet_value:
                        meshnet = str(meshnet_value)
                        self._log_debug("Updated meshNet from LiveGroup: %s", meshnet)
                    if meshnet2_value:
                        meshnet2 = str(meshnet2_value)
                        self._log_debug("Updated meshNet2 from LiveGroup: %s", meshnet2)

            except Exception as e:
                self._log_debug("Could not fetch mesh data from LiveGroup: %s", e)

            # Try to get meshNet/netID from Home API. Inventory build is optional.
            try:
                home_obj = self._fetch_home_object(homeid=str(homeid), sessiontoken=str(sessiontoken))
                if home_obj:
                    self._cached_cloud_home_obj = dict(home_obj)
                    if include_inventory_seed:
                        try:
                            self._set_inventory_from_home_object(home_obj, str(userid), source="cloud_seed")
                        except Exception as inv_err:
                            self._log_debug("Could not build inventory from Home payload: %s", inv_err)

                    meshnet_home = home_obj.get('meshNet')
                    meshnet2_home = home_obj.get('meshNet2')
                    netid_home = home_obj.get('netID')
                    home_name_value = home_obj.get('name')
                    if meshnet_home:
                        meshnet = str(meshnet_home)
                        self._log_debug("Got meshNet from Home API: %s", meshnet)
                    if meshnet2_home:
                        meshnet2 = str(meshnet2_home)
                        self._log_debug("Got meshNet2 from Home API: %s", meshnet2)
                    if netid_home:
                        netid = str(netid_home)
                        self._log_debug("Got netID from Home API: %s", netid)
                    if home_name_value:
                        home_name = str(home_name_value)
                        self._log_debug("Got home name from Home API: %s", home_name)

            except Exception as e:
                self._log_debug("Could not fetch metadata from Home API: %s", e)

        except httpx.HTTPStatusError as e:
            self._log_warning("Cloud API HTTP error: %s - %s", e.response.status_code, e.response.text[:100])
        except Exception as e:
            self._log_warning("Could not fetch login data from cloud API: %s", e)

        return {
            'netid': netid,
            'meshnet': meshnet,
            'meshnet2': meshnet2,
            'homeid': homeid,
            'home_name': home_name,
            'userid': userid,
            'sessiontoken': sessiontoken
        }

    def _perform_handshake_capture(
        self,
        hub_ip: str,
        hub_port: int,
        runtime_session: Optional[PixieRuntimeSession] = None,
        control_ready_event: Optional[threading.Event] = None,
        control_ready_state: Optional[Dict[str, Any]] = None,
        stop_event: Optional[threading.Event] = None,
        keep_control_alive: bool = True,
        command_request_queue: Optional["queue.Queue[Dict[str, Any]]"] = None,
        *,
        command_device_id: Optional[int] = None,
        command_state: Optional[bool] = None,
        command_brightness: Optional[int] = None,
        command_color_rgb: Optional[Tuple[int, int, int]] = None,
        command_white: bool = False,
        command_effect: Optional[str] = None,
        command_target: Optional[str] = None,
        command_cover_action: Optional[str] = None,
        command_cover_action_map: Optional[Dict[str, int]] = None,
        command_cover_tilt_action_map: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform full client-mode TCP handshake sequence (matches Java app flow).

        Args:
            hub_ip: Target hub IP address
            hub_port: Hub TCP control port
            command_device_id: Optional device ID to command after handshake
            command_state: Optional target on/off state for local command
            command_brightness: Optional brightness 0-100 for brightness command
            command_color_rgb: Optional RGB tuple for color command
            command_white: Whether color command was requested via --white
            command_effect: Optional effect name command
            command_target: Optional target endpoint for on/off command
            command_cover_action: Optional cover action command

        Returns:
            Auth result dict if successful
        """

        from .pixie_protocol import (
            PixieMessage,
            PixieEnvelope,
            FLAG_DUAL_DATA,
            FLAG_EACK,
            FLAG_HEARTBEAT,
            FLAG_SINGLE_DATA,
        )

        self._log_debug("Connecting as client to %s:%s", hub_ip, hub_port)

        # Create TCP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)

        try:
            sock.connect((hub_ip, hub_port))
            self._log_debug("TCP connection established")
            self.current_hub = {"host": hub_ip, "port": hub_port}
        except Exception as e:
            self._log_warning("TCP connection failed: %s", e)
            return None

        extracted_key = None
        incoming_queue: "queue.Queue[Tuple[float, bytes]]" = queue.Queue()
        reader_stop = threading.Event()
        reader_thread: Optional[threading.Thread] = None
        connection_closed = False
        should_stop = stop_event or threading.Event()
        pending_requests = deque()
        readiness = {
            "saw_bledata": False,
            "saw_bulk_bledata": False,
            "saw_eack_reply": False,
            "sent_heartbeat": False,
            "saw_heartbeat_reply": False,
            "ready_signaled": False,
        }

        if control_ready_state is not None:
            control_ready_state.update(readiness)

        def _update_ready_state(**kwargs: Any) -> None:
            changed = False
            for key, value in kwargs.items():
                if key in readiness and readiness[key] != value:
                    readiness[key] = value
                    changed = True
            if control_ready_state is not None and changed:
                control_ready_state.update(readiness)

        def _maybe_signal_ready() -> None:
            if readiness["ready_signaled"]:
                return
            if (
                readiness["saw_bledata"]
                and readiness["saw_eack_reply"]
                and readiness["sent_heartbeat"]
                and readiness["saw_heartbeat_reply"]
            ):
                _update_ready_state(ready_signaled=True)
                if runtime_session is not None:
                    runtime_session.mark_primed()
                if control_ready_event is not None:
                    control_ready_event.set()
                self._log_debug("41578 control primed: bleData + eack_reply + first heartbeat roundtrip")

        def _parse_message(raw_b64: str, key: Optional[str]) -> Dict[str, Any]:
            """Decode base64, parse envelope flag, and decrypt JSON when possible."""
            parsed: Dict[str, Any] = {
                "raw_b64": raw_b64,
                "flag": None,
                "envelope": None,
                "plaintext": None,
                "json": None,
                "error": None,
            }

            try:
                envelope_bytes = PixieEnvelope.from_base64(raw_b64)
                envelope = PixieEnvelope.decode(envelope_bytes)
                parsed["envelope"] = envelope
                if not envelope:
                    parsed["error"] = "invalid envelope"
                    return parsed

                parsed["flag"] = envelope.get("flag1")
                if key and parsed["flag"] != FLAG_DUAL_DATA:
                    plaintext = PixieEnvelope.decrypt_envelope(envelope, key)
                    parsed["plaintext"] = plaintext
                    if plaintext:
                        try:
                            parsed["json"] = json.loads(plaintext)
                        except Exception:
                            parsed["json"] = None
                return parsed
            except Exception as exc:
                parsed["error"] = str(exc)
                return parsed

        def _classify_message(direction: str, parsed: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
            """Classify a transport frame by envelope flag, content, and context."""
            flag = parsed.get("flag")
            payload = parsed.get("json") or {}
            op = payload.get("op") if isinstance(payload, dict) else None
            code = payload.get("code") if isinstance(payload, dict) else None

            matched_request: Optional[Dict[str, Any]] = None

            if flag == FLAG_DUAL_DATA:
                return "session_init", None

            if direction == "out":
                if flag == FLAG_HEARTBEAT and op == "ack" and code == 0:
                    return "heartbeat", None
                if flag == FLAG_EACK and op == "ack" and code == 0:
                    return "eack", None
                if flag == FLAG_SINGLE_DATA:
                    return "command", None
                return f"out_flag_{flag}", None

            if flag == FLAG_EACK and op == "ack" and code == 0:
                if pending_requests:
                    matched_request = pending_requests.popleft()
                    if matched_request["kind"] == "heartbeat":
                        return "heartbeat_reply", matched_request
                    if matched_request["kind"] == "eack":
                        return "eack_reply", matched_request
                return "ack", None

            if flag == FLAG_HEARTBEAT and op == "ack" and code == 0:
                if pending_requests:
                    matched_request = pending_requests.popleft()
                    if matched_request["kind"] == "heartbeat":
                        return "heartbeat_reply", matched_request
                return "heartbeat_push", None

            if flag == FLAG_SINGLE_DATA:
                if op == "ack" and code == 0:
                    return "encrypted_ack", None
                if parsed.get("json") is not None:
                    return "device_or_control_update", None
                if parsed.get("plaintext"):
                    return "encrypted_payload", None

            return f"in_flag_{flag}", None

        def _decode_ble_data(hex_payload: str) -> Optional[Dict[str, Any]]:
            """Decode bleData payloads into normalized single or bulk records."""
            if not isinstance(hex_payload, str):
                return None

            clean = hex_payload.strip().lower()
            if not clean or len(clean) % 2 != 0:
                return None

            try:
                raw = bytes.fromhex(clean)
            except Exception:
                return None

            decoded: Dict[str, Any] = {
                "hex": clean,
                "length": len(raw),
                "bytes": [int(b) for b in raw],
            }

            # GwData bulk snapshot:
            # 6-byte header + repeated 4-byte records (id, online, br, rssi_enc).
            if len(raw) >= 18 and (len(raw) - 6) % 4 == 0:
                records: List[Dict[str, Any]] = []
                for i in range(6, len(raw) - 3, 4):
                    dev_id = int(raw[i])
                    online = int(raw[i + 1])
                    br_raw = int(raw[i + 2])
                    if dev_id in (0, 255):
                        continue
                    records.append({
                        "id": dev_id,
                        "online": online,
                        "br_raw": br_raw,
                        "br": self._decode_bulk_br(br_raw),
                    })

                if len(records) >= 2:
                    decoded["kind"] = "bulk"
                    decoded["records"] = records
                    return decoded

            # Command-style c16969 payloads can carry RGB + brightness directly.
            # Format observed on local command path:
            # [seq3][src_le=0304][dst_le][00][c1][69][69][R][G][B][brightness]
            if len(raw) >= 14 and raw[7] == 0xC1 and raw[8] == 0x69 and raw[9] == 0x69:
                decoded["kind"] = "single"
                decoded["opcode"] = "c16969"
                decoded["device_id"] = int(raw[5])
                decoded["rgb"] = [int(raw[10]), int(raw[11]), int(raw[12])]
                brightness_raw = int(raw[13])
                decoded["brightness_raw"] = brightness_raw
                decoded["brightness_0_100"] = max(0, min(100, round((brightness_raw * 100) / 256)))
                decoded["records"] = [{
                    "id": decoded["device_id"],
                    "online": None,
                    "br_raw": brightness_raw,
                    "br": {"type": "single", "raw": brightness_raw, "pct": decoded["brightness_0_100"]},
                    "rgb": list(decoded["rgb"]),
                    "value_byte": None,
                    "tail_flag": None,
                    "is_on_from_tail": None,
                    "sequence_or_counter": None,
                }]
                return decoded

            # Observed switch/light BLE payload shape currently appears to be:
            # [.. .. .. .. .. .. .. .. .. .. dev_id seq level tail]
            # with examples like:
            # 641b1000000000dc1102569b0080 (off)
            # 641b1000000000dc110256e36490 (on)
            if len(raw) >= 11:
                decoded["device_id"] = int(raw[10])
            if len(raw) >= 12:
                decoded["sequence_or_counter"] = int(raw[11])
            if len(raw) >= 13:
                decoded["value_byte"] = int(raw[12])
                decoded["level"] = decoded["value_byte"]
            if len(raw) >= 14:
                tail = int(raw[13])
                decoded["tail_flag"] = tail
                decoded["is_on_from_tail"] = bool(tail & 0x10)

            if isinstance(decoded.get("device_id"), int):
                value_byte = decoded.get("value_byte")
                record_br: Dict[str, Any]
                if isinstance(value_byte, int):
                    record_br = self._decode_bulk_br(value_byte)
                else:
                    record_br = {"type": "single", "raw": None, "pct": None}
                decoded["kind"] = "single"
                decoded["records"] = [{
                    "id": decoded["device_id"],
                    "online": None,
                    "br_raw": value_byte if isinstance(value_byte, int) else None,
                    "br": record_br,
                    "rgb": decoded.get("rgb") if isinstance(decoded.get("rgb"), list) else None,
                    "value_byte": value_byte,
                    "tail_flag": decoded.get("tail_flag"),
                    "is_on_from_tail": decoded.get("is_on_from_tail"),
                    "sequence_or_counter": decoded.get("sequence_or_counter"),
                }]

            return decoded

        def _apply_flag1_update(parsed: Dict[str, Any]) -> None:
            """Apply known flag=1 bleData updates into inventory runtime state."""
            payload = parsed.get("json")
            if not isinstance(payload, dict):
                return

            if payload.get("type") != "bleData":
                return

            ble_hex = payload.get("data")
            decoded = _decode_ble_data(ble_hex)
            if not decoded:
                self._log_debug("BLE decode: unable to parse data field")
                return

            kind = decoded.get("kind")
            records = decoded.get("records") if isinstance(decoded.get("records"), list) else []

            if kind == "bulk":
                self._log_debug("BLE decode (bulk): records=%s", len(records))
                full_snapshot = self._awaiting_initial_gwdata_bulk
                if full_snapshot:
                    self._awaiting_initial_gwdata_bulk = False
                _update_ready_state(saw_bulk_bledata=True)
                self._queue_bulk_ble_records(records, source="hub_gwdata", full_snapshot=full_snapshot)
                if self.inventory:
                    applied = self._apply_bulk_ble_records_to_inventory(
                        records,
                        source="hub_gwdata",
                        full_snapshot=full_snapshot,
                    )
                    self._log_debug("Inventory bulk update: applied=%s", applied)
                return

            if not records:
                return

            if not self.inventory:
                return

            first = records[0]
            dev_id = first.get("id")
            if not isinstance(dev_id, int):
                return

            rec = self.inventory.devices_by_id.get(dev_id)
            if not rec:
                self._log_debug("Inventory: unknown dev_id=%s (not in inventory)", dev_id)
                return

            value_byte = first.get("value_byte")
            tail = first.get("tail_flag")
            on_tail = first.get("is_on_from_tail")
            self._log_debug(
                "BLE decode: dev_id=%s seq=%s value=0x%02x tail=0x%02x on_tail=%s",
                dev_id,
                first.get("sequence_or_counter"),
                value_byte if isinstance(value_byte, int) else 0,
                tail if isinstance(tail, int) else 0,
                on_tail,
            )

            prev_br = rec.runtime.br
            prev_rgb = rec.runtime.rgb
            prev_r = rec.runtime.r

            interpreted = None
            rgb_from_packet = first.get("rgb") if isinstance(first.get("rgb"), list) else None
            br_from_packet = decoded.get("brightness_0_100") if isinstance(decoded.get("brightness_0_100"), int) else None
            update_kwargs: Dict[str, Any] = {
                "online": 1,
                "presence": "online",
            }

            if rgb_from_packet is not None:
                update_kwargs["rgb"] = [int(rgb_from_packet[0]), int(rgb_from_packet[1]), int(rgb_from_packet[2])]
                if br_from_packet is not None:
                    update_kwargs["br"] = br_from_packet

            if isinstance(value_byte, int):
                interpreted = decode_value_byte(rec.model_no, value_byte)
                self._log_debug(
                    "BLE interpreted: model=%s mode=%s data=%s",
                    rec.model_no,
                    interpreted.get("mode"),
                    json.dumps(interpreted, ensure_ascii=False, sort_keys=True),
                )

                mode = interpreted.get("mode")

                if mode == "brightness":
                    update_kwargs["br"] = interpreted.get("brightness_0_100")
                elif mode == "dual_channel":
                    left_on = bool(interpreted.get("left_on"))
                    right_on = bool(interpreted.get("right_on"))
                    if left_on and right_on:
                        update_kwargs["r"] = 3
                    elif left_on:
                        update_kwargs["r"] = 1
                    elif right_on:
                        update_kwargs["r"] = 2
                    else:
                        update_kwargs["r"] = 0
                elif mode == "plug_with_usb":
                    relay_on = bool(interpreted.get("main_relay_on"))
                    usb_on = bool(interpreted.get("usb_on"))
                    update_kwargs["r"] = (1 if relay_on else 0) | (2 if usb_on else 0)
                    update_kwargs["br"] = 100 if relay_on else 0
                elif mode == "raw":
                    # Raw on/off-only models use the value byte directly.
                    # Keep tail-derived fields for debugging only until their
                    # protocol meaning is understood.
                    if (
                        rec.capabilities.supports_onoff
                        and not rec.capabilities.supports_dimming
                        and not rec.capabilities.supports_multi_channel
                        and not rec.capabilities.supports_usb_subentity
                        and not rec.capabilities.supports_cover
                        and isinstance(value_byte, int)
                    ):
                        update_kwargs["br"] = 100 if value_byte > 0 else 0

            update_kwargs["raw"] = {
                "hub_type": payload.get("type"),
                "hub_data": ble_hex,
                "hub_utc": payload.get("UTC"),
                "ble_decoded": decoded,
                "ble_interpreted": interpreted,
            }
            updated_runtime = self.inventory.apply_device_update(
                dev_id,
                source="hub_update",
                **update_kwargs,
            )
            if updated_runtime is None:
                return

            self._notify_inventory_updated()

            summary_parts = []
            if rgb_from_packet is not None:
                summary_parts.append(f"rgb {prev_rgb}->{updated_runtime.rgb}")
                if br_from_packet is not None:
                    summary_parts.append(f"br {prev_br}->{updated_runtime.br}")
            elif interpreted and interpreted.get("mode") == "brightness":
                summary_parts.append(f"br {prev_br}->{updated_runtime.br}")
            elif interpreted and interpreted.get("mode") == "dual_channel":
                summary_parts.append(f"r {prev_r}->{updated_runtime.r}")
                summary_parts.append(f"channel={interpreted.get('channel_state')}")
            elif interpreted and interpreted.get("mode") == "raw" and isinstance(on_tail, bool):
                summary_parts.append(f"on_tail={on_tail}")
                summary_parts.append(f"br {prev_br}->{updated_runtime.br}")
            else:
                summary_parts.append(f"value={value_byte}")

            self._log_debug(
                "Inventory update: id=%s name=%s %s src=hub_update",
                rec.id,
                rec.name,
                ", ".join(summary_parts),
            )

        def _apply_local_command_optimistic_update(
            device_id: int,
            value: Any,
            command_hex: str,
            *,
            target: str,
            opcode_name: str,
            brightness_level: Optional[int] = None,
            rgb_color: Optional[Tuple[int, int, int]] = None,
            effect_name: Optional[str] = None,
            effect_speed: Optional[int] = None,
            cover_button_position: Optional[int] = None,
        ) -> None:
            """Apply an optimistic state update immediately after local command send.

            Args:
                device_id: Device to update
                value: bool for on/off commands, int (0-100) for brightness
                command_hex: Hex string of command sent
                target: Command target (relay, usb, left, right, both, brightness)
                opcode_name: Opcode name for logging

            Hub-originated bleData updates remain authoritative and will overwrite
            this optimistic snapshot via _apply_flag1_update.
            """
            if not self.inventory:
                return

            rec = self.inventory.devices_by_id.get(int(device_id))
            if not rec:
                self._log_debug("Inventory optimistic update skipped: unknown dev_id=%s", device_id)
                return

            prev_br = rec.runtime.br
            prev_rgb = rec.runtime.rgb
            prev_effect = rec.runtime.effect
            prev_effect_speed = rec.runtime.effect_speed
            prev_r = rec.runtime.r
            prev_source = rec.runtime.last_source
            update_kwargs: Dict[str, Any] = {}

            # Handle brightness/color/effect commands
            if target == "brightness":
                if isinstance(value, int):
                    update_kwargs["br"] = value
            elif target == "color":
                if isinstance(brightness_level, int):
                    update_kwargs["br"] = brightness_level
                if rgb_color is not None:
                    update_kwargs["rgb"] = [int(rgb_color[0]), int(rgb_color[1]), int(rgb_color[2])]
            elif target == "effect":
                if isinstance(brightness_level, int):
                    update_kwargs["br"] = brightness_level
                update_kwargs["effect"] = effect_name
                update_kwargs["effect_speed"] = effect_speed
            elif target == "speed":
                if isinstance(brightness_level, int):
                    update_kwargs["br"] = brightness_level
                update_kwargs["effect"] = effect_name
                update_kwargs["effect_speed"] = effect_speed
            elif target == "cover":
                # Cover commands are button presses, not authoritative state updates.
                pass
            elif rec.model_no == "2213":
                update_kwargs["br"] = 100 if value else 0
            elif rec.model_no == "0107":
                # 0107 fallback can start cloud-only with br and no USB detail.
                if isinstance(rec.runtime.r, int):
                    current_relay_on = bool(rec.runtime.r & 0x01)
                    current_usb_on = bool(rec.runtime.r & 0x02)
                elif isinstance(rec.runtime.br, int):
                    current_relay_on = rec.runtime.br > 0
                    current_usb_on = False
                else:
                    current_relay_on = False
                    current_usb_on = False

                if target == "usb":
                    next_relay_on = current_relay_on
                    next_usb_on = value
                else:
                    next_relay_on = value
                    next_usb_on = current_usb_on

                update_kwargs["r"] = (1 if next_relay_on else 0) | (2 if next_usb_on else 0)
                update_kwargs["br"] = 100 if next_relay_on else 0
            else:
                update_kwargs["br"] = 100 if value else 0

            if target in ("left", "right", "both"):
                if target == "left":
                    update_kwargs["r"] = 1 if value else 0
                elif target == "right":
                    update_kwargs["r"] = 2 if value else 0
                else:
                    update_kwargs["r"] = 3 if value else 0

            update_kwargs["raw"] = {
                "local_command": {
                    "opcode": opcode_name,
                    "device_id": int(device_id),
                    "target": target,
                    "requested_state": f"{value}" if target in ("brightness", "color", "effect", "speed", "cover") else ("on" if value else "off"),
                    "command_hex": command_hex,
                    "brightness_level": brightness_level,
                    "rgb_color": list(rgb_color) if rgb_color is not None else None,
                    "effect_name": effect_name,
                    "effect_speed": effect_speed,
                    "cover_button_position": cover_button_position,
                    "pending_verification": True,
                }
            }
            updated_runtime = self.inventory.apply_device_update(
                device_id,
                source="local_command_optimistic",
                **update_kwargs,
            )
            if updated_runtime is None:
                return

            self._log_debug(
                "Inventory optimistic update: id=%s name=%s br %s->%s rgb %s->%s effect %s->%s speed %s->%s r %s->%s src %s->%s",
                rec.id,
                rec.name,
                prev_br,
                updated_runtime.br,
                prev_rgb,
                updated_runtime.rgb,
                prev_effect,
                updated_runtime.effect,
                prev_effect_speed,
                updated_runtime.effect_speed,
                prev_r,
                updated_runtime.r,
                prev_source,
                updated_runtime.last_source,
            )

        def _default_brightness_percent(rec: Optional[Any]) -> int:
            if rec and isinstance(rec.runtime.br, int):
                return max(0, min(100, int(rec.runtime.br)))
            return 100

        def _default_effect_name(rec: Optional[Any]) -> Optional[str]:
            if rec and isinstance(rec.runtime.effect, str):
                normalized = rec.runtime.effect.strip().lower()
                if normalized:
                    return normalized
            return None

        def _default_effect_speed(rec: Optional[Any]) -> int:
            return 0x04

        def _send_requested_local_command(
            *,
            command_device_id: int,
            command_state: Optional[bool] = None,
            command_brightness: Optional[int] = None,
            command_color_rgb: Optional[Tuple[int, int, int]] = None,
            command_effect: Optional[str] = None,
            command_target: Optional[str] = None,
            command_cover_action: Optional[str] = None,
            command_cover_action_map: Optional[Dict[str, int]] = None,
            command_cover_tilt_action_map: Optional[Dict[str, int]] = None,
        ) -> Dict[str, Any]:
            """Send one local command on the already-authenticated TCP socket."""
            if not readiness["ready_signaled"]:
                self._log_debug("Waiting for 41578 control priming before local command send")
                prime_deadline = time.time() + 5.0
                heartbeat_attempt = 0
                while time.time() < prime_deadline and not connection_closed and not should_stop.is_set() and not readiness["ready_signaled"]:
                    if _drain_incoming() > 0 and readiness["ready_signaled"]:
                        break
                    heartbeat_attempt += 1
                    got_traffic = _send_heartbeat_frame(f"HEARTBEAT PRIME #{heartbeat_attempt}")
                    if readiness["ready_signaled"]:
                        break
                    if not got_traffic:
                        self._log_debug(
                            "No incoming TCP traffic in priming heartbeat #%s response window",
                            heartbeat_attempt,
                        )
                    if not readiness["ready_signaled"]:
                        time.sleep(0.2)

            if not readiness["ready_signaled"]:
                raise PixieAuthError(f"41578 control channel not primed (state={readiness})")

            sender_identity = self.stored_username
            if not sender_identity and self.user_id not in (None, "", "unknown"):
                sender_identity = str(self.user_id)
            if not sender_identity:
                raise PixieAuthError("No sender identity available for local command")

            is_cover_cmd = command_cover_action is not None
            is_effect_cmd = command_effect is not None
            is_color_cmd = command_color_rgb is not None
            is_brightness_cmd = (command_brightness is not None) and not is_color_cmd and not is_effect_cmd and not is_cover_cmd
            state_byte_used = None

            rec = None
            if self.inventory:
                rec = self.inventory.devices_by_id.get(int(command_device_id))

            if is_color_cmd and rec and not rec.capabilities.supports_color:
                raise PixieAuthError(f"Model {rec.model_no} does not support color")

            if is_cover_cmd and rec and not rec.capabilities.supports_cover:
                raise PixieAuthError(f"Model {rec.model_no} does not support cover commands")

            if is_effect_cmd and rec:
                allowed_effects = rec.capabilities.effect_names or get_model_effect_names(rec.model_no)
                if not allowed_effects:
                    raise PixieAuthError(f"Model {rec.model_no} does not support effects")
                if command_effect.strip().lower() not in allowed_effects:
                    raise PixieAuthError(f"Effect '{command_effect}' not allowed for model {rec.model_no}: {allowed_effects}")

            if is_effect_cmd:
                effect_name = command_effect.strip().lower()
                effect_speed = _default_effect_speed(rec)
                effect_brightness = _default_brightness_percent(rec)
            else:
                effect_name = None
                effect_speed = None
                effect_brightness = None

            if is_cover_cmd:
                normalized_cover_action = command_cover_action.strip().lower().replace("-", "_")
                cover_button_position = resolve_cover_command_position(
                    normalized_cover_action,
                    action_mapping=command_cover_action_map,
                    tilt_mapping=command_cover_tilt_action_map,
                )
                if cover_button_position is None:
                    raise PixieAuthError(
                        f"No manual button mapping configured for cover action '{normalized_cover_action}'"
                    )
                command_hex = self._build_cover_press_command_hex(
                    command_device_id,
                    button_position=cover_button_position,
                )
                command_debug = self._build_local_bledata_command_debug(
                    key=extracted_key,
                    command_hex=command_hex,
                    from_email=sender_identity,
                )
                command_b64 = command_debug["base64"]
                self._log_debug(
                    "Sending local cover command: device_id=%s action=%s button_position=%s opcode=c16969",
                    command_device_id,
                    normalized_cover_action,
                    cover_button_position,
                )
                if self.verbose:
                    self._log_debug("Cover command hex: %s", command_hex)
                    self._print_local_command_debug(command_debug)
            elif is_effect_cmd:
                command_hex = self._build_effect_command_hex(
                    command_device_id,
                    effect_name=effect_name,
                    effect_speed=effect_speed,
                    brightness_level=effect_brightness,
                )
                command_debug = self._build_local_bledata_command_debug(
                    key=extracted_key,
                    command_hex=command_hex,
                    from_email=sender_identity,
                )
                command_b64 = command_debug["base64"]
                self._log_debug(
                    "Sending effect command: device_id=%s effect=%s speed=0x%02x brightness=%s opcode=f86969",
                    command_device_id,
                    effect_name or "none",
                    effect_speed,
                    effect_brightness,
                )
                if self.verbose:
                    self._log_debug("Effect command hex: %s", command_hex)
                    self._print_local_command_debug(command_debug)
            elif is_color_cmd:
                if command_brightness is not None:
                    color_brightness = max(0, min(100, int(command_brightness)))
                else:
                    color_brightness = _default_brightness_percent(rec)
                if color_brightness == 0:
                    color_brightness = 100

                command_hex = self._build_color_command_hex(
                    command_device_id,
                    rgb=command_color_rgb,
                    brightness_level=color_brightness,
                )
                command_debug = self._build_local_bledata_command_debug(
                    key=extracted_key,
                    command_hex=command_hex,
                    from_email=sender_identity,
                )
                command_b64 = command_debug["base64"]
                self._log_debug(
                    "Sending color command: device_id=%s rgb=%s brightness=%s opcode=c16969",
                    command_device_id,
                    command_color_rgb,
                    color_brightness,
                )
                if self.verbose:
                    self._log_debug("Color command hex: %s", command_hex)
                    self._print_local_command_debug(command_debug)
            elif is_brightness_cmd:
                command_hex = self._build_brightness_command_hex(
                    command_device_id,
                    brightness_level=command_brightness,
                )
                command_debug = self._build_local_bledata_command_debug(
                    key=extracted_key,
                    command_hex=command_hex,
                    from_email=sender_identity,
                )
                command_b64 = command_debug["base64"]
                self._log_debug(
                    "Sending brightness command: device_id=%s brightness=%s opcode=e76969",
                    command_device_id,
                    command_brightness,
                )
                if self.verbose:
                    self._log_debug("Brightness command hex: %s", command_hex)
                    self._print_local_command_debug(command_debug)
            else:
                effective_target = self._resolve_command_target_for_device(
                    command_device_id,
                    command_target,
                )
                command_spec = self._resolve_onoff_command_spec(effective_target)
                if effective_target == "usb":
                    command_hex, state_byte_used = self._build_0107_usb_command_hex(
                        command_device_id,
                        is_on=command_state,
                    )
                else:
                    command_hex = self._build_6969_onoff_command_hex(
                        command_device_id,
                        is_on=command_state,
                        opcode=command_spec["opcode"],
                        selector=command_spec["selector"],
                    )
                command_debug = self._build_local_bledata_command_debug(
                    key=extracted_key,
                    command_hex=command_hex,
                    from_email=sender_identity,
                )
                command_b64 = command_debug["base64"]
                self._log_debug(
                    "Sending local on/off command: device_id=%s target=%s state=%s opcode=%s selector=%s",
                    command_device_id,
                    command_spec["label"],
                    "on" if command_state else "off",
                    command_spec["opcode_name"],
                    command_spec["selector"],
                )
                if state_byte_used is not None:
                    self._log_debug("On/off command state byte: 0x%02x", state_byte_used)
                if self.verbose:
                    self._log_debug("On/off command hex: %s", command_hex)
                    self._print_local_command_debug(command_debug)

            command_parsed = _parse_message(command_b64, extracted_key)
            command_route, command_match = _classify_message("out", command_parsed)
            _log_message("out", command_parsed, command_route, command_match)
            sock.sendall(command_b64.encode("utf-8"))
            if runtime_session is not None:
                runtime_session.mark_command_sent()

            _drain_incoming()

            if is_cover_cmd:
                _apply_local_command_optimistic_update(
                    command_device_id,
                    normalized_cover_action,
                    command_hex,
                    target="cover",
                    opcode_name="c16969",
                    cover_button_position=cover_button_position,
                )
                return {"target": "cover", "device_id": command_device_id}
            if is_brightness_cmd:
                _apply_local_command_optimistic_update(
                    command_device_id,
                    command_brightness,
                    command_hex,
                    target="brightness",
                    opcode_name="e76969",
                )
                return {"target": "brightness", "device_id": command_device_id}
            if is_color_cmd:
                _apply_local_command_optimistic_update(
                    command_device_id,
                    command_color_rgb,
                    command_hex,
                    target="color",
                    opcode_name="c16969",
                    brightness_level=color_brightness,
                    rgb_color=command_color_rgb,
                )
                return {"target": "color", "device_id": command_device_id}
            if is_effect_cmd:
                _apply_local_command_optimistic_update(
                    command_device_id,
                    effect_name,
                    command_hex,
                    target="effect",
                    opcode_name="f86969",
                    brightness_level=effect_brightness,
                    effect_name=effect_name,
                    effect_speed=effect_speed,
                )
                return {"target": "effect", "device_id": command_device_id}

            _apply_local_command_optimistic_update(
                command_device_id,
                command_state,
                command_hex,
                target=effective_target,
                opcode_name=command_spec["opcode_name"],
            )
            return {"target": effective_target, "device_id": command_device_id}

        def _log_message(
            direction: str,
            parsed: Dict[str, Any],
            route: str,
            matched_request: Optional[Dict[str, Any]],
            *,
            byte_len: Optional[int] = None,
        ) -> None:
            """Log routed message details in a way that links requests and replies."""
            if self.suppress_heartbeat_logs and route in {"heartbeat", "heartbeat_reply", "heartbeat_push", "ack"}:
                return

            prefix = "OUT" if direction == "out" else "IN"
            size_note = f" ({byte_len} bytes)" if byte_len is not None else ""
            lines = [
                f"Raw base64: {parsed['raw_b64']}",
            ]

            flag = parsed.get("flag")
            if flag is not None:
                lines.append(f"{prefix} flag: {flag}")

            if matched_request:
                label = matched_request.get("label")
                lines.append(f"Routed as reply to: {label}")

            if parsed.get("json") is not None:
                lines.append(f"{prefix} decrypted JSON: {json.dumps(parsed['json'], ensure_ascii=False)}")
            elif parsed.get("plaintext"):
                lines.append(f"{prefix} decrypted payload: {parsed['plaintext']}")
            elif flag == FLAG_DUAL_DATA:
                lines.append(f"{prefix} plaintext: (dual-block handshake envelope)")
            elif parsed.get("error"):
                lines.append(f"{prefix} parse error: {parsed['error']}")

            self._log_multiline_debug(f"{prefix} {route.upper()}{size_note}", lines)

        def _tcp_reader_loop() -> None:
            """Continuously read TCP frames and enqueue them for processing."""
            while not reader_stop.is_set() and not should_stop.is_set():
                try:
                    packet = sock.recv(4096)
                    if not packet:
                        incoming_queue.put((time.time(), b""))
                        break
                    if runtime_session is not None:
                        runtime_session.mark_inbound_traffic()
                    incoming_queue.put((time.time(), packet))
                except socket.timeout:
                    continue
                except OSError:
                    if not reader_stop.is_set():
                        incoming_queue.put((time.time(), b""))
                    break

        def _drain_incoming(max_messages: int = 20) -> int:
            """Drain queued packets and print/decrypt immediately."""
            nonlocal connection_closed

            processed = 0
            while processed < max_messages:
                try:
                    _, packet = incoming_queue.get_nowait()
                except queue.Empty:
                    break

                if packet == b"":
                    if not connection_closed:
                        self._log_warning("TCP connection closed by hub")
                        connection_closed = True
                        if runtime_session is not None:
                            runtime_session.mark_connection_closed()
                    continue

                raw_b64 = packet.decode("utf-8", errors="ignore").strip()
                if not raw_b64:
                    continue

                parsed = _parse_message(raw_b64, extracted_key)
                route, matched_request = _classify_message("in", parsed)
                _log_message("in", parsed, route, matched_request, byte_len=len(packet))

                payload_obj = parsed.get("json") if isinstance(parsed.get("json"), dict) else None
                if route == "device_or_control_update" and isinstance(payload_obj, dict) and payload_obj.get("type") == "bleData":
                    _update_ready_state(saw_bledata=True)
                    _maybe_signal_ready()
                elif route == "eack_reply":
                    _update_ready_state(saw_eack_reply=True)
                    # In captures, the first ACK after heartbeat can still be routed as
                    # eack_reply due request ordering. Treat any ACK after heartbeat send
                    # as completing the first heartbeat roundtrip.
                    if readiness["sent_heartbeat"]:
                        _update_ready_state(saw_heartbeat_reply=True)
                        if runtime_session is not None:
                            runtime_session.mark_heartbeat_reply()
                    _maybe_signal_ready()
                elif route == "ack":
                    if readiness["sent_heartbeat"]:
                        _update_ready_state(saw_heartbeat_reply=True)
                        if runtime_session is not None:
                            runtime_session.mark_heartbeat_reply()
                    _maybe_signal_ready()
                elif route in ("heartbeat_reply", "heartbeat_push"):
                    _update_ready_state(saw_heartbeat_reply=True)
                    if runtime_session is not None:
                        runtime_session.mark_heartbeat_reply()
                    _maybe_signal_ready()

                if route == "device_or_control_update":
                    _apply_flag1_update(parsed)
                processed += 1

            return processed

        def _send_heartbeat_frame(label: str) -> bool:
            """Send one heartbeat and process a short response window."""
            hb_msg = PixieMessage.build_heartbeat(extracted_key)
            _update_ready_state(sent_heartbeat=True)
            previous_reply_at = runtime_session.last_heartbeat_reply_at if runtime_session is not None else None
            if runtime_session is not None:
                runtime_session.mark_heartbeat_sent()
            pending_requests.append({"kind": "heartbeat", "label": label})
            hb_parsed = _parse_message(hb_msg, extracted_key)
            hb_route, hb_match = _classify_message("out", hb_parsed)
            _log_message("out", hb_parsed, hb_route, hb_match)
            sock.sendall(hb_msg.encode('utf-8'))

            response_window_end = time.time() + 1.5
            got_traffic = False
            while time.time() < response_window_end and not connection_closed and not should_stop.is_set():
                if _drain_incoming() > 0:
                    got_traffic = True
                time.sleep(0.05)
            if runtime_session is not None and runtime_session.last_heartbeat_reply_at == previous_reply_at:
                runtime_session.mark_heartbeat_failure()
            return got_traffic

        try:
            # Java flow: hub sends first, then app sends eack, then heartbeat loop starts.
            self._log_debug("Waiting for hub's initial message")
            response_data = sock.recv(4096)

            if response_data:
                self._log_debug("Received %s bytes from hub", len(response_data))
                try:
                    raw_b64 = response_data.decode('utf-8').strip()
                    self._log_debug("Initial hub raw base64: %s", raw_b64)

                    envelope_bytes = PixieEnvelope.from_base64(raw_b64)
                    self._log_debug("Initial hub envelope bytes: %s...", envelope_bytes.hex()[:100])
                    envelope_struct = PixieEnvelope.decode(envelope_bytes)

                    # Java 2.22 flow:
                    # - data1 decrypted with netID => session key (f14376j)
                    # - data2 decrypted with session key => mesh validation value
                    if envelope_struct and envelope_struct.get("flag1") == 0:
                        parts = PixieEnvelope.decrypt_dual_parts(envelope_struct, self.netid_seed)
                        if parts:
                            part1, part2 = parts
                            extracted_key = part1
                            self.session_key_hex = extracted_key
                            self._log_debug("Session key extracted (Java f14376j): %s", extracted_key)
                            self._log_debug("Mesh validation value (data2): %s", part2)

                            expected_values = {str(v) for v in [self.meshnet, self.meshnet2] if v not in (None, "", "unknown")}
                            if expected_values and part2 not in expected_values:
                                self._log_warning(
                                    "Mesh validation mismatch: got=%s, expected one of %s",
                                    part2,
                                    sorted(expected_values),
                                )
                            elif expected_values:
                                self._log_debug("Mesh validation matched cloud/UDP values")
                            initial_parsed = _parse_message(raw_b64, self.netid_seed)
                            initial_route, initial_match = _classify_message("in", initial_parsed)
                            _log_message("in", initial_parsed, initial_route, initial_match, byte_len=len(response_data))
                        else:
                            self._log_warning("Could not decrypt dual-block envelope")
                    else:
                        self._log_warning("Initial hub message is not dual-block flag=0 envelope")
                except Exception as e:
                    self._log_warning("Response parse error: %s", e)
                    self._log_debug("Initial hub raw bytes: %s...", response_data.hex()[:100])

            if extracted_key:
                # Start asynchronous reader before GwData/eack to avoid missing
                # early unsolicited bulk updates.
                sock.settimeout(0.5)
                reader_thread = threading.Thread(
                    target=_tcp_reader_loop,
                    name="pixie-tcp-reader",
                    daemon=True,
                )
                reader_thread.start()

            # Step 2: Send app-like initial GwData with extracted session key.
            if extracted_key:
                gwdata_msg = PixieMessage.build_gwdata_init(extracted_key)
                gw_parsed = _parse_message(gwdata_msg, extracted_key)
                gw_route, gw_match = _classify_message("out", gw_parsed)
                _log_message("out", gw_parsed, gw_route, gw_match)
                sock.sendall(gwdata_msg.encode('utf-8'))

            # Step 3: Send eack with extracted session key.
            if extracted_key:
                eack_msg = PixieMessage.build_eack(extracted_key)
                pending_requests.append({"kind": "eack", "label": "EACK"})
                out_parsed = _parse_message(eack_msg, extracted_key)
                out_route, out_match = _classify_message("out", out_parsed)
                _log_message("out", out_parsed, out_route, out_match)
                sock.sendall(eack_msg.encode('utf-8'))

                # Let early replies land before forcing the first heartbeat.
                eack_window_end = time.time() + 0.75
                while time.time() < eack_window_end and not connection_closed and not should_stop.is_set():
                    _drain_incoming()
                    if readiness["ready_signaled"]:
                        break
                    time.sleep(0.05)

                # Optional command send after the session is authenticated.
                has_any_command = any(
                    value is not None
                    for value in (command_state, command_brightness, command_color_rgb, command_effect, command_cover_action)
                )
                if command_device_id is not None and has_any_command:
                    try:
                        _send_requested_local_command(
                            command_device_id=command_device_id,
                            command_state=command_state,
                            command_brightness=command_brightness,
                            command_color_rgb=command_color_rgb,
                            command_effect=command_effect,
                            command_target=command_target,
                            command_cover_action=command_cover_action,
                            command_cover_action_map=command_cover_action_map,
                            command_cover_tilt_action_map=command_cover_tilt_action_map,
                        )
                    except Exception as exc:
                        self._log_warning("Local command not sent: %s", exc)

                # Step 4: Continuous heartbeat loop until user stops with Ctrl+C.
                if keep_control_alive:
                    hb_idx = 0
                    self._log_debug("Starting continuous heartbeat loop")
                    try:
                        while not connection_closed and not should_stop.is_set():
                            delay = 2.0 if hb_idx == 0 else 10.0

                            # Keep processing incoming push updates while waiting for
                            # the next heartbeat tick.
                            wait_end = time.time() + delay
                            while time.time() < wait_end and not connection_closed and not should_stop.is_set():
                                _drain_incoming()
                                if command_request_queue is not None:
                                    while True:
                                        try:
                                            command_request = command_request_queue.get_nowait()
                                        except queue.Empty:
                                            break

                                        command_id = command_request.get("command_id") if isinstance(command_request, dict) else None
                                        response_queue = command_request.get("response_queue") if isinstance(command_request, dict) else None
                                        request_kwargs = command_request.get("kwargs") if isinstance(command_request, dict) else None
                                        if runtime_session is not None and isinstance(command_id, int):
                                            runtime_session.mark_command_started(command_id)
                                            runtime_session.throttle_before_command_send()
                                        try:
                                            result = _send_requested_local_command(**(request_kwargs or {}))
                                            if response_queue is not None:
                                                response_queue.put(("ok", result))
                                        except Exception as exc:
                                            if response_queue is not None:
                                                response_queue.put(("error", exc))
                                            else:
                                                self._log_warning("Live queued command failed: %s", exc)
                                        finally:
                                            if runtime_session is not None and isinstance(command_id, int):
                                                runtime_session.mark_command_finished(command_id)
                                time.sleep(0.1)

                            if connection_closed or should_stop.is_set():
                                break

                            hb_idx += 1

                            got_traffic = _send_heartbeat_frame(f"HEARTBEAT #{hb_idx}")

                            if not got_traffic and not self.suppress_heartbeat_logs:
                                self._log_debug(
                                    "No incoming TCP traffic in heartbeat #%s response window",
                                    hb_idx,
                                )
                    except KeyboardInterrupt:
                        self._log_info("Heartbeat loop stopped by user")
                    finally:
                        if should_stop.is_set() and not connection_closed:
                            self._log_info("Control stop signal received")
                else:
                    self._log_debug("Control keepalive skipped (one-shot startup mode)")

        except Exception as e:
            self._log_exception("Handshake error: %s", e)
        finally:
            reader_stop.set()
            if reader_thread and reader_thread.is_alive():
                reader_thread.join(timeout=1.0)
            sock.close()

        # Step 3: Extract netID/meshNet from already fetched cloud config
        self._log_debug("Finalizing credentials")

        if not self.netid_seed:
            raise PixieAuthError("No netID seed available; ensure cloud login succeeded before handshake capture")

        # Use previously fetched values (from _fetch_login_data)
        config = {
            'netid': self.netid_seed,
            'meshnet': self.meshnet,
            'meshnet2': self.meshnet2,
        }

        # Update session key if extracted
        if extracted_key:
            self.session_key_hex = extracted_key

        # Step 4: Skip file persistence (integration-layer config stores auth data)
        # We keep in-memory values only and do not persist to ~/.pixie_auth.
        # credentials are expected to be managed by Home Assistant integration.

        result = {
            'status': 'success',
            'config': config,
            'session_key_hex': self.session_key_hex,
            'hub_ip': hub_ip,
            'hub_port': hub_port
        }

        self._log_debug("Client-mode handshake complete")

        return result

    def _next_command_sequence(self, cmd_type: int) -> bytes:
        """Return the next 3-byte command header: [counter] [cmd_type] 04
        
        Args:
            cmd_type: 0x08 for USB, 0x09 for relay commands
        """
        if self._command_counter < 0x10:
            self._command_counter = 0x10
        counter_byte = (self._command_counter & 0xFF).to_bytes(1, byteorder="little")
        self._command_counter = (self._command_counter + 1) & 0xFF
        if self._command_counter > 0xFF:
            self._command_counter = 0x10
        cmd_type_byte = cmd_type.to_bytes(1, byteorder="little")
        return counter_byte + cmd_type_byte + bytes([0x04])

    def _next_brightness_sequence(self) -> bytes:
        """Return the captured 3-byte dimmer command prefix.

        Local dimmer brightness frames use [counter][counter>>1][counter>>2]
        rather than the relay/USB [counter][cmd_type][04] prefix.
        """
        if self._command_counter < 0x10:
            self._command_counter = 0x10

        counter = self._command_counter & 0xFF
        self._command_counter = (self._command_counter + 1) & 0xFF
        if self._command_counter < 0x10:
            self._command_counter = 0x10

        return bytes([
            counter,
            (counter >> 1) & 0xFF,
            (counter >> 2) & 0xFF,
        ])

    def _build_6969_onoff_command_hex(
        self,
        destination_id: int,
        *,
        is_on: bool,
        opcode: int,
        selector: int = 0,
    ) -> str:
        """Build a [counter][09:relay][04][opcode,0x69,0x69] on/off command packet for local bleData control."""
        sequence = self._next_command_sequence(cmd_type=0x09)  # 0x09 = relay command type
        src_bytes = (1027).to_bytes(2, byteorder="little")
        dst_bytes = int(destination_id).to_bytes(2, byteorder="little", signed=False)
        state_byte = b"\x01" if is_on else b"\x00"
        selector_byte = int(selector).to_bytes(1, byteorder="little", signed=False)
        payload = state_byte + selector_byte + (b"\x00" * 8)
        packet = sequence + src_bytes + dst_bytes + bytes([int(opcode) & 0xFF, 0x69, 0x69]) + payload
        return packet.hex()

    def _build_0107_usb_command_hex(self, destination_id: int, *, is_on: bool) -> str:
        """Build 0107 USB command for direct local TCP test using state bytes 0x08/0x0c.

        This mirrors legacy cloud command semantics where USB toggle is treated
        independently from relay state.
        """
        sequence = self._next_command_sequence(cmd_type=0x08)  # 0x08 = USB command type
        src_bytes = (1027).to_bytes(2, byteorder="little")
        dst_bytes = int(destination_id).to_bytes(2, byteorder="little", signed=False)

        state_byte = 0x0C if is_on else 0x08
        state_byte_val = state_byte.to_bytes(1, byteorder="little", signed=False)
        payload = state_byte_val + (b"\x00" * 3)  # 4 bytes total: [state][000000]
        packet = sequence + src_bytes + dst_bytes + bytes([0xC1, 0x69, 0x69]) + payload
        return packet.hex(), state_byte

    def _build_brightness_command_hex(
        self,
        destination_id: int,
        *,
        brightness_level: int,
    ) -> str:
        """Build a local dimmer brightness command using the captured e76969 format."""
        if not (0 <= brightness_level <= 100):
            raise PixieAuthError(f"Brightness must be 0-100, got {brightness_level}")

        sequence = self._next_brightness_sequence()
        src_bytes = (1027).to_bytes(2, byteorder="little")
        destination_marker = b"\xff\xff"
        dst_bytes = int(destination_id).to_bytes(2, byteorder="little", signed=False)

        # Captures align more closely with a 0-256 scale than simple floor(0-255).
        brightness_byte = min(0xFF, max(0x00, round((brightness_level * 256) / 100)))
        payload = bytes([0x32, 0x00, 0x10, brightness_byte, 0x00, 0x00]) + dst_bytes

        packet = sequence + src_bytes + destination_marker + bytes([0xE7, 0x69, 0x69]) + payload
        return packet.hex()

    def _build_color_command_hex(
        self,
        destination_id: int,
        *,
        rgb: Tuple[int, int, int],
        brightness_level: int,
    ) -> str:
        """Build app-style color command using captured c16969 payload bytes.

        The unresolved prefix bytes stay on the existing local command path,
        but the opcode/payload now match captures: [RRGGBB][brightness].
        """
        if not (0 <= brightness_level <= 100):
            raise PixieAuthError(f"Brightness must be 0-100, got {brightness_level}")

        r, g, b = rgb
        for channel in (r, g, b):
            if not (0 <= channel <= 255):
                raise PixieAuthError(f"RGB channel out of range 0-255: {channel}")

        cmd_num = (self._command_counter & 0xFF)
        self._command_counter = (self._command_counter + 1) & 0xFF
        if self._command_counter < 0x10:
            self._command_counter = 0x10

        dev_id_byte = int(destination_id) & 0xFF
        brightness_byte = min(0xFF, max(0x00, round((brightness_level * 256) / 100)))

        command_hex = (
            f"{cmd_num:02x}"
            "00000304"
            f"{dev_id_byte:02x}"
            "00c16969"
            f"{r:02x}{g:02x}{b:02x}"
            f"{brightness_byte:02x}"
        )
        return command_hex

    def _build_effect_command_hex(
        self,
        destination_id: int,
        *,
        effect_name: Optional[str],
        effect_speed: int,
        brightness_level: int,
    ) -> str:
        """Build app-style effect command using captured f86969 payload bytes.

        Captured payload after opcode is:
        [effect_code][speed][ff][00][brightness].
        """
        effect_map = {
            "none": "00",
            "flash": "01",
            "strobe": "02",
            "smooth": "03",
            "fade": "04",
        }
        normalized = (effect_name or "none").strip().lower()
        if normalized not in effect_map:
            raise PixieAuthError(f"Unsupported effect: {effect_name}")
        if not (0 <= effect_speed <= 255):
            raise PixieAuthError(f"Effect speed must be 0-255, got {effect_speed}")
        if not (0 <= brightness_level <= 100):
            raise PixieAuthError(f"Brightness must be 0-100, got {brightness_level}")

        cmd_num = (self._command_counter & 0xFF)
        self._command_counter = (self._command_counter + 1) & 0xFF
        if self._command_counter < 0x10:
            self._command_counter = 0x10

        dev_id_byte = int(destination_id) & 0xFF
        brightness_byte = min(0xFF, max(0x00, round((brightness_level * 256) / 100)))
        command_hex = (
            f"{cmd_num:02x}"
            "00000304"
            f"{dev_id_byte:02x}"
            "00f86969"
            f"{effect_map[normalized]}"
            f"{effect_speed:02x}"
            "ff00"
            f"{brightness_byte:02x}"
        )
        return command_hex

    def _build_cover_press_command_hex(
        self,
        destination_id: int,
        *,
        button_position: int,
    ) -> str:
        """Build app-style cover button press command using captured c16969 payload format."""
        if not (1 <= int(button_position) <= 9):
            raise PixieAuthError(f"Cover button position must be 1-9, got {button_position}")

        sequence = self._next_brightness_sequence()
        src_bytes = (1027).to_bytes(2, byteorder="little")
        dst_bytes = int(destination_id).to_bytes(2, byteorder="little", signed=False)
        payload = b"\x00\x00\x00" + bytes([(int(button_position) - 1) & 0xFF])
        packet = sequence + src_bytes + dst_bytes + bytes([0xC1, 0x69, 0x69]) + payload
        return packet.hex()

    def _resolve_command_target_for_device(self, device_id: int, requested_target: Optional[str]) -> str:
        """Resolve command target with a per-device default when --target is omitted."""
        if requested_target:
            return requested_target.strip().lower()

        if self.inventory:
            rec = self.inventory.devices_by_id.get(int(device_id))
            if rec and rec.model_no == "0107":
                return "relay"

        return "relay"

    def _resolve_onoff_command_spec(self, target: str) -> Dict[str, Any]:
        """Resolve command target into opcode/selector values recovered from captures."""
        normalized = (target or "relay").strip().lower()
        spec_map: Dict[str, Dict[str, Any]] = {
            "relay": {"opcode": 0xED, "selector": 0, "label": "relay/main", "opcode_name": "ed6969"},
            "usb": {"opcode": 0xC1, "selector": 0, "label": "usb", "opcode_name": "c16969"},
            "left": {"opcode": 0xED, "selector": 1, "label": "left", "opcode_name": "ed6969"},
            "right": {"opcode": 0xED, "selector": 2, "label": "right", "opcode_name": "ed6969"},
            "both": {"opcode": 0xED, "selector": 0, "label": "both", "opcode_name": "ed6969"},
        }
        if normalized not in spec_map:
            raise PixieAuthError(f"Unsupported command target: {target}")
        return spec_map[normalized]

    def _build_local_bledata_command_debug(self, *, key: str, command_hex: str, from_email: str) -> Dict[str, Any]:
        """Build a local bleData command and return all debug stages."""
        payload = {
            "data": {
                "type": "bleData",
                "data": command_hex,
                "repeat": 0,
            },
            "from": from_email,
        }
        plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        encrypted = PixieCrypto.encrypt(plaintext, key)
        envelope = bytes([1]) + encrypted
        return {
            "payload": payload,
            "plaintext_json": plaintext,
            "command_hex": command_hex,
            "encrypted_hex": encrypted.hex(),
            "envelope_hex": envelope.hex(),
            "base64": PixieEnvelope.to_base64(envelope),
        }

    def _print_local_command_debug(self, command_debug: Dict[str, Any]) -> None:
        """Print local command build stages for debugging command failures."""
        if not self._debug_enabled():
            return
        self._log_debug("Local command payload JSON: %s", json.dumps(command_debug.get('payload', {}), ensure_ascii=False))
        self._log_debug("Local command plaintext JSON: %s", command_debug.get('plaintext_json'))
        self._log_debug("Local command encrypted hex: %s", command_debug.get('encrypted_hex'))
        self._log_debug("Local command envelope hex: %s", command_debug.get('envelope_hex'))
        self._log_debug("Local command base64: %s", command_debug.get('base64'))

# Removed: AuthCredentials class and _save_credentials method - no longer needed


# Removed: PixieCrypto, _pkcs7_pad, _pkcs7_unpad - now in pixie_protocol.py
# Removed: bytes_to_hex - not needed



