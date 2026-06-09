#!/usr/bin/env python3
"""lfish - BMC and BIOS firmware updater via Redfish API.

Supports AMI-based BMCs with SimpleUpdate (from URL) and multipart
file upload update methods.  Tracks update progress via the Redfish
TaskService.

The -H/--host flag accepts Slurm-style hostlists (e.g. node[001-008])
and runs against all expanded hosts in parallel.

Usage:
    lfish info     -H HOST [-u USER] [-p PASS]
    lfish methods
    lfish update   -H HOST [-u USER] [-p PASS] -c COMPONENT
                   ( -f FILE | --url URL ) [options]
    lfish tasks    -H HOST [-u USER] [-p PASS]


Architecture
============

Each "update method" (multipart push, SimpleUpdate pull, OEM-action
flavours, future in-band methods, …) is registered into a small
``METHODS`` registry. Adding a new method is a one-place edit:
write the ``apply()`` callable + ``register_method(UpdateMethod(...))``.
Operators can drop ``.py`` files into ``$LFISH_PLUGINS_DIR`` to add
methods at runtime without touching this file.

The common pre/post-flash hooks (UpdateService readiness, prior-
version capture, preserve PATCH, post-flash version verify, AMI
UpdateInformation sanity check) are factored into ``run_with_hooks``
and shared across methods so a new method doesn't reinvent them.

Per-call overrides on the CLI (``--target-override``,
``--image-type-override``, ``--preserve-keys``, ``--no-preserve``,
``--push-uri-override``) let an operator experiment with quirky
BMCs without code edits — discovery is the default, overrides win.

Discovery-driven targeting (added 2026-06-09 after a fleet of
silent-bail / dual-bank / dual-BIOS failures on Gigabyte AMI
hardware):

  * ``Targets`` is built from the live ``FirmwareInventory``, not
    hardcoded to a ``BIOS`` / ``BMC`` member name. Dual-bank chassis
    expose ``BMCImage1`` + ``BMCImage2`` (Gigabyte G293 / R263);
    dual-BIOS chassis expose ``BIOS`` + ``BIOS2``. For BMC the
    active bank wins; for BIOS the *boot* slot wins, identified by
    the absence of a populated ``Version`` field (AMI populates
    Version only on the slot it last wrote, which is the inactive
    bank).
  * ``OemParameters.ImageType`` stays in AMI's closed vocabulary
    (BIOS / BMC / CPLD). It used to carry the same value as
    ``Targets``; that produced HTTP 400 ``InvalidVariableValue``
    when ``Targets`` carried a slot suffix.
  * ``MultipartHttpPushUri`` is read from the UpdateService rather
    than hardcoded — most AMI builds use ``/upload`` but the DMTF
    standard lets the BMC name its own path.
  * ``PreserveConfiguration`` keys are read from the live BMC's
    ``Oem.AMIUpdateService.PreserveConfiguration`` block; the
    previous hardcoded set caused the PATCH to fail silently on
    BMCs that only accept ``{"BMC": true}`` or that use a different
    capitalisation.
  * Preserve is only PATCHed for ``component == "BMC"`` — it has
    no effect on a BIOS / CPLD flash so PATCHing during one of
    those would just be noise.
  * Before each push we poll ``GET /redfish/v1/UpdateService``
    until ServiceEnabled=True and the body carries no
    ``ServiceInUnknownState`` marker. AMI returns HTTP 503 for
    30–120s after a BMC self-reset; firing the next push too soon
    eats it.
  * After Task=Completed/OK we *verify* the active version: BIOS
    via ``Systems/Self.BiosVersion``, BMC via
    ``Managers/Self.FirmwareVersion``. AMI happily reports
    Completed/OK on a write that PrepareFlashArea silently bailed
    on (the new image lives in the inactive slot, the chassis
    boots the old one). Without the version check those failures
    look like success.
  * ``UpdateService.Oem.AMIUpdateService.UpdateInformation.UpdateStatus``
    is checked alongside the Task: AMI surfaces "Failed when
    Preparing Flash Device Area." here while Task says Completed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import requests
import urllib3
from hostlist import expand_hostlist
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Constants ────────────────────────────────────────────────────────

DEFAULT_USER = "admin"
DEFAULT_PASS = "admin"
DEFAULT_WORKERS = 20
UPLOAD_TIMEOUT = 1800   # 30 min — BMC upload endpoints are slow
POLL_INTERVAL = 10      # seconds between task status polls
POLL_TIMEOUT = 1800     # 30 min max wait for a task to finish
REBOOT_TIMEOUT = 600    # 10 min max wait for BMC to come back after flash
REBOOT_INTERVAL = 15    # seconds between reboot polls
READY_TIMEOUT = 300     # 5 min cap on the pre-push UpdateService readiness gate
READY_INTERVAL = 10
VERIFY_BIOS_TIMEOUT = 900   # post-flash chassis-POST can take ~5 min
VERIFY_BMC_TIMEOUT = 600
VERIFY_INTERVAL = 15

# Conservative fallback when the BMC doesn't expose
# Oem.AMIUpdateService.PreserveConfiguration. The discovery path
# is preferred; this is only used when the GET fails entirely.
LEGACY_PRESERVE_KEYS = ["snmp", "kvm", "network", "ipmi", "ntp",
                          "authentication", "syslog"]

# Redfish paths
PATH_ROOT = "/redfish/v1/"
PATH_SYSTEM = "/redfish/v1/Systems/Self"
PATH_MANAGER = "/redfish/v1/Managers/Self"
PATH_UPDATE_SERVICE = "/redfish/v1/UpdateService"
PATH_FW_INVENTORY = "/redfish/v1/UpdateService/FirmwareInventory"
PATH_SIMPLE_UPDATE = "/redfish/v1/UpdateService/Actions/SimpleUpdate"
PATH_SIMPLE_UPDATE_INFO = "/redfish/v1/UpdateService/SimpleUpdateActionInfo"
PATH_DEFAULT_UPLOAD = "/redfish/v1/UpdateService/upload"   # AMI default
PATH_TASKS = "/redfish/v1/TaskService/Tasks"


# ── Redfish client ───────────────────────────────────────────────────

class RedfishClient:
    """Thin wrapper around a Redfish BMC connection."""

    def __init__(self, host, username, password, verify_ssl=False):
        self.host = host
        self.base = f"https://{host}"
        self.auth = (username, password)
        self.verify = verify_ssl
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = self.verify
        self.session.headers.update({"Content-Type": "application/json"})

    # ── HTTP verbs ───────────────────────────────────────────────

    def get(self, path):
        r = self.session.get(self.base + path, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_raw(self, path):
        """GET that returns the Response object instead of parsed JSON.
        Lets callers see HTTP status + body when status != 200 (e.g.
        for the UpdateService readiness gate that watches for HTTP
        503 ServiceInUnknownState)."""
        return self.session.get(self.base + path, timeout=10)

    def patch(self, path, payload, headers=None):
        hh = {"Content-Type": "application/json"}
        if headers:
            hh.update(headers)
        return self.session.patch(self.base + path, json=payload,
                                    headers=hh, timeout=15)

    def post(self, path, payload=None):
        return self.session.post(self.base + path, json=payload, timeout=60)

    def post_multipart(self, path, fields, progress_cb=None):
        """Stream a multipart upload using requests-toolbelt.

        Unlike requests' built-in files= which loads the entire body
        into memory and writes it to the socket in one shot, this
        streams data in ~8 KB chunks so each individual SSL write
        completes quickly — critical for slow BMC upload endpoints.
        """
        encoder = MultipartEncoder(fields=fields)
        if progress_cb:
            monitor = MultipartEncoderMonitor(encoder, progress_cb)
        else:
            monitor = encoder

        return self.session.post(
            self.base + path,
            data=monitor,
            headers={"Content-Type": monitor.content_type},
            timeout=UPLOAD_TIMEOUT,
        )

    # ── Convenience ──────────────────────────────────────────────

    def check_connection(self):
        """Verify Redfish is reachable.  Returns (ok, error_msg)."""
        try:
            self.get(PATH_ROOT)
            return True, None
        except requests.ConnectionError:
            return False, f"Cannot connect to {self.host}"
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                return False, "Authentication failed (HTTP 401)"
            return False, str(e)


# ── Discovery: pick the right Targets / ImageType / preserve set ─────

@dataclass
class UpdateStrategy:
    """Live-discovered per-BMC parameters that drive a method's flash.

    ``upload_uri``
        ``MultipartHttpPushUri`` from the UpdateService, with the AMI
        default as fallback.
    ``target_inventory_uri``
        Slot-specific FirmwareInventory member to put in
        ``UpdateParameters.Targets``. On a dual-bank BMC this is
        ``BMCImage<ActiveImage>``; on a dual-BIOS chassis it's the
        boot slot (the member with NO Version reported).
    ``oem_image_type``
        Generic component class (BIOS / BMC / CPLD) for
        ``OemParameters.ImageType``. Slot-suffixed values trip
        ``UpdateService.1.0.InvalidVariableValue``.
    ``preserve_keys``
        Exactly the keys the BMC's own PreserveConfiguration
        validator accepts. Empty when the BMC doesn't expose the
        block — PATCH is then skipped.
    ``max_image_size``
        For the early-fail size check; ``None`` when unset.
    ``simple_update_action_uri``
        ``Actions["#UpdateService.SimpleUpdate"].target`` from the
        UpdateService block — falls back to the DMTF path when the
        BMC doesn't advertise an explicit target.
    """
    upload_uri: str
    target_inventory_uri: str
    oem_image_type: str
    preserve_keys: List[str] = field(default_factory=list)
    max_image_size: Optional[int] = None
    simple_update_action_uri: str = PATH_SIMPLE_UPDATE


def _pick_bios_slot(client, members, log):
    """Pick which BIOS inventory member to write to.

    On a dual-BIOS chassis (Gigabyte R263-ZG0-AAL2 etc.) AMI
    populates ``Version`` only on the slot it last wrote — the
    BOOT slot has a blank/missing Version. Pick that one so the
    new image lands in the slot the chassis actually boots from.

    Single-BIOS boards just have ``BIOS``; return it unconditionally.
    """
    bios_slots = [m for m in members if m.startswith("BIOS")]
    if len(bios_slots) <= 1:
        return bios_slots[0] if bios_slots else None

    versions = {}
    for slot in bios_slots:
        try:
            d = client.get(f"{PATH_FW_INVENTORY}/{slot}")
        except requests.HTTPError:
            d = {}
        versions[slot] = (d.get("Version") or "").strip()

    no_ver = [s for s, v in versions.items() if not v]
    if len(no_ver) == 1:
        log(f"  Dual-BIOS detected; boot slot = {no_ver[0]} "
            f"(slot {[s for s in versions if s not in no_ver][0]} "
            f"carries the last-written Version field, so it's the "
            f"backup slot)")
        return no_ver[0]
    # Fallback: prefer BIOS2 because Gigabyte's SEL ping-pong
    # consistently parks the boot slot there.
    log("  Dual-BIOS detected but boot slot ambiguous; defaulting to BIOS2")
    return "BIOS2" if "BIOS2" in versions else bios_slots[0]


def _pick_bmc_slot(members, dual_active):
    """Pick which BMC inventory member to write to.

    * Single-member: just ``BMC``.
    * Dual-bank with DualImageConfigurations.ActiveImage exposed:
      ``BMCImage<N>`` where N is the active bank — overwriting the
      active bank makes the BMC reboot into the new image (the
      AMI flow we want).
    * Dual-bank without DualImage info: default to BMCImage1.
    """
    if "BMC" in members:
        return "BMC"
    if dual_active:
        candidate = f"BMCImage{dual_active}"
        if candidate in members:
            return candidate
    if "BMCImage1" in members:
        return "BMCImage1"
    bmc_slots = [m for m in members if m.startswith("BMC")]
    return bmc_slots[0] if bmc_slots else None


def discover_strategy(client, component, log):
    """Return an :class:`UpdateStrategy` for ``component`` on this BMC.

    Raises ``RuntimeError`` if the BMC doesn't expose enough to build
    a workable strategy — the caller turns that into a clear
    operator-facing error before the file gets streamed.
    """
    try:
        us = client.get(PATH_UPDATE_SERVICE)
    except requests.HTTPError as e:
        raise RuntimeError(f"GET {PATH_UPDATE_SERVICE} failed: {e}") from None

    upload_uri = us.get("MultipartHttpPushUri") or PATH_DEFAULT_UPLOAD
    max_size = us.get("MaxImageSizeBytes")

    # Discover the SimpleUpdate Action URI — DMTF spec lets the BMC
    # publish its own; AMI default is /Actions/SimpleUpdate.
    actions = (us.get("Actions") or {})
    simple_uri = ((actions.get("#UpdateService.SimpleUpdate") or {})
                  .get("target")) or PATH_SIMPLE_UPDATE

    # FirmwareInventory members
    try:
        fi = client.get(PATH_FW_INVENTORY)
    except requests.HTTPError as e:
        raise RuntimeError(f"GET {PATH_FW_INVENTORY} failed: {e}") from None
    members = [
        (m.get("@odata.id") or "").rsplit("/", 1)[-1]
        for m in fi.get("Members", [])
    ]
    members = [m for m in members if m]
    if not members:
        raise RuntimeError("FirmwareInventory has no members")

    # Pick the target slot — uppercased component is the canonical
    # class; the slot picker may return a slot-suffixed name.
    cclass = component.upper()
    if cclass == "BIOS":
        slot = _pick_bios_slot(client, members, log)
    elif cclass == "BMC":
        bmc_block = (us.get("Oem") or {}).get("BMC") or {}
        dual = (bmc_block.get("DualImageConfigurations") or {}).get("ActiveImage")
        slot = _pick_bmc_slot(members, dual)
    else:
        # CPLD / NIC / drive — try exact match first, then prefix.
        slot = cclass if cclass in members else None
        if slot is None:
            for m in members:
                if m.startswith(cclass):
                    slot = m
                    break
    if slot is None:
        raise RuntimeError(
            f"no FirmwareInventory member for component {cclass!r}; "
            f"members are {members!r}"
        )

    # PreserveConfiguration keys — read live from the BMC's own
    # schema so a key the validator rejects can't reach the PATCH.
    preserve_keys: List[str] = []
    ami = (us.get("Oem") or {}).get("AMIUpdateService") or {}
    pcfg = ami.get("PreserveConfiguration")
    if isinstance(pcfg, dict):
        preserve_keys = sorted(pcfg.keys())

    target_uri = f"{PATH_FW_INVENTORY}/{slot}"
    log(f"  Strategy  : push={upload_uri} target={target_uri} "
        f"image_type={cclass} preserve={preserve_keys or '(none)'}")
    return UpdateStrategy(
        upload_uri=upload_uri,
        target_inventory_uri=target_uri,
        oem_image_type=cclass,
        preserve_keys=preserve_keys,
        max_image_size=max_size,
        simple_update_action_uri=simple_uri,
    )


# ── Update-method registry ───────────────────────────────────────────

@dataclass
class UpdateContext:
    """Everything an :class:`UpdateMethod` needs to know.

    The same dataclass is handed to every method; per-method
    behaviour is encoded in the apply callable, not in the context
    shape. Operator overrides live in ``overrides`` and win over
    the discovered ``strategy``.
    """
    client: RedfishClient
    component: str
    strategy: UpdateStrategy
    log: Callable
    # Input sources — exactly one of these is meaningful per call.
    filepath: Optional[str] = None
    url: Optional[str] = None
    protocol: Optional[str] = None
    # Operator-facing knobs that don't depend on the method.
    expected_version: Optional[str] = None
    preserve_legacy: bool = False
    no_preserve: bool = False
    # Per-call strategy overrides. Empty value = use the discovered
    # strategy. Keys: ``target_uri``, ``image_type``, ``push_uri``,
    # ``preserve_keys`` (list[str]), ``simple_update_uri``.
    overrides: Dict[str, Any] = field(default_factory=dict)

    # Convenience accessors that honour overrides.

    @property
    def target_uri(self) -> str:
        return self.overrides.get("target_uri") \
            or self.strategy.target_inventory_uri

    @property
    def image_type(self) -> str:
        return self.overrides.get("image_type") \
            or self.strategy.oem_image_type

    @property
    def push_uri(self) -> str:
        return self.overrides.get("push_uri") or self.strategy.upload_uri

    @property
    def simple_update_uri(self) -> str:
        return self.overrides.get("simple_update_uri") \
            or self.strategy.simple_update_action_uri

    @property
    def preserve_keys(self) -> List[str]:
        if self.no_preserve:
            return []
        ov = self.overrides.get("preserve_keys")
        if ov is not None:
            return list(ov)
        return list(self.strategy.preserve_keys)


@dataclass
class UpdateMethod:
    """Registered update method.

    ``apply`` takes an :class:`UpdateContext` and returns a bool
    indicating "the BMC-side flash sequence succeeded". The common
    pre/post hooks (preserve PATCH, version verify, …) live in
    :func:`run_with_hooks` and wrap any registered method.
    """
    name: str
    label: str
    family: str   # "out_of_band" / "in_band"
    description: str
    apply: Callable[[UpdateContext], bool]
    applicable_components: List[str] = field(default_factory=list)
    requires: str = "file"        # "file" | "url" | "either"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "family": self.family,
            "description": self.description,
            "applicable_components": self.applicable_components,
            "requires": self.requires,
        }


METHODS: Dict[str, UpdateMethod] = {}


def register_method(spec: UpdateMethod) -> UpdateMethod:
    """Add a method to the global registry.

    Duplicate names overwrite — intentional so a plugin can monkey-
    patch a built-in's behaviour without renaming.
    """
    METHODS[spec.name] = spec
    return spec


# ── Pre-push readiness gate ──────────────────────────────────────────

def wait_for_update_service_ready(client, log):
    """Poll the UpdateService until it's ready to accept a fresh push.

    AMI MegaRAC SP-X returns HTTP 503 ``ServiceInUnknownState`` for
    ~30–120s after a BMC self-reset (e.g. immediately after a BMC
    flash). Firing the next push during that window aborts before
    the image hits the wire.

    Non-fatal on timeout: the caller proceeds and a real failure
    surfaces from the multipart response. The wait is best-effort
    to avoid the 503 window.
    """
    log(f"  Waiting for UpdateService ready (cap {READY_TIMEOUT}s) ...")
    deadline = time.time() + READY_TIMEOUT
    last_state = ""
    while time.time() < deadline:
        try:
            r = client.get_raw(PATH_UPDATE_SERVICE)
        except (requests.ConnectionError, requests.Timeout) as e:
            state = f"connect: {type(e).__name__}"
            if state != last_state:
                log(f"  {state} — retrying"); last_state = state
            time.sleep(READY_INTERVAL)
            continue
        body = r.text or ""
        if r.status_code == 200 and "ServiceInUnknownState" not in body:
            try:
                enabled = (r.json() or {}).get("ServiceEnabled")
            except (json.JSONDecodeError, ValueError):
                enabled = None
            if enabled is None or enabled is True:
                log(f"  UpdateService ready (HTTP 200, ServiceEnabled={enabled})")
                return True
            state = f"HTTP 200 but ServiceEnabled={enabled!r}"
        elif r.status_code == 503:
            state = "HTTP 503 ServiceInUnknownState"
        else:
            state = f"HTTP {r.status_code}"
        if state != last_state:
            log(f"  {state} — retrying"); last_state = state
        time.sleep(READY_INTERVAL)
    log(f"  Readiness cap reached ({READY_TIMEOUT}s); pushing anyway")
    return False


# ── PreserveConfiguration via Redfish PATCH ──────────────────────────

def patch_preserve_redfish(client, keys, log):
    """PATCH ``Oem.AMIUpdateService.PreserveConfiguration`` via Redfish.

    AMI requires an ``If-Match`` ETag on every PATCH; fetch the
    current etag, then submit. Non-fatal: a failed PATCH just means
    the BMC may revert preserved sections on reboot — the flash
    itself proceeds.
    """
    if not keys:
        log("  Preserve : skipped — empty key set")
        return True
    try:
        gr = client.get_raw(PATH_UPDATE_SERVICE)
    except requests.RequestException as e:
        log(f"  Preserve : etag fetch failed ({e}); skipping PATCH")
        return False
    etag = gr.headers.get("ETag")
    if not etag:
        try:
            etag = (gr.json() or {}).get("@odata.etag")
        except (json.JSONDecodeError, ValueError):
            etag = None

    body = {"Oem": {"AMIUpdateService": {
        "PreserveConfiguration": {k: True for k in keys},
    }}}
    headers = {"If-Match": etag} if etag else {}
    try:
        pr = client.patch(PATH_UPDATE_SERVICE, body, headers=headers)
    except requests.RequestException as e:
        log(f"  Preserve : PATCH failed ({e})")
        return False
    if pr.status_code >= 400:
        log(f"  Preserve : PATCH HTTP {pr.status_code} — "
            f"{(pr.text or '')[:200]}")
        return False
    log(f"  Preserve : PATCHed {keys}")
    return True


# ── AMI maintenance API (legacy fallback) ────────────────────────────

def _ami_api_login(base_url, username, password, verify_ssl):
    """Log in to the AMI proprietary REST API.

    Returns (session, csrf_token) on success, raises on failure.
    """
    api = requests.Session()
    api.verify = verify_ssl

    r = api.post(f"{base_url}/api/session",
                 data={"username": username, "password": password},
                 timeout=15)

    if r.status_code != 200:
        raise RuntimeError(f"BMC API login failed (HTTP {r.status_code})")

    data = r.json()
    csrf = data.get("CSRFToken", "")
    if not csrf:
        raise RuntimeError("No CSRF token in login response")

    api.headers.update({"X-CSRFTOKEN": csrf})
    return api


def set_preserve_config_legacy(client, log):
    """Legacy AMI proprietary REST path: ``PUT /api/maintenance/backup_config``.

    Kept as a fallback for older AMI builds where the Redfish
    PATCH path isn't honoured. Most current AMI builds prefer the
    Redfish path (``patch_preserve_redfish``).
    """
    log("  Preserve : (legacy) AMI maintenance API path")

    try:
        api = _ami_api_login(client.base, *client.auth, client.verify)
    except (requests.ConnectionError, requests.Timeout) as e:
        log(f"  Preserve : cannot reach BMC maintenance API: {e}")
        return False
    except (RuntimeError, json.JSONDecodeError, ValueError) as e:
        log(f"  Preserve : {e}")
        return False

    try:
        r = api.put(f"{client.base}/api/maintenance/backup_config",
                    data=json.dumps({k: 1 for k in LEGACY_PRESERVE_KEYS}),
                    headers={"Content-Type": "application/json"},
                    timeout=15)
    except (requests.ConnectionError, requests.Timeout) as e:
        log(f"  Preserve : PUT failed: {e}")
        return False

    if r.status_code != 200:
        log(f"  Preserve : PUT HTTP {r.status_code}: {r.text[:300]}")
        return False

    try:
        result = r.json()
        enabled = [k for k in LEGACY_PRESERVE_KEYS if result.get(k) == 1]
        log(f"  Preserve : legacy path enabled {', '.join(enabled)}")
    except (json.JSONDecodeError, ValueError):
        log("  Preserve : legacy path set (could not parse response)")

    return True


def apply_preserve(ctx: UpdateContext) -> bool:
    """Top-level preserve dispatcher.

    Skipped entirely unless ``component == "BMC"`` — the keys name
    BMC config sections (network, auth, IPMI, …) so PATCHing them
    during a BIOS / CPLD flash would just be noise.
    """
    if ctx.no_preserve:
        ctx.log("  Preserve : disabled by --no-preserve")
        return True
    if ctx.component.upper() != "BMC":
        ctx.log("  Preserve : skipped — not a BMC flash")
        return True
    if ctx.preserve_legacy:
        return set_preserve_config_legacy(ctx.client, ctx.log)
    return patch_preserve_redfish(ctx.client, ctx.preserve_keys, ctx.log)


# ── Task tracking ────────────────────────────────────────────────────

def _extract_task_uri(response):
    """Try to find a task URI in an update response."""
    # Location header (most common)
    loc = response.headers.get("Location")
    if loc:
        return urlparse(loc).path

    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        return None

    # Direct task object
    if body.get("@odata.type", "").startswith("#Task."):
        return body.get("@odata.id")

    # Buried in extended error info
    messages = body.get("Messages",
                        body.get("error", {}).get("@Message.ExtendedInfo", []))
    for msg in messages:
        for arg in msg.get("MessageArgs", []):
            if "/TaskService/Tasks/" in str(arg):
                return arg

    return None


def _resolve_completed_task(client, task_uri, log):
    """When a TaskMonitor returns 404, fall back to the persistent Task.

    A TaskMonitor (transient) is removed after completion, but the
    underlying Task at /redfish/v1/TaskService/Tasks/<id> persists
    and holds the final state, status and messages.
    """
    if "/TaskMonitors/" not in task_uri:
        return None

    task_id = task_uri.rstrip("/").rsplit("/", 1)[-1]
    fallback_uri = f"{PATH_TASKS}/{task_id}"
    try:
        return client.get(fallback_uri)
    except requests.HTTPError:
        return None


def _poll_task(client, task_uri, log):
    """Poll a task until it reaches a terminal state or times out."""
    log(f"  Tracking task: {task_uri}")
    start = time.time()
    last_pct = -1

    while time.time() - start < POLL_TIMEOUT:
        try:
            t = client.get(task_uri)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # TaskMonitor disappeared — task completed and was cleaned up.
                # Fall back to the persistent Task to read the real outcome.
                t = _resolve_completed_task(client, task_uri, log)
                if t:
                    state = t.get("TaskState", "Unknown")
                    status = t.get("TaskStatus", "")
                    for msg in t.get("Messages", []):
                        log(f"  {msg.get('Message', '')}")
                    if state == "Completed" and status in ("OK", ""):
                        log("  Update task reached Completed/OK.")
                        return True
                    log(f"  Update task ended: state={state}, status={status}")
                    return False
                log("  Task monitor cleared and Task record not found — assuming success.")
                return True
            log(f"  Task returned {e.response.status_code} — may have completed.")
            return True

        state = t.get("TaskState", "Unknown")
        pct = t.get("PercentComplete")

        if pct is not None and pct != last_pct:
            last_pct = pct
            bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
            log(f"  [{bar}] {pct:3d}%  {state}")
        elif pct is None and state != "Unknown":
            log(f"  State: {state}")

        if state in ("Completed", "Exception", "Killed", "Cancelled"):
            for msg in t.get("Messages", []):
                log(f"  {msg.get('Message', '')}")
            if state == "Completed" and t.get("TaskStatus", "") in ("OK", ""):
                log("  Update task reached Completed/OK.")
                return True
            log(f"  Update task ended: state={state}, status={t.get('TaskStatus', '')}")
            return False

        time.sleep(POLL_INTERVAL)

    log(f"  Timed out after {POLL_TIMEOUT}s — check BMC manually.")
    return False


# ── BMC reboot wait + post-flash version verify ──────────────────────

def _wait_for_bmc(client, component, old_version, log):
    """Wait for the BMC to come back online after a firmware flash.

    Just confirms reachability — the active-version comparison
    happens in :func:`verify_active_firmware_version`.
    """
    log(f"  Waiting for BMC to come back online (cap {REBOOT_TIMEOUT}s) ...")
    start = time.time()

    while time.time() - start < REBOOT_TIMEOUT:
        time.sleep(REBOOT_INTERVAL)
        elapsed = int(time.time() - start)
        try:
            client.get(PATH_ROOT)
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            log(f"  {elapsed:4d}s — offline")
            continue
        log(f"  {elapsed:4d}s — back online")
        return True

    log(f"  BMC did not come back within {REBOOT_TIMEOUT}s — check manually.")
    return False


def verify_active_firmware_version(client, component, expected, log):
    """Confirm the running firmware matches ``expected`` after a flash.

    AMI happily reports Task=Completed/OK on a write that
    PrepareFlashArea silently bailed on, or a dual-BIOS write that
    landed in the inactive slot. Without polling the actual
    BiosVersion / FirmwareVersion, those silent-bails read as
    success.

    BIOS lives at ``/redfish/v1/Systems/Self.BiosVersion`` and is
    only visible once the chassis POSTs into the new image —
    expect 3-5 minutes. BMC lives at
    ``/redfish/v1/Managers/Self.FirmwareVersion`` and surfaces as
    soon as the BMC's own reboot completes.

    No expected version supplied = no-op (e.g. update-via-url where
    we can't know the target version up front).
    """
    if not expected:
        log("  Verify  : no expected version provided — skipping")
        return True

    component = component.upper()
    if component == "BIOS":
        cap, path, field = VERIFY_BIOS_TIMEOUT, PATH_SYSTEM, "BiosVersion"
    elif component == "BMC":
        cap, path, field = VERIFY_BMC_TIMEOUT, PATH_MANAGER, "FirmwareVersion"
    else:
        log(f"  Verify  : no canonical version path for {component}; skipping")
        return True

    log(f"  Verify  : polling {path}.{field} for {expected!r} "
        f"(cap {cap}s, every {VERIFY_INTERVAL}s)")
    deadline = time.time() + cap
    seen = "<unknown>"
    while time.time() < deadline:
        try:
            d = client.get(path)
            v = d.get(field)
            if v != seen:
                log(f"  Verify  : {field}={v!r} (target {expected!r})")
                seen = v
            if v == expected:
                log("  Verify  : match — flash confirmed active")
                return True
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout):
            pass
        time.sleep(VERIFY_INTERVAL)
    log(f"  Verify  : FAILED — {field} still {seen!r} after {cap}s "
        f"(expected {expected!r}). AMI may have reported Task=Completed/OK "
        f"on a write that PrepareFlashArea silently bailed on. Check the "
        f"BMC console + the SEL for 'BIOS Switch To N' / 'BIOS Update "
        f"Image-N Success' events.")
    return False


def _read_update_information(client):
    """Read the AMI ``UpdateInformation`` block.

    Best-effort: returns a dict (possibly empty). Used to detect
    silent bails where Task says Completed but
    UpdateInformation.UpdateStatus says "Failed when Preparing
    Flash Device Area." or similar.
    """
    try:
        us = client.get(PATH_UPDATE_SERVICE)
    except requests.HTTPError:
        return {}
    ami = (us.get("Oem") or {}).get("AMIUpdateService") or {}
    info = ami.get("UpdateInformation") or {}
    return info if isinstance(info, dict) else {}


def _get_current_version(client, component):
    """Fetch the current firmware version string for a component."""
    try:
        if component.upper() == "BMC":
            return client.get(PATH_MANAGER).get("FirmwareVersion")
        elif component.upper() == "BIOS":
            return client.get(PATH_SYSTEM).get("BiosVersion")
        else:
            return client.get(f"{PATH_FW_INVENTORY}/{component}").get("Version")
    except requests.HTTPError:
        return None


# ── Shared pre/post hooks wrapper ────────────────────────────────────

def run_with_hooks(method: UpdateMethod, ctx: UpdateContext) -> bool:
    """Wrap a method's apply() with the standard pre/post hooks.

    Pre-apply:
      * Capture the prior firmware version (for the operator log)
      * Log the target version
      * UpdateService readiness gate (HTTP 503 ServiceInUnknownState)
      * Preserve PATCH (BMC only — skipped otherwise)

    Apply: ``method.apply(ctx)``

    Post-apply (only when apply returned True):
      * Cross-check ``Oem.AMIUpdateService.UpdateInformation.UpdateStatus``
        — AMI surfaces silent bails here while Task says Completed
      * For BMC: wait for the BMC to come back over Redfish
      * Verify the active firmware version equals ``expected_version``

    Any of the hooks can short-circuit by returning False. The
    method itself never has to remember to call them, which is the
    whole point of the abstraction.
    """
    ctx.log(f"  Method   : {method.name} — {method.label}")

    # Capture prior version for log + UpdateInformation correlation.
    prior = _get_current_version(ctx.client, ctx.component)
    if prior:
        ctx.log(f"  Current   : {prior}")
    if ctx.expected_version:
        ctx.log(f"  Target    : {ctx.expected_version}")

    # Pre-push gate.
    wait_for_update_service_ready(ctx.client, ctx.log)

    # Preserve.
    apply_preserve(ctx)

    # Method apply.
    ok = method.apply(ctx)
    if not ok:
        return False

    # AMI UpdateInformation sanity check — Task can say Completed
    # while UpdateInformation reports "Failed when Preparing Flash
    # Device Area." on a silent-bail.
    info = _read_update_information(ctx.client)
    upd_status = (info.get("UpdateStatus") or "")
    if upd_status and "Failed" in upd_status:
        ctx.log(f"  UpdateInformation: {upd_status} "
                f"(target={info.get('UpdateTarget')!r}, "
                f"%={info.get('FlashPercentage')!r}) — overriding "
                f"Task=Completed; flash did not take.")
        return False

    # Post-flash version verify. For BMC also wait for reboot first.
    if ctx.component.upper() == "BMC":
        _wait_for_bmc(ctx.client, ctx.component, prior, ctx.log)
    return verify_active_firmware_version(
        ctx.client, ctx.component, ctx.expected_version, ctx.log)


# ── Update response handling ─────────────────────────────────────────

def _handle_update_response(r, ctx: UpdateContext) -> bool:
    """Shared response handler used by methods that POST and then
    poll a TaskService task. Returns True if the task reached
    Completed/OK; the surrounding ``run_with_hooks`` does the
    UpdateInformation + version verify."""
    log = ctx.log
    log(f"  Response: {r.status_code}")

    if r.status_code in (200, 202, 204):
        task_uri = _extract_task_uri(r)
        if task_uri:
            return _poll_task(ctx.client, task_uri, log)
        try:
            log(f"  {json.dumps(r.json(), indent=2)}")
        except (json.JSONDecodeError, ValueError):
            pass
        log("  Accepted. Monitor progress via: lfish tasks -H <host>")
        return True

    # Error
    try:
        log(f"  {json.dumps(r.json(), indent=2)}")
    except (json.JSONDecodeError, ValueError):
        log(f"  {r.text[:500]}")
    return False


# ── Built-in update methods ──────────────────────────────────────────

def _apply_multipart(ctx: UpdateContext) -> bool:
    """Stream the image to MultipartHttpPushUri."""
    log = ctx.log
    if not ctx.filepath or not os.path.isfile(ctx.filepath):
        log(f"  Error: file not found: {ctx.filepath!r}")
        return False

    filesize = os.path.getsize(ctx.filepath)
    log(f"  File      : {ctx.filepath} ({filesize / 1048576:.1f} MB)")
    if ctx.strategy.max_image_size and filesize > ctx.strategy.max_image_size:
        log(f"  Error: image ({filesize} B) exceeds limit "
            f"({ctx.strategy.max_image_size} B)")
        return False

    update_params = {"Targets": [ctx.target_uri]}
    oem_params = {"ImageType": ctx.image_type}
    push_uri = ctx.push_uri

    log(f"  Uploading streaming to {push_uri} (cap {UPLOAD_TIMEOUT}s) ...")
    log(f"    Targets   = {ctx.target_uri}")
    log(f"    ImageType = {ctx.image_type}")

    last_pct = [0]

    def _progress(monitor):
        pct = int(monitor.bytes_read / monitor.len * 100)
        if pct >= last_pct[0] + 10:
            last_pct[0] = pct
            log(f"  Sent {pct}% ({monitor.bytes_read // 1048576}/{monitor.len // 1048576} MB)")

    fw = None
    try:
        fw = open(ctx.filepath, "rb")
        fields = [
            ("UpdateParameters", ("", json.dumps(update_params), "application/json")),
            ("OemParameters",    ("", json.dumps(oem_params),    "application/json")),
            ("UpdateFile",       (os.path.basename(ctx.filepath), fw, "application/octet-stream")),
        ]
        r = ctx.client.post_multipart(push_uri, fields, progress_cb=_progress)
    except (requests.ConnectionError, requests.Timeout):
        if fw:
            fw.close()
        sent_pct = last_pct[0]
        if sent_pct >= 100:
            log("  Upload complete — BMC dropped connection (flashing/rebooting).")
        else:
            log(f"  Connection lost after sending ~{sent_pct}% of the image.")
            log("  BMC may be flashing or the upload may have failed.")
        # In both cases the verify hook decides the actual outcome.
        return True
    except OSError as e:
        if fw:
            fw.close()
        log(f"  I/O error: {e}")
        return False
    finally:
        if fw and not fw.closed:
            fw.close()

    return _handle_update_response(r, ctx)


register_method(UpdateMethod(
    name="multipart",
    label="Redfish MultipartHttpPushUri (DMTF, AMI default)",
    family="out_of_band",
    description=(
        "Stream the firmware blob to the MultipartHttpPushUri "
        "advertised by /redfish/v1/UpdateService — falls back to "
        "/redfish/v1/UpdateService/upload when the BMC doesn't "
        "advertise one. Targets + ImageType come from discovery; "
        "operator overrides via --target-override / "
        "--image-type-override / --push-uri-override."
    ),
    apply=_apply_multipart,
    applicable_components=["BIOS", "BMC", "CPLD"],
    requires="file",
))


def _apply_simple_update(ctx: UpdateContext) -> bool:
    """Trigger Redfish SimpleUpdate — BMC pulls the image from URL."""
    log = ctx.log
    if not ctx.url:
        log("  Error: simple_update needs --url")
        return False

    protocol = ctx.protocol or (urlparse(ctx.url).scheme.upper() or "HTTPS")
    log(f"  ImageURI  : {ctx.url}")
    log(f"  Protocol  : {protocol}")
    log(f"  Action    : {ctx.simple_update_uri}")

    try:
        r = ctx.client.post(ctx.simple_update_uri, {
            "ImageURI": ctx.url,
            "TransferProtocol": protocol,
            "UpdateComponent": ctx.component.upper(),
        })
    except (requests.ConnectionError, requests.Timeout):
        log("  BMC dropped connection (flashing/rebooting).")
        return True

    return _handle_update_response(r, ctx)


register_method(UpdateMethod(
    name="simple_update",
    label="Redfish SimpleUpdate (BMC pulls from URL)",
    family="out_of_band",
    description=(
        "POST to /redfish/v1/UpdateService/Actions/SimpleUpdate "
        "(or whatever Actions['#UpdateService.SimpleUpdate'].target "
        "advertises) so the BMC fetches the image from a URL we "
        "name. Useful when you don't want to stream a 60 MB blob "
        "through your laptop."
    ),
    apply=_apply_simple_update,
    applicable_components=["BIOS", "BMC", "CPLD"],
    requires="url",
))


# ── Plugin loading ───────────────────────────────────────────────────

def load_plugins() -> List[str]:
    """Import every ``.py`` file in ``$LFISH_PLUGINS_DIR``.

    Each plugin module sees ``register_method`` and ``UpdateMethod``
    as builtins of its own namespace (via attribute injection on
    the module before exec) so it can do::

        register_method(UpdateMethod(
            name="asus_oem_bios",
            ...,
            apply=lambda ctx: ...,
        ))

    The function returns the list of plugin file paths it loaded,
    in alphabetical order, for the ``lfish methods`` output to
    show provenance.
    """
    loaded = []
    plugin_dir = os.environ.get("LFISH_PLUGINS_DIR")
    if not plugin_dir or not os.path.isdir(plugin_dir):
        return loaded
    for fname in sorted(os.listdir(plugin_dir)):
        if not fname.endswith(".py") or fname.startswith("_"):
            continue
        path = os.path.join(plugin_dir, fname)
        try:
            spec = importlib.util.spec_from_file_location(
                f"lfish_plugin_{fname[:-3]}", path,
            )
            mod = importlib.util.module_from_spec(spec)
            # Expose the registry hooks before exec'ing the module.
            mod.register_method = register_method
            mod.UpdateMethod = UpdateMethod
            mod.UpdateContext = UpdateContext
            mod.UpdateStrategy = UpdateStrategy
            mod.METHODS = METHODS
            spec.loader.exec_module(mod)
            loaded.append(path)
        except Exception as e:                    # noqa: BLE001
            sys.stderr.write(f"lfish: plugin {path} failed: {e}\n")
    return loaded


# ── Method picker + override parsing ─────────────────────────────────

def pick_method(args) -> UpdateMethod:
    """Pick a method based on --method or auto-detect.

    Auto-detect: ``-f FILE`` → ``multipart``; ``--url URL`` →
    ``simple_update``. Raises ValueError on unknown method.
    """
    if args.method:
        m = METHODS.get(args.method)
        if not m:
            raise ValueError(
                f"unknown method {args.method!r}; "
                f"valid: {sorted(METHODS.keys())}"
            )
        return m
    if args.file:
        return METHODS["multipart"]
    return METHODS["simple_update"]


def parse_overrides(args) -> Dict[str, Any]:
    """Build the overrides dict from CLI flags."""
    ov: Dict[str, Any] = {}
    if args.target_override:
        ov["target_uri"] = args.target_override
    if args.image_type_override:
        ov["image_type"] = args.image_type_override
    if args.push_uri_override:
        ov["push_uri"] = args.push_uri_override
    if args.simple_update_uri_override:
        ov["simple_update_uri"] = args.simple_update_uri_override
    if args.preserve_keys is not None:
        ov["preserve_keys"] = [k.strip() for k in args.preserve_keys.split(",") if k.strip()]
    return ov


# ── Commands ─────────────────────────────────────────────────────────

def cmd_info(client, log):
    """Display current firmware versions and system information."""

    # System
    try:
        s = client.get(PATH_SYSTEM)
        log(f"  Manufacturer : {s.get('Manufacturer', 'N/A')}")
        log(f"  Model        : {s.get('Model', 'N/A')}")
        log(f"  BIOS Version : {s.get('BiosVersion') or 'N/A'}")
        log(f"  Power State  : {s.get('PowerState', 'N/A')}")
    except requests.HTTPError:
        log("  [!] Could not query system info")

    log("")

    # BMC
    try:
        m = client.get(PATH_MANAGER)
        log(f"  BMC Version  : {m.get('FirmwareVersion', 'N/A')}")
        log(f"  BMC Type     : {m.get('ManagerType', 'N/A')}")
    except requests.HTTPError:
        log("  [!] Could not query manager info")

    log("")

    # Firmware inventory
    try:
        inv = client.get(PATH_FW_INVENTORY)
        for member in inv.get("Members", []):
            try:
                fw = client.get(member["@odata.id"])
                name = fw.get("Name", fw.get("Id", "?"))
                ver = fw.get("Version", "N/A")
                tag = " (updateable)" if fw.get("Updateable") else ""
                log(f"  {name:16s} : {ver}{tag}")
            except requests.HTTPError:
                log(f"  {member['@odata.id']:16s} : [error]")
    except requests.HTTPError:
        log("  [!] Could not query firmware inventory")

    # Capabilities + discovered preserve schema.
    try:
        us = client.get(PATH_UPDATE_SERVICE)
        max_mb = us.get("MaxImageSizeBytes", 0) / 1048576
        if max_mb:
            log(f"\n  Max image    : {max_mb:.0f} MB")
        push_uri = us.get("MultipartHttpPushUri") or PATH_DEFAULT_UPLOAD
        log(f"  Push URI     : {push_uri}")
        ami = (us.get("Oem") or {}).get("AMIUpdateService") or {}
        pcfg = ami.get("PreserveConfiguration")
        if isinstance(pcfg, dict):
            log(f"  Preserve keys: {', '.join(sorted(pcfg.keys())) or '(empty)'}")
        info = ami.get("UpdateInformation") or {}
        if info:
            log(f"  Last update  : status={info.get('UpdateStatus')!r} "
                f"target={info.get('UpdateTarget')!r}")
    except requests.HTTPError:
        pass

    try:
        ai = client.get(PATH_SIMPLE_UPDATE_INFO)
        for p in ai.get("Parameters", []):
            if p.get("Name") == "UpdateComponent":
                log(f"  Components   : {', '.join(p.get('AllowableValues', []))}")
    except requests.HTTPError:
        pass


def cmd_tasks(client, log):
    """List active and recent update tasks."""
    members = client.get(PATH_TASKS).get("Members", [])
    if not members:
        log("  No tasks.")
        return

    for member in members:
        try:
            t = client.get(member["@odata.id"])
            pct = t.get("PercentComplete")
            pct_str = f" ({pct}%)" if pct is not None else ""
            log(f"  Task {t.get('Id', '?')}: "
                f"{t.get('TaskState', '?')} / {t.get('TaskStatus', '?')}{pct_str}")
            for msg in t.get("Messages", []):
                log(f"    {msg.get('Message', '')}")
        except requests.HTTPError:
            log(f"  {member['@odata.id']}: [error]")


def cmd_methods(args, log):
    """List registered update methods. Doesn't talk to a BMC."""
    # Load plugins so the listing reflects what an actual update
    # run would see.
    plugin_paths = load_plugins()
    log(f"  Registered methods ({len(METHODS)}):\n")
    for name in sorted(METHODS):
        m = METHODS[name]
        log(f"  • {m.name}")
        log(f"      label      : {m.label}")
        log(f"      family     : {m.family}")
        log(f"      requires   : {m.requires}")
        log(f"      components : {', '.join(m.applicable_components) or '(any)'}")
        for line in (m.description or "").splitlines() or [""]:
            log(f"      desc       : {line.strip()}")
        log("")
    if plugin_paths:
        log("  Plugins loaded from $LFISH_PLUGINS_DIR:")
        for p in plugin_paths:
            log(f"    {p}")
    else:
        env_hint = os.environ.get("LFISH_PLUGINS_DIR", "<unset>")
        log(f"  Plugins: $LFISH_PLUGINS_DIR = {env_hint}")


def cmd_update(client, args, log):
    """Validate inputs, then dispatch the requested method via the
    shared pre/post-hook wrapper."""
    component = args.component.upper()

    # Validate component against what the BMC SimpleUpdateActionInfo
    # advertises — generous: only blocks when the BMC explicitly
    # publishes an AllowableValues list AND our component isn't on
    # it. AMI builds without the schema let everything through here
    # and rely on the FirmwareInventory walk to error cleanly.
    try:
        ai = client.get(PATH_SIMPLE_UPDATE_INFO)
        for p in ai.get("Parameters", []):
            if p.get("Name") == "UpdateComponent":
                allowed = [v.upper() for v in p.get("AllowableValues", [])]
                if allowed and component not in allowed:
                    log(f"  Error: '{component}' is not a valid target.")
                    log(f"  Allowed: {', '.join(allowed)}")
                    return False
    except requests.HTTPError:
        pass

    # Pick the method early so we can validate input-source
    # compatibility before doing any discovery.
    try:
        method = pick_method(args)
    except ValueError as e:
        log(f"  Error: {e}")
        return False
    log(f"  Component : {component}")
    if method.requires == "file" and not args.file:
        log(f"  Error: method {method.name!r} needs -f FILE")
        return False
    if method.requires == "url" and not args.url:
        log(f"  Error: method {method.name!r} needs --url URL")
        return False
    if method.applicable_components \
            and component not in [c.upper() for c in method.applicable_components]:
        log(f"  Error: method {method.name!r} can't update {component!r} "
            f"(applicable: {method.applicable_components})")
        return False

    # Run discovery once; the method consumes the result via the
    # context object plus operator overrides.
    try:
        strategy = discover_strategy(client, component, log)
    except RuntimeError as e:
        log(f"  Error: discovery failed: {e}")
        return False

    ctx = UpdateContext(
        client=client,
        component=component,
        strategy=strategy,
        log=log,
        filepath=args.file,
        url=args.url,
        protocol=args.protocol,
        expected_version=(args.expected_version or "").strip() or None,
        preserve_legacy=bool(args.preserve_legacy),
        no_preserve=bool(args.no_preserve),
        overrides=parse_overrides(args),
    )

    if ctx.overrides:
        log(f"  Overrides : {ctx.overrides}")

    return run_with_hooks(method, ctx)


# ── Per-host dispatch ────────────────────────────────────────────────

COMMANDS = {
    "info":    lambda client, args, log: (cmd_info(client, log), True)[1],
    "tasks":   lambda client, args, log: (cmd_tasks(client, log), True)[1],
    "update":  cmd_update,
}


_print_lock = threading.Lock()


def _make_prefixed_log(host, width):
    """Return a log() that prints each line with a fixed-width host prefix.

    A lock serialises writes so per-line output never interleaves
    between threads.  Multi-line strings get a prefix on every line.
    """
    prefix = f"[{host:<{width}}]"

    def log(text):
        with _print_lock:
            for line in str(text).split("\n"):
                print(f"{prefix} {line}", flush=True)

    return log


# Patterns that extract structured fields from log() lines so the
# dashboard can show "BMC=12.61.39 BIOS=F22" instead of whatever the
# most recent line happened to be.  The order in which keys are added
# is preserved so the dashboard column layout is predictable.
_FIELD_PATTERNS = [
    # info command
    (re.compile(r"^\s*Manufacturer\s*:\s*(.+)$"),  "Vendor"),
    (re.compile(r"^\s*Model\s*:\s*(.+)$"),         "Model"),
    (re.compile(r"^\s*Power State\s*:\s*(.+)$"),   "Power"),
    (re.compile(r"^\s*BMC Version\s*:\s*(.+)$"),   "BMC"),
    (re.compile(r"^\s*BIOS Version\s*:\s*(.+)$"),  "BIOS"),

    # update command
    (re.compile(r"^\s*Method\s*:\s*(\S+)"),        "Method"),
    (re.compile(r"^\s*Component\s*:\s*(.+)$"),     "Comp"),
    (re.compile(r"^\s*Current\s*:\s*(.+)$"),       "From"),
    (re.compile(r"^\s*Target\s*:\s*(.+)$"),        "To"),
    (re.compile(r"^\s*Sent (\d+%).*$"),            "Upload"),
    (re.compile(r"^\s*Response:\s*(\d+)$"),        "HTTP"),
    (re.compile(r"^\s*\[#+\-*\]\s*(\d+%)\s+(\w+)"),"Task"),
    (re.compile(r"^\s*State:\s*(\w+)$"),           "Task"),
    (re.compile(r"^\s*Verify\s*:\s*match.*$"),     "Verified"),
]

_STATUS_LINE_PATTERNS = [
    (re.compile(r"^\s*(Waiting for UpdateService.*)$"),        "ready-gate"),
    (re.compile(r"^\s*(Preserve\s*:.*)$"),                     "preserve"),
    (re.compile(r"^\s*(Uploading.*)$"),                        "uploading"),
    (re.compile(r"^\s*(Tracking task.*)$"),                    "tracking"),
    (re.compile(r"^\s*(Waiting for BMC.*)$"),                  "waiting"),
    (re.compile(r"^\s*(BMC dropped connection.*)$"),           "rebooting"),
    (re.compile(r"^\s*(Verify\s*:\s*polling.*)$"),             "verifying"),
    (re.compile(r"^\s*Verify\s*:\s*match.*$"),                 "verified"),
    (re.compile(r"^\s*Verify\s*:\s*FAILED.*$"),                "verify-fail"),
    (re.compile(r"^\s*Update task reached Completed.*$"),      "task-ok"),
    (re.compile(r"^\s*Update task ended.*$"),                  "task-fail"),
    (re.compile(r"^\s*Cannot connect.*$"),                     "unreachable"),
    (re.compile(r"^\s*Authentication failed.*$"),              "auth failed"),
    (re.compile(r"^\s*Error:.*$"),                             "error"),
]


class Dashboard:
    """Live multi-host status display.

    Each host occupies one line.  Log() output is parsed into
    structured fields (BMC, BIOS, Upload%, …) which accumulate in
    the dashboard, so earlier fields are not overwritten by later
    log lines.  When a host finishes, the line is left in place
    with a ✓ or ✗ marker.

    Once all hosts have completed (or any time the caller invokes
    `dump_all()`), the dashboard is erased and the full per-host
    output is printed in *hostlist order* so reviewing failures
    is straightforward.
    """

    OK_MARK = "✓"
    FAIL_MARK = "✗"
    PENDING_MARK = "·"

    def __init__(self, hosts):
        self.hosts = list(hosts)
        self.width = max(len(h) for h in self.hosts)
        self.fields = {h: {} for h in self.hosts}    # ordered key -> value
        self.status = {h: "" for h in self.hosts}
        self.buffers = {h: [] for h in self.hosts}   # all log lines, for replay
        self.done = {h: None for h in self.hosts}    # None | True | False
        self.dumped = set()                          # hosts whose output has been dumped
        self.lock = threading.Lock()
        self.drawn = 0

    def make_log(self, host):
        """Return a log() callable bound to this host."""
        def log(text):
            text = str(text)
            with self.lock:
                self.buffers[host].append(text)
                for raw_line in text.split("\n"):
                    self._absorb(host, raw_line)
                self._redraw()
        return log

    def finish(self, host, success):
        """Mark a host as complete; just update the dashboard line."""
        with self.lock:
            self.done[host] = bool(success)
            self._redraw()

    def dump_all(self):
        """Erase the dashboard and print every host's full output in
        hostlist order.  Safe to call after all hosts have finished.
        """
        with self.lock:
            self._erase()
            for host in self.hosts:
                self._dump_host_block(host)

    # ── Internal ───────────────────────────────────────────────

    def _absorb(self, host, line):
        """Update fields/status from one log line."""
        # Field extraction
        for pattern, key in _FIELD_PATTERNS:
            m = pattern.match(line)
            if m:
                self.fields[host][key] = m.group(1).strip()
                # Don't return — multiple patterns may match in theory
        # Status phrase
        for pattern, status in _STATUS_LINE_PATTERNS:
            if pattern.match(line):
                self.status[host] = status
                break

    def _mark(self, host):
        if self.done[host] is True:
            return self.OK_MARK
        if self.done[host] is False:
            return self.FAIL_MARK
        return self.PENDING_MARK

    def _format_fields(self, host):
        parts = [f"{k}={v}" for k, v in self.fields[host].items()]
        # Show status phrase as a trailing tag if it's still relevant
        # (i.e. the host is in progress).  Hide it once the host is done
        # since the ✓/✗ marker already conveys that.
        if self.status[host] and self.done[host] is None:
            parts.append(f"({self.status[host]})")
        if not parts:
            return "queued" if self.done[host] is None else ""
        return "  ".join(parts)

    def _term_width(self):
        try:
            return os.get_terminal_size().columns
        except OSError:
            return 120

    def _erase(self):
        if self.drawn:
            sys.stdout.write(f"\x1b[{self.drawn}A\x1b[J")
            self.drawn = 0

    def _redraw(self):
        self._erase()
        term_width = self._term_width()

        for host in self.hosts:
            line = (f"  {self._mark(host)} "
                    f"{host:<{self.width}}  "
                    f"{self._format_fields(host)}")
            if len(line) > term_width:
                line = line[:term_width - 1] + "…"
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
        self.drawn = len(self.hosts)

    def _dump_host_block(self, host):
        """Print one host's full buffered output (caller should have
        already erased the live dashboard)."""
        if host in self.dumped:
            return
        self.dumped.add(host)

        sep = "─" * 60
        mark = self._mark(host)
        print(f"\n{sep}\n  {mark} {host}\n{sep}")
        for chunk in self.buffers[host]:
            for line in chunk.split("\n"):
                print(line)


def run_on_host(host, args, verify_ssl, log):
    """Execute the requested command on one host.

    Returns (host, success).  Output is emitted via the supplied log()
    callable in real time — nothing is buffered.
    """
    client = RedfishClient(host, args.user, args.password, verify_ssl)
    ok, err = client.check_connection()
    if not ok:
        log(f"  {err}")
        return host, False

    handler = COMMANDS.get(args.command)
    success = handler(client, args, log) if handler else False
    return host, success


# ── CLI ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        prog="lfish",
        description="BMC/BIOS firmware updater via Redfish",
        epilog="HOST accepts Slurm-style hostlists, e.g. node[001-008] or 10.0.0.[1-5]",
    )
    p.add_argument("-H", "--host",
                   help="BMC host(s) — single, comma-separated, "
                        "or Slurm hostlist (e.g. node[001-008]). "
                        "Required for every command except `methods`.")
    p.add_argument("-u", "--user", default=DEFAULT_USER,
                   help=f"Redfish username (default: {DEFAULT_USER})")
    p.add_argument("-p", "--password", default=DEFAULT_PASS,
                   help="Redfish password (default: ****)")
    p.add_argument("-k", "--insecure", action="store_true", default=True,
                   help="Skip TLS certificate verification (default)")
    p.add_argument("--secure", action="store_true",
                   help="Enable TLS certificate verification")
    p.add_argument("-w", "--workers", type=int, default=DEFAULT_WORKERS,
                   help=f"Max parallel hosts (default: {DEFAULT_WORKERS})")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Multi-host: stream every log line with a [host] "
                        "prefix instead of the live dashboard")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("info",    help="Show firmware versions and system info")
    sub.add_parser("tasks",   help="List update tasks")
    sub.add_parser("methods", help="List registered update methods + plugin sources")

    up = sub.add_parser("update", help="Update BMC or BIOS firmware")
    up.add_argument("-c", "--component", required=True,
                    help="Component to update (BMC, BIOS, MB_CPLD, BPB_CPLD, ...)")
    src = up.add_mutually_exclusive_group(required=True)
    src.add_argument("-f", "--file", help="Local firmware image to upload")
    src.add_argument("--url", help="Remote firmware image URL (HTTP/HTTPS/FTP)")
    up.add_argument("--protocol", choices=["HTTP", "HTTPS", "FTP"],
                    help="Transfer protocol (auto-detected from URL if omitted)")
    up.add_argument("--method",
                    help="Force a specific update method (default: auto-detect "
                         "from input source). Use `lfish methods` to list.")

    # Strategy overrides — for working around quirky BMCs without
    # editing the discover_strategy() heuristics.
    up.add_argument("--target-override",
                    help="Override Targets URI from discovery "
                         "(e.g. /redfish/v1/UpdateService/FirmwareInventory/BIOS2)")
    up.add_argument("--image-type-override",
                    help="Override OemParameters.ImageType from discovery "
                         "(BIOS / BMC / CPLD).")
    up.add_argument("--push-uri-override",
                    help="Override MultipartHttpPushUri.")
    up.add_argument("--simple-update-uri-override",
                    help="Override SimpleUpdate action target.")
    up.add_argument("--preserve-keys",
                    help="Override discovered preserve key set, "
                         "comma-separated (e.g. 'BMC,Network').")
    up.add_argument("--no-preserve", action="store_true", default=False,
                    help="Skip preserve PATCH entirely.")
    up.add_argument("--preserve-legacy", action="store_true", default=False,
                    help="Use the AMI proprietary /api/maintenance/backup_config "
                         "path instead of the Redfish PATCH path.")
    up.add_argument("--expected-version", default=None,
                    help="Target firmware version string. When supplied, lfish "
                         "polls the BMC's running BiosVersion / FirmwareVersion "
                         "after the flash and reports FAILED if it doesn't match "
                         "— catches AMI's silent-bail where Task says "
                         "Completed/OK but the new image isn't running.")
    # Back-compat: --preserve-config maps to the discovered set.
    up.add_argument("--preserve-config", action="store_true", default=False,
                    help="(back-compat alias for the default behaviour: "
                         "preserve discovered keys for a BMC flash)")

    return p


def main():
    args = build_parser().parse_args()

    # Plugins load every run so the methods registry is current.
    load_plugins()

    verify_ssl = args.secure and not args.insecure

    # `methods` is a metadata command — no BMC needed.
    if args.command == "methods":
        cmd_methods(args, print)
        sys.exit(0)

    if not args.host:
        sys.stderr.write("lfish: -H/--host is required for this command\n")
        sys.exit(2)

    # Expand hostlist
    try:
        hosts = expand_hostlist(args.host)
    except Exception:
        hosts = [args.host]

    # ── Single host: real-time output, no prefix ────────────────
    if len(hosts) == 1:
        _, success = run_on_host(hosts[0], args, verify_ssl, print)
        sys.exit(0 if success else 1)

    # ── Multiple hosts: parallel execution ──────────────────────
    print(f"Targeting {len(hosts)} hosts (workers={args.workers}): "
          f"{', '.join(hosts[:5])}{'...' if len(hosts) > 5 else ''}\n")

    failed = []
    workers = min(args.workers, len(hosts))

    # Live dashboard if stdout is a TTY and -v is not requested,
    # otherwise stream prefixed lines (so piping to a file still
    # yields readable output).
    use_dashboard = sys.stdout.isatty() and not args.verbose
    dashboard = Dashboard(hosts) if use_dashboard else None

    if use_dashboard:
        # Draw initial empty dashboard so threads can update in place
        with dashboard.lock:
            dashboard._redraw()

    width = max(len(h) for h in hosts)

    def make_log(host):
        if dashboard is not None:
            return dashboard.make_log(host)
        return _make_prefixed_log(host, width)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(run_on_host, h, args, verify_ssl, make_log(h)): h
            for h in hosts
        }
        for future in as_completed(futures):
            host, ok = future.result()
            if dashboard is not None:
                dashboard.finish(host, ok)
            if not ok:
                failed.append(host)

    # All hosts done — replace the dashboard with full per-host
    # output in hostlist order.
    if dashboard is not None:
        dashboard.dump_all()

    ok_count = len(hosts) - len(failed)
    print(f"\n{'═' * 60}\n  Done: {ok_count}/{len(hosts)} succeeded")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
