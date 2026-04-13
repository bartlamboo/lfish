#!/usr/bin/env python3
"""lfish - BMC and BIOS firmware updater via Redfish API.

Supports AMI-based BMCs with SimpleUpdate (from URL) and multipart
file upload update methods.  Tracks update progress via the Redfish
TaskService.

The -H/--host flag accepts Slurm-style hostlists (e.g. node[001-008])
and runs against all expanded hosts in parallel.

Usage:
    lfish info     -H HOST [-u USER] [-p PASS]
    lfish update   -H HOST [-u USER] [-p PASS] -c COMPONENT -f FILE | --url URL [--protocol PROTO] [--preserve-config]
    lfish tasks    -H HOST [-u USER] [-p PASS]
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

PRESERVE_KEYS = ["snmp", "kvm", "network", "ipmi", "ntp", "authentication", "syslog"]

# Redfish paths
PATH_ROOT = "/redfish/v1/"
PATH_SYSTEM = "/redfish/v1/Systems/Self"
PATH_MANAGER = "/redfish/v1/Managers/Self"
PATH_UPDATE_SERVICE = "/redfish/v1/UpdateService"
PATH_FW_INVENTORY = "/redfish/v1/UpdateService/FirmwareInventory"
PATH_SIMPLE_UPDATE = "/redfish/v1/UpdateService/Actions/SimpleUpdate"
PATH_SIMPLE_UPDATE_INFO = "/redfish/v1/UpdateService/SimpleUpdateActionInfo"
PATH_UPLOAD = "/redfish/v1/UpdateService/upload"
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


# ── AMI maintenance API (non-Redfish) ────────────────────────────────

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


def set_preserve_config(client, log):
    """Enable config preservation for the next firmware flash.

    Uses the AMI proprietary REST API (/api/maintenance/backup_config),
    which is separate from Redfish.  Must be called *before* the update.
    """
    log("  Setting preserve configuration via BMC maintenance API ...")

    try:
        api = _ami_api_login(client.base, *client.auth, client.verify)
    except (requests.ConnectionError, requests.Timeout) as e:
        log(f"  [!] Cannot reach BMC maintenance API: {e}")
        return False
    except (RuntimeError, json.JSONDecodeError, ValueError) as e:
        log(f"  [!] {e}")
        return False

    try:
        r = api.put(f"{client.base}/api/maintenance/backup_config",
                    data=json.dumps({k: 1 for k in PRESERVE_KEYS}),
                    headers={"Content-Type": "application/json"},
                    timeout=15)
    except (requests.ConnectionError, requests.Timeout) as e:
        log(f"  [!] Failed to set backup_config: {e}")
        return False

    if r.status_code != 200:
        log(f"  [!] backup_config PUT failed (HTTP {r.status_code}): {r.text[:300]}")
        return False

    try:
        result = r.json()
        enabled = [k for k in PRESERVE_KEYS if result.get(k) == 1]
        log(f"  Preserve enabled: {', '.join(enabled)}")
    except (json.JSONDecodeError, ValueError):
        log("  Preserve flags set (could not parse response)")

    return True


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


def _poll_task(client, task_uri, log):
    """Poll a task until it reaches a terminal state or times out."""
    log(f"  Tracking task: {task_uri}")
    start = time.time()
    last_pct = -1

    while time.time() - start < POLL_TIMEOUT:
        try:
            t = client.get(task_uri)
        except requests.HTTPError as e:
            # Task endpoint may vanish after completion on some BMCs
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
                log("  Update completed successfully.")
                return True
            log(f"  Update ended: state={state}, status={t.get('TaskStatus', '')}")
            return False

        time.sleep(POLL_INTERVAL)

    log(f"  Timed out after {POLL_TIMEOUT}s — check BMC manually.")
    return False


# ── BMC reboot wait ──────────────────────────────────────────────────

def _wait_for_bmc(client, component, old_version, log):
    """Wait for the BMC to come back online after a firmware flash.

    Polls the Redfish root endpoint until it responds, then compares
    the firmware version to confirm the update landed.
    """
    log(f"  Waiting for BMC to come back online (timeout: {REBOOT_TIMEOUT}s) ...")
    start = time.time()

    while time.time() - start < REBOOT_TIMEOUT:
        time.sleep(REBOOT_INTERVAL)
        elapsed = int(time.time() - start)
        try:
            client.get(PATH_ROOT)
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            log(f"  {elapsed:4d}s — offline")
            continue

        # BMC is back — check firmware version
        log(f"  {elapsed:4d}s — back online")
        new_version = None
        try:
            if component == "BMC":
                new_version = client.get(PATH_MANAGER).get("FirmwareVersion")
            elif component == "BIOS":
                new_version = client.get(PATH_SYSTEM).get("BiosVersion")
            else:
                inv = client.get(f"{PATH_FW_INVENTORY}/{component}")
                new_version = inv.get("Version")
        except requests.HTTPError:
            pass

        if old_version and new_version and new_version != old_version:
            log(f"  Firmware updated: {old_version} -> {new_version}")
        elif new_version:
            log(f"  Firmware version: {new_version}")
        else:
            log(f"  BMC is back (could not read version)")
        return True

    log(f"  BMC did not come back within {REBOOT_TIMEOUT}s — check manually.")
    return False


def _get_current_version(client, component):
    """Fetch the current firmware version string for a component."""
    try:
        if component == "BMC":
            return client.get(PATH_MANAGER).get("FirmwareVersion")
        elif component == "BIOS":
            return client.get(PATH_SYSTEM).get("BiosVersion")
        else:
            return client.get(f"{PATH_FW_INVENTORY}/{component}").get("Version")
    except requests.HTTPError:
        return None


# ── Update response handling ─────────────────────────────────────────

def _handle_update_response(r, client, log):
    """Shared response handler for both update methods.

    Returns True/False indicating success.
    """
    log(f"  Response: {r.status_code}")

    if r.status_code in (200, 202, 204):
        task_uri = _extract_task_uri(r)
        if task_uri:
            return _poll_task(client, task_uri, log)
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


# ── Update methods ───────────────────────────────────────────────────

def _update_via_file(client, component, filepath, log):
    """Upload a firmware image via multipart HTTP push."""
    if not os.path.isfile(filepath):
        log(f"  Error: file not found: {filepath}")
        return False

    filesize = os.path.getsize(filepath)
    log(f"  File      : {filepath} ({filesize / 1048576:.1f} MB)")

    # Validate against max image size
    try:
        max_bytes = client.get(PATH_UPDATE_SERVICE).get("MaxImageSizeBytes")
        if max_bytes and filesize > max_bytes:
            log(f"  Error: image ({filesize} B) exceeds limit ({max_bytes} B)")
            return False
    except requests.HTTPError:
        pass

    # Capture version before flashing so we can compare afterwards
    old_version = _get_current_version(client, component)
    if old_version:
        log(f"  Current   : {old_version}")

    update_params = {
        "Targets": [f"{PATH_FW_INVENTORY}/{component}"],
    }
    oem_params = {"ImageType": component}

    # Track upload progress
    last_pct = [0]

    def _progress(monitor):
        pct = int(monitor.bytes_read / monitor.len * 100)
        if pct >= last_pct[0] + 10:
            last_pct[0] = pct
            log(f"  Sent {pct}% ({monitor.bytes_read // 1048576}/{monitor.len // 1048576} MB)")

    log(f"  Uploading (streaming, timeout: {UPLOAD_TIMEOUT}s) ...")
    try:
        fw = open(filepath, "rb")
        fields = [
            ("UpdateParameters", ("", json.dumps(update_params), "application/json")),
            ("OemParameters",    ("", json.dumps(oem_params),    "application/json")),
            ("UpdateFile",       (os.path.basename(filepath), fw, "application/octet-stream")),
        ]
        r = client.post_multipart(PATH_UPLOAD, fields, progress_cb=_progress)
        fw.close()
    except (requests.ConnectionError, requests.Timeout):
        fw.close()
        sent_pct = last_pct[0]
        if sent_pct >= 100:
            # Full image was sent — BMC dropped connection to flash/reboot
            log("  Upload complete — BMC dropped connection (flashing/rebooting).")
            return _wait_for_bmc(client, component, old_version, log)
        else:
            # Partial upload — BMC may still have enough or may have failed
            log(f"  Connection lost after sending ~{sent_pct}% of the image.")
            log("  BMC may be flashing or the upload may have failed.")
            return _wait_for_bmc(client, component, old_version, log)
    except OSError as e:
        fw.close()
        log(f"  I/O error: {e}")
        return False

    return _handle_update_response(r, client, log)


def _update_via_url(client, component, image_url, protocol, log):
    """Trigger a firmware update via Redfish SimpleUpdate (remote URL)."""
    if not protocol:
        protocol = urlparse(image_url).scheme.upper() or "HTTPS"

    old_version = _get_current_version(client, component)
    if old_version:
        log(f"  Current   : {old_version}")

    log(f"  ImageURI  : {image_url}")
    log(f"  Protocol  : {protocol}")

    try:
        r = client.post(PATH_SIMPLE_UPDATE, {
            "ImageURI": image_url,
            "TransferProtocol": protocol,
            "UpdateComponent": component,
        })
    except (requests.ConnectionError, requests.Timeout):
        log("  BMC dropped connection (flashing/rebooting).")
        return _wait_for_bmc(client, component, old_version, log)

    return _handle_update_response(r, client, log)


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

    # Capabilities
    try:
        us = client.get(PATH_UPDATE_SERVICE)
        max_mb = us.get("MaxImageSizeBytes", 0) / 1048576
        if max_mb:
            log(f"\n  Max image    : {max_mb:.0f} MB")
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


def cmd_update(client, args, log):
    """Validate inputs, optionally set preserve-config, then flash."""
    component = args.component.upper()

    # Validate component against what the BMC supports
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

    log(f"  Component : {component}")

    # Preserve configuration (must happen before flashing)
    if args.preserve_config:
        if not set_preserve_config(client, log):
            log("  Error: could not set preserve config — aborting.")
            return False

    if args.file:
        return _update_via_file(client, component, args.file, log)
    return _update_via_url(client, component, args.url, args.protocol, log)


# ── Per-host dispatch ────────────────────────────────────────────────

COMMANDS = {
    "info":   lambda client, args, log: (cmd_info(client, log), True)[1],
    "tasks":  lambda client, args, log: (cmd_tasks(client, log), True)[1],
    "update": cmd_update,
}


def run_on_host(host, args, verify_ssl):
    """Execute the requested command on one host.

    Returns (host, success, output_lines).
    """
    lines = []
    log = lines.append

    client = RedfishClient(host, args.user, args.password, verify_ssl)
    ok, err = client.check_connection()
    if not ok:
        log(f"  {err}")
        return host, False, lines

    handler = COMMANDS.get(args.command)
    success = handler(client, args, log) if handler else False
    return host, success, lines


# ── CLI ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        prog="lfish",
        description="BMC/BIOS firmware updater via Redfish",
        epilog="HOST accepts Slurm-style hostlists, e.g. node[001-008] or 10.0.0.[1-5]",
    )
    p.add_argument("-H", "--host", required=True,
                   help="BMC host(s) — single, comma-separated, "
                        "or Slurm hostlist (e.g. node[001-008])")
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

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("info",  help="Show firmware versions and system info")
    sub.add_parser("tasks", help="List update tasks")

    up = sub.add_parser("update", help="Update BMC or BIOS firmware")
    up.add_argument("-c", "--component", required=True,
                    help="Component to update (BMC, BIOS, MB_CPLD, BPB_CPLD, ...)")
    src = up.add_mutually_exclusive_group(required=True)
    src.add_argument("-f", "--file", help="Local firmware image to upload")
    src.add_argument("--url", help="Remote firmware image URL (HTTP/HTTPS/FTP)")
    up.add_argument("--protocol", choices=["HTTP", "HTTPS", "FTP"],
                    help="Transfer protocol (auto-detected from URL if omitted)")
    up.add_argument("--preserve-config", action="store_true", default=False,
                    help="Preserve BMC settings (network, users, IPMI, ...) during update")

    return p


def _print_host_block(host, lines, multi):
    """Print buffered output for a host, with a header in multi mode."""
    if multi:
        sep = "─" * 60
        print(f"\n{sep}\n  {host}\n{sep}")
    for line in lines:
        print(line)


def main():
    args = build_parser().parse_args()
    verify_ssl = args.secure and not args.insecure

    # Expand hostlist
    try:
        hosts = expand_hostlist(args.host)
    except Exception:
        hosts = [args.host]

    # ── Single host: run directly with real-time output ─────────
    if len(hosts) == 1:
        client = RedfishClient(hosts[0], args.user, args.password, verify_ssl)
        ok, err = client.check_connection()
        if not ok:
            print(f"  {err}")
            sys.exit(1)
        handler = COMMANDS.get(args.command)
        success = handler(client, args, print) if handler else False
        sys.exit(0 if success else 1)

    # ── Multiple hosts: parallel execution ───────────────────────
    print(f"Targeting {len(hosts)} hosts (workers={args.workers}): "
          f"{', '.join(hosts[:5])}{'...' if len(hosts) > 5 else ''}")

    failed = []
    workers = min(args.workers, len(hosts))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_on_host, h, args, verify_ssl): h
                   for h in hosts}
        for future in as_completed(futures):
            host, ok, lines = future.result()
            _print_host_block(host, lines, multi=True)
            if not ok:
                failed.append(host)

    ok_count = len(hosts) - len(failed)
    print(f"\n{'═' * 60}\n  Done: {ok_count}/{len(hosts)} succeeded")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
