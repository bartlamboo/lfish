#!/usr/bin/env python3
"""lfish - BMC and BIOS firmware updater via Redfish API.

Supports AMI-based BMCs with SimpleUpdate (from URL) and multipart
file upload update methods. Tracks update progress via the Redfish
TaskService.

The -H/--host flag accepts Slurm-style hostlists (e.g. node[001-008])
and runs against all expanded hosts in parallel.

Usage:
    lfish info     -H HOST [-u USER] [-p PASS]
    lfish update   -H HOST [-u USER] [-p PASS] -c COMPONENT -f FILE | --url IMAGE_URL [--protocol PROTO]
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_USER = "admin"
DEFAULT_PASS = "admin"
DEFAULT_WORKERS = 20
POLL_INTERVAL = 10  # seconds between task polls
POLL_TIMEOUT = 1800  # 30 minute max wait


# ── Redfish helpers ──────────────────────────────────────────────────

class RedfishClient:
    """Thin wrapper around a Redfish BMC connection."""

    def __init__(self, host, username, password, verify_ssl=False):
        self.base = f"https://{host}"
        self.auth = (username, password)
        self.verify = verify_ssl
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = self.verify
        self.session.headers.update({"Content-Type": "application/json"})

    def get(self, path):
        url = self.base + path
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path, payload=None, **kwargs):
        url = self.base + path
        r = self.session.post(url, json=payload, timeout=60, **kwargs)
        return r

    def post_multipart(self, path, files):
        url = self.base + path
        # Drop JSON content-type for multipart
        headers = {k: v for k, v in self.session.headers.items()
                   if k.lower() != "content-type"}
        r = self.session.post(url, files=files, headers=headers, timeout=300)
        return r


# ── Commands ─────────────────────────────────────────────────────────

def cmd_info(client, log):
    """Display current firmware versions and system info."""
    log("Querying system information...\n")

    # System info
    try:
        system = client.get("/redfish/v1/Systems/Self")
        log(f"  Manufacturer : {system.get('Manufacturer', 'N/A')}")
        log(f"  Model        : {system.get('Model', 'N/A')}")
        log(f"  BIOS Version : {system.get('BiosVersion') or 'N/A'}")
        power = system.get("PowerState", "N/A")
        log(f"  Power State  : {power}")
    except requests.HTTPError:
        log("  [!] Could not query /redfish/v1/Systems/Self")

    log("")

    # Manager / BMC info
    try:
        manager = client.get("/redfish/v1/Managers/Self")
        log(f"  BMC Version  : {manager.get('FirmwareVersion', 'N/A')}")
        log(f"  BMC Type     : {manager.get('ManagerType', 'N/A')}")
    except requests.HTTPError:
        log("  [!] Could not query /redfish/v1/Managers/Self")

    log("")

    # Firmware inventory
    try:
        inv = client.get("/redfish/v1/UpdateService/FirmwareInventory")
        members = inv.get("Members", [])
        if members:
            log("  Firmware Inventory:")
            for m in members:
                try:
                    fw = client.get(m["@odata.id"])
                    ver = fw.get("Version", "N/A")
                    upd = fw.get("Updateable", False)
                    name = fw.get("Name", fw.get("Id", "?"))
                    flag = " (updateable)" if upd else ""
                    log(f"    {name:16s} : {ver}{flag}")
                except requests.HTTPError:
                    log(f"    {m['@odata.id']:16s} : [error]")
    except requests.HTTPError:
        log("  [!] Could not query firmware inventory")

    # Update service capabilities
    try:
        us = client.get("/redfish/v1/UpdateService")
        max_size = us.get("MaxImageSizeBytes")
        if max_size:
            log(f"\n  Max image size: {max_size / 1048576:.0f} MB")
    except requests.HTTPError:
        pass

    # Supported update components
    try:
        ai = client.get("/redfish/v1/UpdateService/SimpleUpdateActionInfo")
        for p in ai.get("Parameters", []):
            if p.get("Name") == "UpdateComponent":
                vals = p.get("AllowableValues", [])
                log(f"  Update targets: {', '.join(vals)}")
    except requests.HTTPError:
        pass


def cmd_tasks(client, log):
    """List active and recent tasks."""
    tasks = client.get("/redfish/v1/TaskService/Tasks")
    members = tasks.get("Members", [])
    if not members:
        log("No tasks.")
        return
    for m in members:
        try:
            t = client.get(m["@odata.id"])
            tid = t.get("Id", "?")
            state = t.get("TaskState", "?")
            status = t.get("TaskStatus", "?")
            pct = t.get("PercentComplete")
            pct_str = f" ({pct}%)" if pct is not None else ""
            log(f"  Task {tid}: {state} / {status}{pct_str}")
            for msg in t.get("Messages", []):
                log(f"    {msg.get('Message', '')}")
        except requests.HTTPError:
            log(f"  {m['@odata.id']}: [error reading task]")


def poll_task(client, task_uri, log):
    """Poll a task until it completes or times out."""
    log(f"Tracking task: {task_uri}")
    start = time.time()
    last_pct = -1

    while time.time() - start < POLL_TIMEOUT:
        try:
            t = client.get(task_uri)
        except requests.HTTPError as e:
            log(f"  Task endpoint returned {e.response.status_code} — may have completed.")
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
            status = t.get("TaskStatus", "")
            for msg in t.get("Messages", []):
                log(f"  {msg.get('Message', '')}")
            if state == "Completed" and status in ("OK", ""):
                log("  Update completed successfully.")
                return True
            else:
                log(f"  Update ended with state={state}, status={status}")
                return False

        time.sleep(POLL_INTERVAL)

    log(f"  Timed out after {POLL_TIMEOUT}s. Check BMC manually.")
    return False


def _extract_task_uri(response):
    """Extract task URI from the response of an update POST."""
    # Check Location header (most common)
    loc = response.headers.get("Location")
    if loc:
        parsed = urlparse(loc)
        return parsed.path

    # Check response body
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        return None

    # Direct task ID
    if body.get("@odata.type", "").startswith("#Task."):
        return body.get("@odata.id")

    # Nested in error / message
    for msg in body.get("Messages", body.get("error", {}).get("@Message.ExtendedInfo", [])):
        args = msg.get("MessageArgs", [])
        for arg in args:
            if "/TaskService/Tasks/" in str(arg):
                return arg

    return None


def update_via_file(client, component, filepath, log):
    """Upload a firmware image file via multipart HTTP push."""
    if not os.path.isfile(filepath):
        log(f"Error: file not found: {filepath}")
        return False

    filesize = os.path.getsize(filepath)
    log(f"  File: {filepath} ({filesize / 1048576:.1f} MB)")

    # Check max image size
    try:
        us = client.get("/redfish/v1/UpdateService")
        max_size = us.get("MaxImageSizeBytes")
        if max_size and filesize > max_size:
            log(f"Error: file ({filesize} bytes) exceeds max allowed ({max_size} bytes)")
            return False
    except requests.HTTPError:
        pass

    upload_uri = "/redfish/v1/UpdateService/upload"

    # Build multipart payload with UpdateParameters + firmware file
    update_params = {
        "Targets": [f"/redfish/v1/UpdateService/FirmwareInventory/{component}"],
        "@Redfish.OperationApplyTime": "Immediate"
    }

    log(f"  Uploading to {upload_uri} ...")
    with open(filepath, "rb") as fw:
        files = [
            ("UpdateParameters", (None, json.dumps(update_params), "application/json")),
            ("UpdateFile", (os.path.basename(filepath), fw, "application/octet-stream")),
        ]
        r = client.post_multipart(upload_uri, files=files)

    log(f"  Response: {r.status_code}")
    if r.status_code in (200, 202, 204):
        task_uri = _extract_task_uri(r)
        if task_uri:
            return poll_task(client, task_uri, log)
        # Some BMCs return 200 with inline progress via AMI OEM
        try:
            body = r.json()
            log(f"  {json.dumps(body, indent=2)}")
        except (json.JSONDecodeError, ValueError):
            pass
        log("  Upload accepted. Monitor progress via: lfish tasks -H <host>")
        return True
    else:
        try:
            log(f"  {json.dumps(r.json(), indent=2)}")
        except (json.JSONDecodeError, ValueError):
            log(f"  {r.text[:500]}")
        return False


def update_via_url(client, component, image_url, protocol, log):
    """Trigger a firmware update via SimpleUpdate (remote URL)."""
    if not protocol:
        parsed = urlparse(image_url)
        protocol = parsed.scheme.upper() or "HTTPS"

    payload = {
        "ImageURI": image_url,
        "TransferProtocol": protocol,
        "UpdateComponent": component,
    }

    action_uri = "/redfish/v1/UpdateService/Actions/SimpleUpdate"
    log(f"  Sending SimpleUpdate to {action_uri}")
    log(f"  ImageURI  : {image_url}")
    log(f"  Protocol  : {protocol}")
    log(f"  Component : {component}")

    r = client.post(action_uri, payload=payload)
    log(f"  Response: {r.status_code}")

    if r.status_code in (200, 202, 204):
        task_uri = _extract_task_uri(r)
        if task_uri:
            return poll_task(client, task_uri, log)
        try:
            body = r.json()
            log(f"  {json.dumps(body, indent=2)}")
        except (json.JSONDecodeError, ValueError):
            pass
        log("  Update initiated. Monitor progress via: lfish tasks -H <host>")
        return True
    else:
        try:
            log(f"  {json.dumps(r.json(), indent=2)}")
        except (json.JSONDecodeError, ValueError):
            log(f"  {r.text[:500]}")
        return False


def cmd_update(client, args, log):
    """Dispatch a firmware update."""
    component = args.component.upper()

    # Validate component name against what the BMC supports
    try:
        ai = client.get("/redfish/v1/UpdateService/SimpleUpdateActionInfo")
        for p in ai.get("Parameters", []):
            if p.get("Name") == "UpdateComponent":
                allowed = [v.upper() for v in p.get("AllowableValues", [])]
                if allowed and component not in allowed:
                    log(f"Error: '{component}' is not a valid update target.")
                    log(f"  Allowed: {', '.join(allowed)}")
                    return False
    except requests.HTTPError:
        pass  # proceed anyway

    log(f"Starting {component} firmware update...")

    if args.file:
        return update_via_file(client, component, args.file, log)
    elif args.url:
        return update_via_url(client, component, args.url, args.protocol, log)
    else:
        log("Error: specify --file or --url")
        return False


# ── Per-host runner ──────────────────────────────────────────────────

def run_on_host(host, args, verify_ssl):
    """Run the requested command against a single host.

    Returns (host, success: bool, output: list[str]).
    """
    lines = []
    log = lines.append

    client = RedfishClient(host, args.user, args.password,
                           verify_ssl=verify_ssl)

    # Connectivity check
    try:
        client.get("/redfish/v1/")
    except requests.ConnectionError:
        log(f"Error: cannot connect to {host}")
        return host, False, lines
    except requests.HTTPError as e:
        if e.response.status_code == 401:
            log("Error: authentication failed (HTTP 401)")
        else:
            log(f"Error: {e}")
        return host, False, lines

    if args.command == "info":
        cmd_info(client, log)
        return host, True, lines
    elif args.command == "tasks":
        cmd_tasks(client, log)
        return host, True, lines
    elif args.command == "update":
        ok = cmd_update(client, args, log)
        return host, ok, lines

    return host, False, lines


# ── CLI ──────────────────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        prog="lfish",
        description="BMC/BIOS firmware updater via Redfish",
        epilog="HOST accepts Slurm-style hostlists, e.g. node[001-008] or 10.0.0.[1-5]",
    )
    parser.add_argument("-H", "--host", required=True,
                        help="BMC host(s) — single host, comma-separated, "
                             "or Slurm hostlist (e.g. node[001-008])")
    parser.add_argument("-u", "--user", default=DEFAULT_USER,
                        help=f"Redfish username (default: {DEFAULT_USER})")
    parser.add_argument("-p", "--password", default=DEFAULT_PASS,
                        help="Redfish password (default: ****)")
    parser.add_argument("-k", "--insecure", action="store_true", default=True,
                        help="Skip TLS certificate verification (default)")
    parser.add_argument("--secure", action="store_true",
                        help="Enable TLS certificate verification")
    parser.add_argument("-w", "--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Max parallel hosts (default: {DEFAULT_WORKERS})")

    sub = parser.add_subparsers(dest="command", required=True)

    # info
    sub.add_parser("info", help="Show firmware versions and system info")

    # tasks
    sub.add_parser("tasks", help="List update tasks")

    # update
    up = sub.add_parser("update", help="Update BMC or BIOS firmware")
    up.add_argument("-c", "--component", required=True,
                    help="Component to update (e.g. BMC, BIOS, MB_CPLD, BPB_CPLD)")
    source = up.add_mutually_exclusive_group(required=True)
    source.add_argument("-f", "--file",
                        help="Local firmware image file to upload")
    source.add_argument("--url",
                        help="Remote firmware image URL (HTTP/HTTPS/FTP)")
    up.add_argument("--protocol", choices=["HTTP", "HTTPS", "FTP"],
                    help="Transfer protocol (auto-detected from URL if omitted)")

    return parser


def print_host_output(host, lines, multi):
    """Print output for a host, adding a header when running multi-host."""
    if multi:
        sep = "─" * 60
        print(f"\n{sep}")
        print(f"  {host}")
        print(sep)
    for line in lines:
        print(line)


def main():
    parser = build_parser()
    args = parser.parse_args()
    verify_ssl = args.secure and not args.insecure

    # Expand hostlist
    try:
        hosts = expand_hostlist(args.host)
    except Exception:
        # Fall back to treating it as a single host
        hosts = [args.host]

    multi = len(hosts) > 1
    if multi:
        print(f"Targeting {len(hosts)} hosts (workers={args.workers}): "
              f"{', '.join(hosts[:5])}{'...' if len(hosts) > 5 else ''}")

    if not multi:
        # Single host — run directly, no threading overhead
        host, ok, lines = run_on_host(hosts[0], args, verify_ssl)
        print_host_output(host, lines, multi=False)
        sys.exit(0 if ok else 1)

    # Multi-host — parallel execution
    failed = []
    workers = min(args.workers, len(hosts))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_on_host, h, args, verify_ssl): h
                   for h in hosts}
        for future in as_completed(futures):
            host, ok, lines = future.result()
            print_host_output(host, lines, multi=True)
            if not ok:
                failed.append(host)

    # Summary
    print(f"\n{'═' * 60}")
    ok_count = len(hosts) - len(failed)
    print(f"  Done: {ok_count}/{len(hosts)} succeeded")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
