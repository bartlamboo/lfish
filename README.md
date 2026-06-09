# lfish

BMC and BIOS firmware updater built on the Redfish API.

Targets AMI MegaRAC SP-X BMCs in particular — the discovery and
verification logic is built around the quirks of that family — but
the plugin-friendly registry lets you slot in an OEM-specific
update method for any BMC the rest of the codebase can talk to.

Designed for fleet work: the `-H` flag takes Slurm-style hostlists
and runs every host in parallel under a live dashboard.

## Install

```bash
pip3 install -r requirements.txt
```

## Quick start

```bash
# Show firmware versions
./lfish.py -H 192.168.1.10 -u admin -p secret info

# List registered update methods (no BMC needed)
./lfish.py methods

# Update BMC firmware from a local file (auto-picks the `multipart` method)
./lfish.py -H 192.168.1.10 -u admin -p secret update -c BMC -f firmware.bin

# Same, but verify the resulting active version
./lfish.py -H 192.168.1.10 -u admin -p secret update \
    -c BMC -f firmware.bin --expected-version 12.61.39

# Update across a whole rack in parallel, preserving BMC config
./lfish.py -H 'bmc[001-032]' -u admin -p secret update \
    -c BMC -f firmware.bin --preserve-config

# Have the BMC pull the image itself (auto-picks `simple_update`)
./lfish.py -H 192.168.1.10 -u admin -p secret update \
    -c BIOS --url https://files.example.com/bios_v2.0.bin
```

## Commands

| Command   | Description                                                |
|-----------|------------------------------------------------------------|
| `info`    | Show firmware versions, system info, BMC capabilities      |
| `update`  | Update BMC / BIOS / CPLD firmware                          |
| `tasks`   | List active/recent Redfish update tasks                    |
| `methods` | List registered update methods + loaded plugin sources     |

## Global options

| Flag               | Description                                                                  |
|--------------------|------------------------------------------------------------------------------|
| `-H`, `--host`     | BMC host(s) — single, comma-separated, or hostlist (e.g. `node[001-008]`)    |
| `-u`, `--user`     | Redfish username (default: `admin`)                                          |
| `-p`, `--password` | Redfish password (default: `admin`)                                          |
| `-w`, `--workers`  | Max parallel hosts (default: `20`)                                           |
| `-v`, `--verbose`  | Multi-host: stream every log line with `[host]` prefix instead of dashboard  |
| `-k`, `--insecure` | Skip TLS verification (default)                                              |
| `--secure`         | Enable TLS verification                                                      |

## Update options

| Flag                            | Description                                                                                          |
|---------------------------------|------------------------------------------------------------------------------------------------------|
| `-c`, `--component`             | Target component (`BMC`, `BIOS`, `MB_CPLD`, `BPB_CPLD`, …)                                           |
| `-f`, `--file`                  | Local firmware image to upload (auto-picks `multipart`)                                              |
| `--url`                         | Remote firmware image URL (auto-picks `simple_update`)                                               |
| `--protocol`                    | Override transfer protocol for `--url` (auto-detected from scheme)                                   |
| `--method NAME`                 | Force a specific method by name; defaults to auto-detect from input source                           |
| `--expected-version V`          | Target version string — post-flash verify reports FAILED if the running version doesn't match        |
| `--preserve-config`             | Preserve BMC settings during a BMC flash (back-compat alias for the default discovered key set)      |
| `--no-preserve`                 | Skip the preserve PATCH entirely                                                                     |
| `--preserve-keys K1,K2,…`       | Override the discovered preserve key set (comma-separated)                                           |
| `--preserve-legacy`             | Use the AMI proprietary `/api/maintenance/backup_config` path instead of the Redfish PATCH path      |
| `--target-override URI`         | Override the `UpdateParameters.Targets` URI (e.g. `…/FirmwareInventory/BIOS2`)                       |
| `--image-type-override TYPE`    | Override `OemParameters.ImageType` (must be in AMI's closed vocab: `BIOS` / `BMC` / `CPLD`)          |
| `--push-uri-override URI`       | Override the `MultipartHttpPushUri` discovered from the UpdateService                                |
| `--simple-update-uri-override` | Override the `Actions["#UpdateService.SimpleUpdate"].target` URI                                     |

## Architecture

Each update method (multipart push, SimpleUpdate pull, an OEM
flavour, a future in-band path, …) is a tiny `UpdateMethod` entry
registered into the `METHODS` dict. A method's only required piece
of work is its `apply(ctx)` callable; everything else — preserve
PATCH, ready-gate, AMI quirks check, version verify — lives in the
shared `run_with_hooks()` wrapper. Adding a method is a one-place
edit: write the callable, `register_method(UpdateMethod(...))`.

Two built-in methods ship:

| Name            | Requires | Description                                                          |
|-----------------|----------|----------------------------------------------------------------------|
| `multipart`     | `-f`     | Stream firmware to `MultipartHttpPushUri` (DMTF, AMI default)        |
| `simple_update` | `--url`  | POST to `Actions/#UpdateService.SimpleUpdate` — BMC pulls the image  |

Per-BMC parameters are *discovered* at flash time and held in an
`UpdateStrategy`:

* `Targets` — built from the live `FirmwareInventory`. Dual-bank
  BMC chassis (`BMCImage1` / `BMCImage2`) and dual-BIOS chassis
  (`BIOS` / `BIOS2`) are detected; the BMC path picks the active
  bank (overwriting it triggers the AMI swap-and-reboot flow) and
  the BIOS path picks the *boot* slot (identified by the absence
  of a populated `Version` field — AMI populates `Version` only
  on the slot it last wrote, which is the inactive bank).
* `OemParameters.ImageType` — pinned to AMI's closed vocabulary
  (`BIOS` / `BMC` / `CPLD`). Splitting it from `Targets` avoids
  HTTP 400 `InvalidVariableValue` when `Targets` carries a slot
  suffix.
* `MultipartHttpPushUri` — read from `/redfish/v1/UpdateService`,
  not hardcoded.
* `PreserveConfiguration` keys — read from
  `Oem.AMIUpdateService.PreserveConfiguration` so a key the
  validator rejects can't reach the PATCH.

`UpdateContext` is what each method's `apply()` receives. It bundles
the client, component, discovered strategy, log function, input
source (file / URL / protocol), and an `overrides` dict populated
from the CLI. The convenience properties on the context
(`target_uri`, `image_type`, `push_uri`, `simple_update_uri`,
`preserve_keys`) all honour overrides, so a method body never has
to ask "did the operator override this?" — it just reads the
property.

### What `run_with_hooks` does around `apply()`

Pre-apply:

1. Capture the prior firmware version (for the operator log).
2. Wait for `UpdateService` ready — polls until `ServiceEnabled=True`
   with no `ServiceInUnknownState` marker (AMI returns HTTP 503
   for 30–120s after a BMC self-reset; firing the next push too
   soon eats it).
3. PATCH `PreserveConfiguration` (BMC component only — no-op for
   BIOS / CPLD).

Apply: `method.apply(ctx)`.

Post-apply (only when apply returned True):

4. Cross-check `Oem.AMIUpdateService.UpdateInformation.UpdateStatus`
   — AMI surfaces silent bails here while Task says Completed.
5. For BMC: wait for the BMC to come back over Redfish.
6. Verify the *active* firmware version: BIOS via
   `Systems/Self.BiosVersion`, BMC via
   `Managers/Self.FirmwareVersion`. Without this check AMI's
   "completed but didn't take" failures look like success.

## Plugins

Drop `.py` files into the directory pointed at by the
`LFISH_PLUGINS_DIR` env var. Every plugin module sees these names
in its own namespace before it runs:

* `register_method`
* `UpdateMethod`
* `UpdateContext`
* `UpdateStrategy`
* `METHODS`

so a plugin doesn't need to import anything from lfish itself.
Duplicate names overwrite by design — a plugin can monkey-patch a
built-in's behaviour without renaming.

`lfish methods` lists every registered method and prints the
plugin paths it loaded, so you can confirm a plugin was picked up
without firing an actual flash.

### Example plugin

A ready-to-copy template ships in `plugins/examples/asus_oem_bios.py`.
Drop a copy (with the OEM action URI swapped out for your BMC's
actual endpoint) into `$LFISH_PLUGINS_DIR`. The condensed version:

```python
"""OEM BIOS update for an ASUS BMC that ships its own action URI.

Talks to a vendor-specific action endpoint instead of the standard
multipart push — registered as `asus_oem_bios` so an operator can
opt in with `--method asus_oem_bios`.
"""


def apply(ctx):
    # ctx.target_uri honours --target-override; same for the others
    ctx.log(f"  ASUS OEM BIOS flash → {ctx.target_uri}")
    r = ctx.client.post(
        "/redfish/v1/UpdateService/Oem/Asus/Actions/Asus.UpdateBios",
        {"Image": ctx.filepath, "Component": ctx.component},
    )
    if not r.ok:
        ctx.log(f"  HTTP {r.status_code}: {r.text[:300]}")
        return False
    return True


register_method(UpdateMethod(
    name="asus_oem_bios",
    label="ASUS OEM BIOS action",
    family="out_of_band",
    description="Vendor-specific BIOS update via the ASUS OEM action.",
    apply=apply,
    applicable_components=["BIOS"],
    requires="file",
))
```

Then:

```bash
LFISH_PLUGINS_DIR=~/lfish-plugins ./lfish.py methods
LFISH_PLUGINS_DIR=~/lfish-plugins ./lfish.py \
    -H bmc01 -u admin -p secret \
    update -c BIOS -f bios.bin --method asus_oem_bios
```

## Why all the quirks?

The discovery + verify hooks landed after a fleet of failures on
Gigabyte AMI hardware that all looked like "Task = Completed/OK"
but where the new image either lived in the wrong slot, never made
it past `PrepareFlashArea`, or simply wasn't the one the chassis
booted. Each defence point in `run_with_hooks` exists because the
naive flow missed one of those:

* **Dual-BIOS chassis (R263-ZG0)** — without slot detection a write
  goes to whichever member the BMC happens to list first, which
  on these boards is the *inactive* slot. The flash succeeds; the
  chassis boots the old BIOS forever. Picking the boot slot via
  the "Version field absence" trick fixes it.
* **Dual-bank BMC (G293 / R263)** — without checking
  `DualImageConfigurations.ActiveImage` you can write the inactive
  bank, which the BMC will never swap to.
* **`ImageType=BIOS2`** — AMI validates `OemParameters.ImageType`
  against a closed list. Putting the slot-suffixed name there
  returns HTTP 400 `InvalidVariableValue`; splitting `Targets`
  from `ImageType` is what lets the slot suffix live on `Targets`
  alone.
* **HTTP 503 ServiceInUnknownState** — happens for 30–120s after a
  BMC self-reset (which `multipart` triggers as it finishes). The
  next push fired too soon gets swallowed. The pre-push gate
  waits it out.
* **`UpdateStatus` overrides Task** — AMI's
  `Oem.AMIUpdateService.UpdateInformation.UpdateStatus` will read
  `Failed when Preparing Flash Device Area.` while the Redfish
  Task reads `Completed/OK`. Checking both catches the silent
  bail.
* **Post-flash version verify** — catches the union of the above:
  if the chassis is running the version we expected, it doesn't
  matter which trapdoor we just avoided.

## Hostlist format

The `-H` flag accepts Slurm-style hostlists via
[python-hostlist](https://pypi.org/project/python-hostlist/):

| Input                    | Expands to                                       |
|--------------------------|--------------------------------------------------|
| `node[001-003]`          | `node001`, `node002`, `node003`                  |
| `192.168.1.[10-12]`      | `192.168.1.10`, `192.168.1.11`, `192.168.1.12`   |
| `gpu[01-02],cpu[01-03]`  | `gpu01`, `gpu02`, `cpu01`, `cpu02`, `cpu03`      |
| `node[1,3,5-7]`          | `node1`, `node3`, `node5`, `node6`, `node7`      |

Use `-w` to cap concurrency (e.g. `-w 4` to flash four nodes at a
time).

### Output modes

When stdout is a TTY and `-v` is not requested, multi-host runs
use a **live dashboard**. Each host gets a single line that
accumulates structured fields parsed from the log output (`BMC`,
`BIOS`, `Method`, `Upload`, `Task`, …) plus a status phrase in
parentheses for the current activity. A `✓` (success) or `✗`
(failed) marker appears once the host completes:

```
  ✓ cyclo-node05.ipmi  Vendor=GIGABYTE  Model=R152-Z32  BIOS=F22  BMC=12.61.39  Power=On
  · cyclo-node06.ipmi  Method=multipart  Comp=BMC  From=12.61.19  Upload=80%  (uploading)
  · cyclo-node07.ipmi  Method=multipart  Comp=BMC  From=12.61.19  Upload=100%  Task=Running  (tracking)
  ✗ cyclo-node08.ipmi  Method=multipart  Comp=BMC  From=12.61.19  (failed)
  · cyclo-node09.ipmi  queued
```

Once **all** hosts complete the live dashboard is erased and each
host's full per-host output is printed in hostlist order, followed
by a `Done: N/M succeeded` summary.

Pass `-v` / `--verbose` to switch to the streaming view (every log
line printed in real time with a `[hostname]` prefix). Streaming
is also used automatically when stdout isn't a TTY.

## Examples

```bash
# Inspect a single BMC, including discovered preserve keys and
# UpdateInformation last-status
./lfish.py -H 192.168.1.10 -u admin -p secret info

# Update BMC on 8 nodes, 4 at a time, verifying the result
./lfish.py -H 'node[001-008]' -u admin -p secret -w 4 \
    update -c BMC -f bmc_fw.bin \
    --preserve-config --expected-version 12.61.39

# Force the multipart path even when --url is the obvious choice
./lfish.py -H 192.168.1.10 -u admin -p secret \
    update -c BIOS -f bios.bin --method multipart

# Talk to a quirky BMC: hand it explicit target + image type
./lfish.py -H weird01 -u admin -p secret \
    update -c BIOS -f bios.bin \
    --target-override /redfish/v1/UpdateService/FirmwareInventory/BIOS2 \
    --image-type-override BIOS

# Skip the preserve PATCH entirely (e.g. it's known to crash this BMC)
./lfish.py -H weird01 -u admin -p secret \
    update -c BMC -f bmc.bin --no-preserve

# Watch task progress on a finished or in-flight rack run
./lfish.py -H 'node[001-008]' -u admin -p secret tasks

# See what methods are loaded (including plugins)
LFISH_PLUGINS_DIR=~/lfish-plugins ./lfish.py methods
```
