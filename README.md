# lfish

BMC and BIOS firmware updater via the Redfish API.
Built for AMI-based BMCs (tested on Gigabyte S451-Z30) with support
for parallel operations across many hosts using Slurm-style hostlists.

## Install

```bash
pip3 install -r requirements.txt
```

## Quick start

```bash
# Show firmware versions
./lfish.py -H 192.168.1.10 -u admin -p secret info

# Update BMC firmware from a local file
./lfish.py -H 192.168.1.10 -u admin -p secret update -c BMC -f firmware.bin

# Same, but keep the current BMC config (network, users, IPMI, ...)
./lfish.py -H 192.168.1.10 -u admin -p secret update -c BMC -f firmware.bin --preserve-config

# Update across a whole rack in parallel
./lfish.py -H 'bmc[001-032]' -u admin -p secret update -c BMC -f firmware.bin --preserve-config
```

## Usage

```
lfish.py [-h] -H HOST [-u USER] [-p PASSWORD] [-w WORKERS] {info,tasks,update} ...
```

### Commands

| Command  | Description                            |
|----------|----------------------------------------|
| `info`   | Show firmware versions and system info |
| `update` | Update BMC or BIOS firmware            |
| `tasks`  | List active/recent update tasks        |

### Global options

| Flag               | Description                                                               |
|--------------------|---------------------------------------------------------------------------|
| `-H`, `--host`     | BMC host(s) -- single, comma-separated, or hostlist (e.g. `node[001-008]`) |
| `-u`, `--user`     | Redfish username (default: `admin`)                                       |
| `-p`, `--password` | Redfish password (default: `admin`)                                       |
| `-w`, `--workers`  | Max parallel hosts (default: `20`)                                        |
| `-v`, `--verbose`  | Multi-host: stream every log line with `[host]` prefix instead of dashboard |
| `-k`, `--insecure` | Skip TLS verification (default)                                          |
| `--secure`         | Enable TLS verification                                                   |

### Update options

| Flag                | Description                                                  |
|---------------------|--------------------------------------------------------------|
| `-c`, `--component` | Target component (`BMC`, `BIOS`, `MB_CPLD`, `BPB_CPLD`, ...) |
| `-f`, `--file`      | Local firmware image to upload                               |
| `--url`             | Remote firmware image URL (HTTP/HTTPS/FTP)                   |
| `--protocol`        | Override transfer protocol (auto-detected from URL)          |
| `--preserve-config` | Preserve BMC settings during update (see below)              |

## Preserve configuration

By default a BMC firmware update resets all settings to factory defaults.
Pass `--preserve-config` to keep the current configuration across the flash.

Settings preserved:

- **Network** -- IP addresses, VLAN, DNS
- **Authentication** -- local users and passwords
- **IPMI** -- IPMI-over-LAN settings
- **KVM** -- remote console settings
- **SNMP** -- trap destinations and community strings
- **NTP** -- time synchronisation
- **Syslog** -- remote logging targets

This works by setting flags via the AMI proprietary maintenance API
(`/api/maintenance/backup_config`) before triggering the Redfish update.

## Hostlist format

The `-H` flag accepts Slurm-style hostlists via [python-hostlist](https://pypi.org/project/python-hostlist/):

| Input                    | Expands to                                      |
|--------------------------|-------------------------------------------------|
| `node[001-003]`          | `node001`, `node002`, `node003`                 |
| `192.168.1.[10-12]`      | `192.168.1.10`, `192.168.1.11`, `192.168.1.12`  |
| `gpu[01-02],cpu[01-03]`  | `gpu01`, `gpu02`, `cpu01`, `cpu02`, `cpu03`      |
| `node[1,3,5-7]`          | `node1`, `node3`, `node5`, `node6`, `node7`     |

When targeting multiple hosts each runs in its own thread.  Use `-w`
to limit concurrency (e.g. `-w 4` to flash four nodes at a time).

### Output modes

When stdout is a terminal, multi-host runs use a **live dashboard** by
default.  Each host gets a single line that accumulates structured
fields parsed from the log output (`BMC`, `BIOS`, `Upload`, `Task`, …)
plus a status phrase in parentheses for the current activity.  As
hosts complete, a `✓` (success) or `✗` (failed) marker appears next
to the name:

```
  ✓ cyclo-node05.ipmi  Vendor=GIGABYTE  Model=R152-Z32  BIOS=F22  BMC=12.61.39  Power=On
  · cyclo-node06.ipmi  Comp=BMC  From=12.61.19  Upload=80%  (uploading)
  · cyclo-node07.ipmi  Comp=BMC  From=12.61.19  Upload=100%  Task=Running  (tracking)
  ✗ cyclo-node08.ipmi  Comp=BMC  From=12.61.19  (failed)
  · cyclo-node09.ipmi  queued
```

Earlier fields are not overwritten by later log lines, so for `info`
you keep seeing all the version/manufacturer data, and for `update`
the upload/task progress accumulates next to the firmware versions.

Once **all** hosts have completed, the live dashboard is erased and
each host's full per-host output is printed in **hostlist order**,
followed by a `Done: N/M succeeded` summary.  This makes it easy to
review results host-by-host even when hosts finished in a different
order from how they were specified.

Pass `-v` / `--verbose` to switch to the streaming view, where every
log line is printed in real time with a `[hostname]` prefix.  This
mode is also used automatically when stdout isn't a TTY (e.g. when
piping output to a file or another process).

## What happens during an update

1. **Validate** the component name against what the BMC supports.
2. **Set preserve flags** via the AMI maintenance API (if `--preserve-config`).
3. **Stream the firmware image** to `/redfish/v1/UpdateService/upload`
   with a live progress indicator (every 10%).
4. **Track the Redfish task** until it reaches a terminal state.  When
   the task's `TaskMonitor` is cleared (HTTP 404 on subsequent polls)
   the underlying `Task` is fetched as a fallback to read the final
   outcome.
5. **Wait for the BMC to come back online** if it dropped the connection
   to flash and reboot, then **verify the new firmware version**.

## Examples

```bash
# Single host info
./lfish.py -H 192.168.1.10 -u admin -p secret info

# Info across a rack
./lfish.py -H 'bmc[001-032]' -u admin -p secret info

# Update BMC from a local file
./lfish.py -H 192.168.1.10 -u admin -p secret update -c BMC -f bmc_fw.bin

# Update BIOS from a remote URL
./lfish.py -H 192.168.1.10 -u admin -p secret update -c BIOS \
    --url https://files.example.com/bios_v2.0.bin

# Update BMC on 8 nodes, 4 at a time, keeping config
./lfish.py -H 'node[001-008]' -u admin -p secret -w 4 \
    update -c BMC -f bmc_fw.bin --preserve-config

# Check update progress
./lfish.py -H 'node[001-008]' -u admin -p secret tasks
```
