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

When targeting multiple hosts each runs in its own thread and output is
grouped per host with a summary at the end.  Use `-w` to limit concurrency
(e.g. `-w 4` to flash four nodes at a time).

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
