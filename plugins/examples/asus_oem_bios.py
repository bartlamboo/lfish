"""Example: OEM BIOS update via a vendor-specific Redfish action.

This file is a TEMPLATE. The `Asus.UpdateBios` action URI below is
a placeholder — it shows the registration pattern but is NOT a
verified ASUS endpoint. Before using on real hardware, GET your
BMC's `/redfish/v1/UpdateService` and look at the `Actions` /
`Oem` blocks to find the actual OEM action URI it advertises.

How to use:
    1. Drop this file into `$LFISH_PLUGINS_DIR`
    2. Replace the OEM action URI + payload with your BMC's actual
       endpoint
    3. Run: lfish methods   (to confirm it loaded)
    4. Run: lfish -H <host> update -c BIOS -f bios.bin \\
                  --method asus_oem_bios

The standard `multipart` method already handles the DMTF push that
most ASUS / AMI BMCs accept; reach for a plugin only when the
built-ins don't fit (silent-bail OEM that needs a different action,
in-band path that hits the host instead of the BMC, etc.).

What the runtime gives a plugin
-------------------------------
`register_method`, `UpdateMethod`, `UpdateContext`, `UpdateStrategy`,
and `METHODS` are injected into this module's namespace by
`lfish.load_plugins()`, so no imports from lfish itself are needed.

The `ctx` passed to `apply()` is an `UpdateContext`. Read it via the
property accessors so CLI overrides
(`--target-override` / `--image-type-override` / `--push-uri-override`
/ `--preserve-keys` / `--no-preserve`) work transparently:

    ctx.client          # RedfishClient — .get/.post/.patch/.post_multipart
    ctx.component       # "BIOS" / "BMC" / "CPLD"
    ctx.filepath        # local file path (when requires="file")
    ctx.url             # remote URL       (when requires="url")
    ctx.protocol        # "HTTP" / "HTTPS" / "FTP" or None
    ctx.target_uri      # FirmwareInventory member, override-aware
    ctx.image_type      # OemParameters.ImageType, override-aware
    ctx.push_uri        # MultipartHttpPushUri, override-aware
    ctx.simple_update_uri  # SimpleUpdate action target, override-aware
    ctx.preserve_keys   # discovered preserve keys, override-aware
    ctx.log             # operator-facing log callable

Pre/post hooks (UpdateService ready-gate, preserve PATCH for BMC,
AMI UpdateInformation sanity check, post-flash version verify) are
applied around `apply()` by `run_with_hooks`. Don't redo them here
— just do the vendor-specific bit and return True on success.
"""


def apply(ctx):
    ctx.log(f"  ASUS OEM BIOS flash -> {ctx.target_uri}")

    # TODO: confirm this URI + payload against your BMC's actual
    # /redfish/v1/UpdateService Actions / Oem block.
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
    label="ASUS OEM BIOS action (template)",
    family="out_of_band",
    description=(
        "Template plugin: vendor-specific BIOS update via an OEM "
        "Redfish action. Replace the action URI in apply() with the "
        "one your BMC advertises."
    ),
    apply=apply,
    applicable_components=["BIOS"],
    requires="file",
))
