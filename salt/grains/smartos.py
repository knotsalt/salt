"""
SmartOS grain provider

:maintainer:    Jorge Schrauwen <sjorge@blackdot.be>
:maturity:      new
:depends:       salt.utils, salt.module.cmdmod
:platform:      SmartOS

.. versionadded:: 2017.7.0

"""

import logging
import os
import re

import salt.modules.cmdmod
import salt.utils.dictupdate
import salt.utils.json
import salt.utils.path
import salt.utils.platform
import salt.utils.stringutils

__virtualname__ = "smartos"
__salt__ = {
    "cmd.run": salt.modules.cmdmod.run,
}

log = logging.getLogger(__name__)


def __virtual__():
    """
    Only load when we are on SmartOS
    """
    if salt.utils.platform.is_smartos():
        return __virtualname__
    return False


def _smartos_computenode_data():
    """
    Return useful information from a SmartOS compute node
    """
    # Provides:
    #   vms_total
    #   vms_running
    #   vms_stopped
    #   vms_type
    #   sdc_version
    #   vm_capable
    #   vm_hw_virt

    grains = {}

    # collect vm data
    vms = {}
    for vm in __salt__["cmd.run"]("vmadm list -p -o uuid,alias,state,type").split("\n"):
        vm = dict(list(zip(["uuid", "alias", "state", "type"], vm.split(":"))))
        vms[vm["uuid"]] = vm
        del vms[vm["uuid"]]["uuid"]

    # set vm grains
    grains["computenode_vms_total"] = len(vms)
    grains["computenode_vms_running"] = 0
    grains["computenode_vms_stopped"] = 0
    grains["computenode_vms_type"] = {"KVM": 0, "LX": 0, "OS": 0}
    for vm in vms:
        if vms[vm]["state"].lower() == "running":
            grains["computenode_vms_running"] += 1
        elif vms[vm]["state"].lower() == "stopped":
            grains["computenode_vms_stopped"] += 1

        if vms[vm]["type"] not in grains["computenode_vms_type"]:
            # NOTE: be prepared for when bhyve gets its own type
            grains["computenode_vms_type"][vms[vm]["type"]] = 0
        grains["computenode_vms_type"][vms[vm]["type"]] += 1

    # sysinfo derived grains
    sysinfo = salt.utils.json.loads(__salt__["cmd.run"]("sysinfo"))
    grains["computenode_sdc_version"] = sysinfo["SDC Version"]
    grains["computenode_vm_capable"] = sysinfo["VM Capable"]
    if sysinfo["VM Capable"]:
        grains["computenode_vm_hw_virt"] = sysinfo["CPU Virtualization"]

    # sysinfo derived smbios grains
    grains["manufacturer"] = sysinfo["Manufacturer"]
    grains["productname"] = sysinfo["Product"]
    grains["uuid"] = sysinfo["UUID"]

    return grains


def _smartos_zone_data():
    """
    Return useful information from a SmartOS zone
    """
    # Provides:
    #   zoneid
    #   zonename
    #   imageversion
    grains = {}

    zoneinfo = __salt__["cmd.run"]("zoneadm list -p").strip().split(":")
    grains["zoneid"] = zoneinfo[0]
    grains["zonename"] = zoneinfo[1]

    imageversion = re.compile("Image:\\s(.+)")
    grains["imageversion"] = "Unknown"
    if os.path.isfile("/etc/product"):
        with salt.utils.files.fopen("/etc/product", "r") as fp_:
            for line in fp_:
                line = salt.utils.stringutils.to_unicode(line)
                match = imageversion.match(line)
                if match:
                    grains["imageversion"] = match.group(1)

    return grains


def _smartos_zone_pkgsrc_data():
    """
    SmartOS zone pkgsrc information
    """
    # Provides:
    #   pkgsrcversion
    #   pkgsrcpath

    grains = {
        "pkgsrcversion": "Unknown",
        "pkgsrcpath": "Unknown",
    }

    # NOTE: we are specifically interested in the SmartOS pkgsrc version and path
    #       - PKG_PATH MAY be different on non-SmartOS systems, but they will not
    #         use this grains module.
    #       - A sysadmin with advanced needs COULD create a 'spin' with a totally
    #         different URL. But at that point the value would be meaning less in
    #         the context of the pkgsrcversion grain as it will not followed the
    #         SmartOS pkgsrc versioning. So 'Unknown' would be appropriate.
    pkgsrcpath = re.compile("PKG_PATH=(.+)")
    pkgsrcversion = re.compile(
        "^https?://pkgsrc.joyent.com/packages/SmartOS/(.+)/(.+)/All$"
    )
    pkg_install_paths = [
        "/opt/local/etc/pkg_install.conf",
        "/opt/tools/etc/pkg_install.conf",
    ]
    for pkg_install in pkg_install_paths:
        if os.path.isfile(pkg_install):
            with salt.utils.files.fopen(pkg_install, "r") as fp_:
                for line in fp_:
                    line = salt.utils.stringutils.to_unicode(line)
                    match_pkgsrcpath = pkgsrcpath.match(line)
                    if match_pkgsrcpath:
                        grains["pkgsrcpath"] = match_pkgsrcpath.group(1)
                        match_pkgsrcversion = pkgsrcversion.match(
                            match_pkgsrcpath.group(1)
                        )
                        if match_pkgsrcversion:
                            grains["pkgsrcversion"] = match_pkgsrcversion.group(1)
                        break

    return grains


def _smartos_zone_pkgin_data():
    """
    SmartOS zone pkgin information
    """
    # Provides:
    #   pkgin_repositories

    grains = {
        "pkgin_repositories": [],
    }

    pkginrepo = re.compile("^(?:https|http|ftp|file)://.*$")
    repositories_path = [
        "/opt/local/etc/pkgin/repositories.conf",
        "/opt/tools/etc/pkgin/repositories.conf",
    ]
    for repositories in repositories_path:
        if os.path.isfile(repositories):
            with salt.utils.files.fopen(repositories, "r") as fp_:
                for line in fp_:
                    line = salt.utils.stringutils.to_unicode(line).strip()
                    if pkginrepo.match(line):
                        grains["pkgin_repositories"].append(line)

    return grains


def smartos():
    """
    Provide grains for SmartOS
    """
    grains = {}

    if salt.utils.platform.is_smartos_zone():
        grains = salt.utils.dictupdate.update(
            grains, _smartos_zone_data(), merge_lists=True
        )
    elif salt.utils.platform.is_smartos_globalzone():
        grains = salt.utils.dictupdate.update(
            grains, _smartos_computenode_data(), merge_lists=True
        )
    grains = salt.utils.dictupdate.update(
        grains, _smartos_zone_pkgin_data(), merge_lists=True
    )
    grains = salt.utils.dictupdate.update(
        grains, _smartos_zone_pkgsrc_data(), merge_lists=True
    )

    return grains
