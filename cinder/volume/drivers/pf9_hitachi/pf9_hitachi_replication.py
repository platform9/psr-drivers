"""
Platform9 Hitachi VSP Extended Replication Driver.

Implements Cinder group-replication actions (microversion 3.38):
- H1: manage_existing() — import promoted S-VOLs post-failover
- H5: enable_replication() — activate UR replication on a CG
- H7: failover_replication() — split and promote S-VOLs
- H2: unmanage() — release Cinder claim without affecting pair
- H6: disable_replication() — delete UR pairs
- H4: create_group_snapshot() — snapshot S-VOLs for test recovery
- H3: update_group() — add/remove volumes from live CG
- H8: list_replication_targets() — return secondary backend_id
"""

from cinder import interface
from cinder.volume.drivers.pf9_hitachi import hbsd_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_iscsi


@interface.volumedriver
class HBSDGroupReplicationFCDriver(hbsd_fc.HBSDFCDriver):
    """
    Hitachi VSP Remote Replication Driver for Fibre Channel.

    Implements Cinder group-replication actions for UR replication.

    Use this driver if your Hitachi backend is configured for FC transport.
    MRO puts the mixin first, so its __init__ and _update_volume_stats run
    and cooperatively call super() into HBSDFCDriver for FC setup and stats.

    Transport-specific methods (initialize_connection, terminate_connection, etc.)
    are inherited directly from HBSDFCDriver and work without modification.

    Configuration (cinder.conf):
        [hitachi_vsp_fc]
        volume_driver = cinder.volume.drivers.pf9_hitachi.pf9_hitachi_replication.HBSDGroupReplicationFCDriver
        san_ip = <primary-cm-ip>
        san_login = system
        san_password = <password>
        replication_device = backend_id:hitachi-vsp-secondary,san_ip:<secondary-cm-ip>,...
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_FC"


@interface.volumedriver
class HBSDGroupReplicationISCSIDriver(hbsd_iscsi.HBSDISCSIDriver):
    """
    Hitachi VSP Remote Replication Driver for iSCSI.

    Identical group-replication behavior to the FC variant above — same
    mixin, same Configuration Manager REST calls. Transport-specific methods
    (iSCSI target/portal handling) come from HBSDISCSIDriver instead.

    Use this driver if your Hitachi backend is configured for iSCSI transport.

    Configuration (cinder.conf):
        [hitachi_vsp_iscsi]
        volume_driver = cinder.volume.drivers.pf9_hitachi.pf9_hitachi_replication.HBSDGroupReplicationISCSIDriver
        san_ip = <primary-cm-ip>
        san_login = system
        san_password = <password>
        replication_device = backend_id:hitachi-vsp-secondary,san_ip:<secondary-cm-ip>,...
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_ISCSI"
