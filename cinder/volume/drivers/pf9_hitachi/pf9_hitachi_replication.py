# Copyright (C) 2026 Platform9 Systems, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
"""Platform9 Hitachi VSP extended replication drivers.

These two classes add no behaviour of their own -- each only fixes the
transport. Every group-replication operation lives in
``hbsd_replication.HBSDREPLICATION``, which ``hbsd_fc``/``hbsd_iscsi``
instantiate as ``self.common`` whenever ``replication_device`` (or the
legacy ``hitachi_mirror_*`` set) is configured. There is no mixin.

Which means ``volume_driver`` can name either these classes or the stock
``hbsd_fc.HBSDFCDriver`` / ``hbsd_iscsi.HBSDISCSIDriver`` -- the behaviour
is identical, because what actually turns group replication on is
``replication_device`` plus a group type carrying
``consistent_group_replication_enabled`` or ``group_replication_enabled``.
These names exist because they say so out loud, and they are what
DEPLOYMENT.md configures.

What the extension adds over the upstream ``hbsd_*`` driver, which blocks or
omits all of it once ``replication_device`` is set:

===  ====================================  ==================================
Gap  Cinder entry point                    What it does on the array
===  ====================================  ==================================
H1   ``manage_existing``                   Adopts a promoted S-VOL on the
                                           secondary.
H2   ``unmanage``                          Releases Cinder's claim and
                                           leaves the copy pair intact.
H3   ``update_group``                      Adds to / removes from a live
                                           copy group.
H4   ``create_group_snapshot``             Thin Image group on the
                                           secondary, for test recovery.
H5   ``enable_replication``                Creates the UR copy group and
                                           its pairs. Restarts pairs that
                                           are merely suspended instead of
                                           rebuilding them.
H6   ``disable_replication``               Deletes the pairs.
H7   ``failover_replication``              Splits and promotes -- see
                                           "Failover mode" below.
H8   ``list_replication_targets``          Nothing. The volume manager
                                           answers this action from
                                           ``cinder.conf`` and never calls
                                           the driver, so the method is
                                           unreachable; it is kept only so
                                           the contract is complete.
===  ====================================  ==================================

**Failover mode.** Cinder gives ``failover_replication`` two parameters and
consumes one of them itself, so ``secondary_backend_id`` is the only channel
a client has. It accepts an optional suffix:

* ``<backend_id>`` or ``<backend_id>:emergency`` -- force split (takeover).
  Crash-consistent, works with the primary gone. **This is the default.**
* ``<backend_id>:graceful`` -- copy-group pairsplit issued from the primary,
  which drains the journal to a consistency point first. Needs both sites
  reachable.
* ``default`` -- failback, a swap resync. A mode suffix is rejected here.

A group type may set ``hbsd:group_replication_failover_mode = graceful`` to
change the default for every request against that group.

**Reporting.** With ``hitachi_replication_report_pair_status`` enabled (the
default) the pool capabilities carry per-copy-group pair state and
consistency time under ``pf9_group_replication_pairs``, readable through
``GET /v3/scheduler-stats/get_pools?detail=True``. Cinder exposes neither
replication lag nor a vendor pair state through any other API. Volumes are
also stamped with ``psr_pvol_id``, ``psr_svol_id`` and ``psr_copy_group``
metadata, because ``provider_location`` appears in no Cinder API view.
"""

from cinder import interface
from cinder.volume.drivers.pf9_hitachi import hbsd_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_iscsi


@interface.volumedriver
class HBSDGroupReplicationFCDriver(hbsd_fc.HBSDFCDriver):
    """Hitachi VSP remote replication driver for Fibre Channel.

    Use this when the backend is configured for FC transport. Connection
    handling is inherited from HBSDFCDriver unchanged; the replication
    behaviour comes from HBSDREPLICATION via self.common.

    Configuration (cinder.conf)::

        [hitachi_vsp_fc]
        volume_driver = cinder.volume.drivers.pf9_hitachi.\\
pf9_hitachi_replication.HBSDGroupReplicationFCDriver
        san_ip = <primary-cm-ip>
        san_login = <user>
        san_password = <password>
        hitachi_storage_id = <primary-serial>
        hitachi_pools = <pool>
        hitachi_replication_journal_size = <gb>
        replication_device = backend_id:<label>,san_ip:<secondary-cm-ip>,
            san_login:<user>,san_password:<password>,
            storage_id:<secondary-serial>,pool:<pool>
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_FC"


@interface.volumedriver
class HBSDGroupReplicationISCSIDriver(hbsd_iscsi.HBSDISCSIDriver):
    """Hitachi VSP remote replication driver for iSCSI.

    Identical replication behaviour to the FC class above -- the same
    HBSDREPLICATION common object and the same Configuration Manager calls.
    Only the transport differs: target and portal handling is inherited
    from HBSDISCSIDriver.

    Configuration is the same as the FC class, with
    ``HBSDGroupReplicationISCSIDriver`` as the ``volume_driver`` and the
    iSCSI keys (``use_chap_auth``, ``chap_username``, ``chap_password``)
    available on both the backend section and ``replication_device``.
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_ISCSI"
