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

from cinder import interface
from cinder.volume.drivers.pf9_hitachi import hbsd_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_iscsi


@interface.volumedriver
class HBSDGroupReplicationFCDriver(hbsd_fc.HBSDFCDriver):
    """Hitachi VSP group replication driver for Fibre Channel.

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

    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_FC"


@interface.volumedriver
class HBSDGroupReplicationISCSIDriver(hbsd_iscsi.HBSDISCSIDriver):
    """Hitachi VSP group replication driver for iSCSI.

    Configured like the FC class above, with this class as volume_driver and
    the iSCSI keys (use_chap_auth, chap_username, chap_password) available on
    both the backend section and replication_device.
    """

    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_ISCSI"
