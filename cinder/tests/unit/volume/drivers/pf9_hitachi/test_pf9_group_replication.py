# Copyright (C) 2020, 2024, Hitachi, Ltd.
# Copyright (C) 2025, Hitachi Vantara
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
"""Unit tests for pf9_hitachi group replication."""

import inspect
import itertools
import json
from unittest import mock

from oslo_config import cfg
import requests

from cinder import context as cinder_context
from cinder import exception
from cinder.objects import fields
from cinder.objects import group_snapshot as obj_group_snap
from cinder.objects import snapshot as obj_snap
from cinder.tests.unit import fake_group
from cinder.tests.unit import fake_group_snapshot
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder.tests.unit import test
from cinder.volume import configuration as conf
from cinder.volume.drivers.pf9_hitachi import hbsd_common
from cinder.volume.drivers.pf9_hitachi import hbsd_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_replication
from cinder.volume.drivers.pf9_hitachi import hbsd_rest
from cinder.volume.drivers.pf9_hitachi import hbsd_rest_api
from cinder.volume.drivers.pf9_hitachi import hbsd_rest_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_utils
from cinder.volume import group_types
from cinder.volume import volume_utils

# Configuration parameter values
GROUP_ID = '11111111-1111-1111-1111-111111111111'
GROUP_SNAPSHOT_ID = '22222222-2222-2222-2222-222222222222'

CONFIG_MAP = {
    'serial': '886000123456',
    'my_ip': '127.0.0.1',
    'rest_server_ip_addr': '172.16.18.108',
    'rest_server_ip_port': '23451',
    'port_id': 'CL1-A',
    'host_grp_name': 'HBSD-0123456789abcdef',
    'host_mode': 'LINUX/IRIX',
    'host_wwn': '0123456789abcdef',
    'target_wwn': '1111111123456789',
    'user_id': 'user',
    'user_pass': 'password',
    'pool_name': 'test_pool',
    'auth_user': 'auth_user',
    'auth_password': 'auth_password',
}

REMOTE_CONFIG_MAP = {
    'serial': '886000456789',
    'my_ip': '127.0.0.1',
    'rest_server_ip_addr': '172.16.18.107',
    'rest_server_ip_port': '334',
    'port_id': 'CL2-B',
    'host_grp_name': 'HBSD-0123456789abcdef',
    'host_mode': 'LINUX/IRIX',
    'host_wwn': '0123456789abcdef',
    'target_wwn': '2222222234567891',
    'user_id': 'remote-user',
    'user_pass': 'remote-password',
    'pool_name': 'remote_pool',
    'auth_user': 'remote_user',
    'auth_password': 'remote_password',
}

# Dummy response for FC zoning device mapping
DEVICE_MAP = {
    'fabric_name': {
        'initiator_port_wwn_list': [CONFIG_MAP['host_wwn']],
        'target_port_wwn_list': [CONFIG_MAP['target_wwn']]}}

REMOTE_DEVICE_MAP = {
    'fabric_name': {
        'initiator_port_wwn_list': [REMOTE_CONFIG_MAP['host_wwn']],
        'target_port_wwn_list': [REMOTE_CONFIG_MAP['target_wwn']]}}

DEFAULT_CONNECTOR = {
    'host': 'host',
    'ip': CONFIG_MAP['my_ip'],
    'wwpns': [CONFIG_MAP['host_wwn']],
    'multipath': False,
}

REMOTE_DEFAULT_CONNECTOR = {
    'host': 'host',
    'ip': REMOTE_CONFIG_MAP['my_ip'],
    'wwpns': [REMOTE_CONFIG_MAP['host_wwn']],
    'multipath': False,
}

CTXT = cinder_context.get_admin_context()

TEST_VOLUME = []
for i in range(9):
    volume = {}
    volume['id'] = '00000000-0000-0000-0000-{0:012d}'.format(i)
    volume['name'] = 'test-volume{0:d}'.format(i)
    volume['volume_type_id'] = '00000000-0000-0000-0000-{0:012d}'.format(i)
    if i == 3 or i == 8:
        volume['provider_location'] = None
    elif i == 4:
        volume['provider_location'] = json.dumps(
            {'pldev': 4, 'sldev': 4,
             'remote-copy': hbsd_utils.REP_TYPE_ASYNC})
    elif i == 5:
        volume['provider_location'] = json.dumps(
            {'pldev': 5, 'sldev': 5,
             'remote-copy': hbsd_utils.REP_TYPE_ASYNC})
        volume['replication_status'] = fields.ReplicationStatus.ENABLED
    elif i == 6:
        volume['provider_location'] = json.dumps(
            {'pldev': 6, 'sldev': 6,
             'remote-copy': hbsd_utils.REP_TYPE_ASYNC})
        volume['replication_status'] = fields.ReplicationStatus.FAILED_OVER
    elif i == 7:
        volume['volume_type_id'] = '00000000-0000-0000-0000-{0:012d}'.format(i)
        volume['provider_location'] = json.dumps(
            {'pldev': 7,
             'remote-copy': hbsd_utils.REP_TYPE_ASYNC})
        volume['replication_status'] = fields.ReplicationStatus.ENABLED
    else:
        volume['provider_location'] = '{0:d}'.format(i)
    volume['size'] = 128
    if i == 2:
        volume['status'] = 'in-use'
    elif i == 8:
        volume['status'] = None
    else:
        volume['status'] = 'available'
    volume = fake_volume.fake_volume_obj(CTXT, **volume)
    volume.volume_type = fake_volume.fake_volume_type_obj(CTXT)
    TEST_VOLUME.append(volume)


def _volume_get(context, volume_id):
    """Return predefined volume info."""
    return TEST_VOLUME[int(volume_id.replace("-", ""))]


TEST_SNAPSHOT = []
for i in range(6):
    snapshot = {}
    snapshot['id'] = '10000000-0000-0000-0000-{0:012d}'.format(i)
    snapshot['name'] = 'TEST_SNAPSHOT{0:d}'.format(i)
    snapshot['provider_location'] = '{0:d}'.format(i + 1)
    snapshot['status'] = 'available'
    snapshot['volume_id'] = '00000000-0000-0000-0000-{0:012d}'.format(i)
    snapshot['volume'] = _volume_get(None, snapshot['volume_id'])
    snapshot['volume_name'] = 'test-volume{0:d}'.format(i)
    snapshot['volume_size'] = 128
    if i == 5:
        snapshot['provider_location'] = json.dumps(
            {'sldev': 5})
    snapshot = obj_snap.Snapshot._from_db_object(
        CTXT, obj_snap.Snapshot(),
        fake_snapshot.fake_db_snapshot(**snapshot))
    TEST_SNAPSHOT.append(snapshot)

TEST_GROUP = []
for i in range(2):
    group = {}
    group['id'] = '20000000-0000-0000-0000-{0:012d}'.format(i)
    group['status'] = 'available'
    group = fake_group.fake_group_obj(CTXT, **group)
    TEST_GROUP.append(group)

TEST_GROUP_SNAP = []
group_snapshot = {}
group_snapshot['id'] = '30000000-0000-0000-0000-{0:012d}'.format(0)
group_snapshot['status'] = 'available'
group_snapshot = obj_group_snap.GroupSnapshot._from_db_object(
    CTXT, obj_group_snap.GroupSnapshot(),
    fake_group_snapshot.fake_db_group_snapshot(**group_snapshot))
TEST_GROUP_SNAP.append(group_snapshot)

# Dummy response for REST API
POST_SESSIONS_RESULT = {
    "token": "b74777a3-f9f0-4ea8-bd8f-09847fac48d3",
    "sessionId": 0,
}

REMOTE_POST_SESSIONS_RESULT = {
    "token": "b74777a3-f9f0-4ea8-bd8f-09847fac48d4",
    "sessionId": 0,
}

GET_PORTS_RESULT = {
    "data": [
        {
            "portId": CONFIG_MAP['port_id'],
            "portType": "FIBRE",
            "portAttributes": [
                "TAR",
                "MCU",
                "RCU",
                "ELUN"
            ],
            "fabricMode": True,
            "portConnection": "PtoP",
            "lunSecuritySetting": True,
            "wwn": CONFIG_MAP['target_wwn'],
        },
    ],
}

REMOTE_GET_PORTS_RESULT = {
    "data": [
        {
            "portId": REMOTE_CONFIG_MAP['port_id'],
            "portType": "FIBRE",
            "portAttributes": [
                "TAR",
                "MCU",
                "RCU",
                "ELUN"
            ],
            "fabricMode": True,
            "portConnection": "PtoP",
            "lunSecuritySetting": True,
            "wwn": REMOTE_CONFIG_MAP['target_wwn'],
        },
    ],
}

GET_HOST_WWNS_RESULT = {
    "data": [
        {
            "hostGroupNumber": 0,
            "hostWwn": CONFIG_MAP['host_wwn'],
        },
    ],
}

REMOTE_GET_HOST_WWNS_RESULT = {
    "data": [
        {
            "hostGroupNumber": 0,
            "hostWwn": REMOTE_CONFIG_MAP['host_wwn'],
        },
    ],
}

COMPLETED_SUCCEEDED_RESULT = {
    "status": "Completed",
    "state": "Succeeded",
    "affectedResources": ('a/b/c/1',),
}

REMOTE_COMPLETED_SUCCEEDED_RESULT = {
    "status": "Completed",
    "state": "Succeeded",
    "affectedResources": ('a/b/c/2',),
}

COMPLETED_FAILED_RESULT_LU_DEFINED = {
    "status": "Completed",
    "state": "Failed",
    "error": {
        "errorCode": {
            "SSB1": "B958",
            "SSB2": "015A",
        },
    },
}

GET_LDEV_RESULT = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP"],
    "status": "NML",
    "poolId": 30,
    "dataReductionStatus": "DISABLED",
    "dataReductionMode": "disabled",
    "label": "00000000000000000000000000000000",
}

GET_LDEV_RESULT_SPLIT = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP"],
    "status": "NML",
    "poolId": 30,
    "dataReductionStatus": "DISABLED",
    "dataReductionMode": "disabled",
    "label": "00000000000000000000000000000004",
}

GET_LDEV_RESULT_LABEL = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP"],
    "status": "NML",
    "poolId": 30,
    "dataReductionStatus": "DISABLED",
    "dataReductionMode": "disabled",
    "label": "00000000000000000000000000000001",
}

GET_LDEV_RESULT_MAPPED = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP"],
    "status": "NML",
    "ports": [
        {
            "portId": CONFIG_MAP['port_id'],
            "hostGroupNumber": 0,
            "hostGroupName": CONFIG_MAP['host_grp_name'],
            "lun": 1
        },
    ],
}

GET_LDEV_RESULT_PAIR = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP", "HTI"],
    "status": "NML",
    "label": "10000000000000000000000000000000",
}

GET_LDEV_RESULT_REP = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP", "HORC"],
    "status": "NML",
    "numOfPorts": 1,
    "label": "00000000000000000000000000000004",
}

GET_LDEV_RESULT_REP_LABEL = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP", "HORC"],
    "status": "NML",
    "numOfPorts": 1,
    "label": "00000000000000000000000000000001",
}

GET_LDEV_RESULT_REP_PAIR = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP", "HORC", "HTI"],
    "status": "NML",
    "numOfPorts": 1,
}

GET_POOLS_RESULT = {
    "data": [
        {
            "poolId": 30,
            "poolName": CONFIG_MAP['pool_name'],
            "availableVolumeCapacity": 480144,
            "totalPoolCapacity": 507780,
            "totalLocatedCapacity": 71453172,
            "virtualVolumeCapacityRate": -1,
        },
    ],
}

GET_SNAPSHOTS_RESULT = {
    "data": [
        {
            "primaryOrSecondary": "S-VOL",
            "status": "PSUS",
            "pvolLdevId": 0,
            "muNumber": 1,
            "svolLdevId": 1,
        },
    ],
}

GET_SNAPSHOTS_RESULT_SMPL = {
    "data": [
        {
            "primaryOrSecondary": "S-VOL",
            "status": "SMPL",
            "pvolLdevId": 0,
            "muNumber": 1,
            "svolLdevId": 1,
        },
    ],
}

GET_SNAPSHOTS_RESULT_PAIR = {
    "data": [
        {
            "primaryOrSecondary": "S-VOL",
            "status": "PAIR",
            "pvolLdevId": 0,
            "muNumber": 1,
            "svolLdevId": 1,
        },
    ],
}

GET_SNAPSHOTS_RESULT_BUSY = {
    "data": [
        {
            "primaryOrSecondary": "P-VOL",
            "status": "PSUP",
            "pvolLdevId": 0,
            "muNumber": 1,
            "svolLdevId": 1,
        },
    ],
}

GET_SNAPSHOTS_RESULT_PSUS = {
    "data": [
        {
            "primaryOrSecondary": "S-VOL",
            "status": "PSUS",
            "pvolLdevId": 4,
            "muNumber": 1,
            "svolLdevId": 5,
        },
    ],
}

GET_LUNS_RESULT = {
    "data": [
        {
            "ldevId": 0,
            "lun": 1,
        },
    ],
}

GET_HOST_GROUP_RESULT = {
    "hostGroupName": CONFIG_MAP['host_grp_name'],
}

GET_HOST_GROUPS_RESULT = {
    "data": [
        {
            "hostGroupNumber": 0,
            "portId": CONFIG_MAP['port_id'],
            "hostGroupName": "HBSD-test",
        },
    ],
}

GET_HOST_GROUPS_RESULT_PAIR = {
    "data": [
        {
            "hostGroupNumber": 1,
            "portId": CONFIG_MAP['port_id'],
            "hostGroupName": "HBSD-pair00",
        },
    ],
}

REMOTE_GET_HOST_GROUPS_RESULT_PAIR = {
    "data": [
        {
            "hostGroupNumber": 1,
            "portId": REMOTE_CONFIG_MAP['port_id'],
            "hostGroupName": "HBSD-pair00",
        },
    ],
}

GET_LDEVS_RESULT = {
    "data": [
        {
            "ldevId": 0,
            "label": "15960cc738c94c5bb4f1365be5eeed44",
        },
        {
            "ldevId": 1,
            "label": "15960cc738c94c5bb4f1365be5eeed45",
        },
    ],
}

GET_REMOTE_MIRROR_COPYPAIR_RESULT = {
    'pvolLdevId': 4,
    'svolLdevId': 4,
    'pvolStatus': 'PAIR',
    'svolStatus': 'PAIR',
    'replicationType': hbsd_utils.REP_TYPE_ASYNC,
    'pvolJournalId': 0,
    'svolJournalId': 0,
}

GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS = {
    'pvolLdevId': 5,
    'svolLdevId': 5,
    'pvolStatus': 'PSUS',
    'svolStatus': 'SSWS',
    'replicationType': hbsd_utils.REP_TYPE_ASYNC,
    'pvolJournalId': 0,
    'svolJournalId': 0,
}

GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS_FAILBACK = {
    'pvolLdevId': 6,
    'svolLdevId': 6,
    'pvolStatus': 'PSUS',
    'svolStatus': 'SSWS',
    'replicationType': hbsd_utils.REP_TYPE_ASYNC,
    'pvolJournalId': 0,
    'svolJournalId': 0,
}

GET_REMOTE_MIRROR_COPYPAIR_RESULT_SPLIT = {
    'pvolLdevId': 6,
    'svolLdevId': 6,
    'pvolStatus': 'PSUE',
    'svolStatus': 'SSWS',
    'replicationType': hbsd_utils.REP_TYPE_ASYNC,
    'pvolJournalId': 0,
    'svolJournalId': 0,
}

GET_REMOTE_MIRROR_COPYPAIR_RESULT_PSUS = {
    'pvolLdevId': 6,
    'svolLdevId': 6,
    'pvolStatus': 'PSUS',
    'svolStatus': 'SSUS',
    'replicationType': hbsd_utils.REP_TYPE_ASYNC,
    'pvolJournalId': 0,
    'svolJournalId': 0,
}

GET_REMOTE_MIRROR_COPYGROUP_RESULT = {
    'copyGroupName': 'HBSD-127.0.0.100U00',
    'copyPairs': [GET_REMOTE_MIRROR_COPYPAIR_RESULT],
}

GET_REMOTE_MIRROR_COPYGROUP_RESULT_ERROR = {
    "errorSource": "<URL>",
    "message": "<message>",
    "solution": "<solution>",
    "messageId": hbsd_replication._MSGID_INSTANCE_CANNOT_OPERATED,
    "errorCode": {
                   "SSB1": "",
                   "SSB2": "",
    }
}

GET_REMOTE_MIRROR_COPYGROUP_RESULT_SSWS = {
    'copyGroupName': 'HBSD-127.0.0.100U00',
    'copyPairs': [GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS_FAILBACK],
}

GET_REMOTE_MIRROR_COPYGROUPS_RESULT_PAIR = {
    "data": [
        {
            'copyGroupName': 'HBSD-127.0.0.100U00',
            'copyPairs': [GET_REMOTE_MIRROR_COPYPAIR_RESULT],
        },
    ],
}

GET_REMOTE_MIRROR_COPYGROUPS_RESULT_SSWS = {
    "data": [
        {
            'copyGroupName': 'HBSD-127.0.0.100U00',
            'copyPairs': [GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS_FAILBACK],
        },
    ],
}

GET_JOURNAL_RESULT = {
    'firstLdevId': 1,
}

REMOTE_GET_JOURNAL_RESULT = {
    'firstLdevId': 2,
}


GET_JOURNAL_RESULT_MESSAGEID_KART40054E = {
    'firstLdevId': 2,
    'pvolJournalId': 10,
    'svolJournalId': 10,
    'journalId': 10,
    "data": [
        {
            "test1": "test1",
            "journalId": 0,
            "test2": "test2",
        }
    ],
    "errorSource": "testurl",
    "message": "testmessage",
    "solution": "test",
    "messageId": "KART40054-E",
    "errorCode": {
                   "SSB1": "1",
                   "SSB2": "2",
    }
}

GET_JOURNAL_RESULT_MESSAGEID_KART40046E = {
    'firstLdevId': 2,
    'pvolJournalId': 10,
    'svolJournalId': 10,
    'journalId': 10,
    "data": [
        {
            "test1": "test1",
            "journalId": 0,
            "test2": "test2",
        }
    ],
    "errorSource": "testurl",
    "message": "testmessage",
    "solution": "test",
    "messageId": "KART40046-E",
    "errorCode": {
                   "SSB1": "1",
                   "SSB2": "2",
    }
}

NOTFOUND_RESULT = {
    "data": [],
}

ERROR_RESULT = {
    "errorSource": "<URL>",
    "message": "<message>",
    "solution": "<solution>",
    "messageId": "<messageId>",
    "errorCode": {
                   "SSB1": "",
                   "SSB2": "",
    }
}

global_counter = 0


def _brick_get_connector_properties(multipath=False, enforce_multipath=False):
    """Return a predefined connector object."""
    return DEFAULT_CONNECTOR


class FakeLookupService():
    """Dummy FC zoning mapping lookup service class."""

    def get_device_mapping_from_network(self, initiator_wwns, target_wwns):
        """Return predefined FC zoning mapping."""
        return DEVICE_MAP


class FakeResponse():

    def __init__(self, status_code, data=None, headers=None):
        self.status_code = status_code
        self.data = data
        self.text = data
        self.content = data
        self.headers = {'Content-Type': 'json'} if headers is None else headers

    def json(self):
        return self.data


class PF9GroupReplicationFCTest(test.TestCase):
    """Unit tests for group replication on the stock HBSD FC driver."""

    test_existing_ref = {'source-id': '1'}
    test_existing_ref_name = {
        'source-name': '15960cc7-38c9-4c5b-b4f1-365be5eeed45'}

    def setUp(self):
        """Set up the test environment."""
        def _set_required(opts, required):
            for opt in opts:
                opt.required = required

        # Initialize Cinder and avoid checking driver options.
        rest_required_opts = [
            opt for opt in hbsd_rest.REST_VOLUME_OPTS if opt.required]
        common_required_opts = [
            opt for opt in hbsd_common.COMMON_VOLUME_OPTS if opt.required]
        _set_required(rest_required_opts, False)
        _set_required(common_required_opts, False)
        super(PF9GroupReplicationFCTest, self).setUp()
        _set_required(rest_required_opts, True)
        _set_required(common_required_opts, True)

        self.configuration = conf.Configuration(
            hbsd_rest.REST_VOLUME_OPTS + hbsd_rest.REST_PAIR_OPTS +
            hbsd_common.COMMON_VOLUME_OPTS + hbsd_common.COMMON_PORT_OPTS +
            hbsd_common.COMMON_PAIR_OPTS + hbsd_common.COMMON_NAME_OPTS +
            hbsd_common.COMMON_EXTEND_OPTS + hbsd_replication._REP_OPTS +
            hbsd_replication.COMMON_REPLICATION_OPTS +
            hbsd_replication.COMMON_MIRROR_OPTS +
            hbsd_replication.ISCSI_MIRROR_OPTS +
            hbsd_replication.REST_MIRROR_OPTS +
            hbsd_replication.REST_MIRROR_API_OPTS +
            hbsd_replication.REST_MIRROR_SSL_OPTS +
            hbsd_rest_fc.FC_VOLUME_OPTS,
            conf.SHARED_CONF_GROUP)

        self.ctxt = cinder_context.get_admin_context()
        self._setup_config()
        self._setup_driver()

    def _setup_config(self):
        """Set configuration parameter values."""
        self.override_config('volume_backend_name', "RESTFC",
                             group=conf.SHARED_CONF_GROUP)
        self.override_config(
            'volume_driver',
            "cinder.volume.drivers.pf9_hitachi.hbsd_fc.HBSDFCDriver",
            group=conf.SHARED_CONF_GROUP)
        self.override_config('reserved_percentage', "0",
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('use_multipath_for_image_xfer', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('enforce_multipath_for_image_xfer', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('max_over_subscription_ratio', 500.0,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('driver_ssl_cert_verify', False,
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_storage_id', CONFIG_MAP['serial'],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_pools', ["30"],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_snap_pool', None,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_ldev_range', "0-1",
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_target_ports', [CONFIG_MAP['port_id']],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_compute_target_ports',
                             [CONFIG_MAP['port_id']],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_group_create', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_group_delete', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_copy_speed', 3,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_copy_check_interval', 3,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_async_copy_check_interval', 10,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_port_scheduler', False,
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('san_login', CONFIG_MAP['user_id'],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_password', CONFIG_MAP['user_pass'],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_ip', CONFIG_MAP['rest_server_ip_addr'],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_api_port', CONFIG_MAP['rest_server_ip_port'],
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_rest_disable_io_wait', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_tcp_keepalive', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_discard_zero_page', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_lun_timeout', hbsd_rest._LUN_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_lun_retry_interval',
                             hbsd_rest._LUN_RETRY_INTERVAL,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_restore_timeout',
                             hbsd_rest._RESTORE_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_state_transition_timeout',
                             hbsd_rest._STATE_TRANSITION_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_lock_timeout',
                             hbsd_rest_api._LOCK_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_timeout',
                             hbsd_rest_api._REST_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_extend_timeout',
                             hbsd_rest_api._EXTEND_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_exec_retry_interval',
                             hbsd_rest_api._EXEC_RETRY_INTERVAL,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_connect_timeout',
                             hbsd_rest_api._DEFAULT_CONNECT_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_job_api_response_timeout',
                             hbsd_rest_api._JOB_API_RESPONSE_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_get_api_response_timeout',
                             hbsd_rest_api._GET_API_RESPONSE_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_server_busy_timeout',
                             hbsd_rest_api._REST_SERVER_BUSY_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_keep_session_loop_interval',
                             hbsd_rest_api._KEEP_SESSION_LOOP_INTERVAL,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_another_ldev_mapped_retry_timeout',
                             hbsd_rest_api._ANOTHER_LDEV_MAPPED_RETRY_TIMEOUT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_tcp_keepidle',
                             hbsd_rest_api._TCP_KEEPIDLE,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_tcp_keepintvl',
                             hbsd_rest_api._TCP_KEEPINTVL,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_tcp_keepcnt',
                             hbsd_rest_api._TCP_KEEPCNT,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_host_mode_options', [],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_use_object_caching', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_max_request_workers',
                             hbsd_rest_api._MAX_REQUEST_WORKERS,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_zoning_request', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_extend_snapshot_volumes', False,
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('use_chap_auth', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('chap_username', CONFIG_MAP['auth_user'],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('chap_password', CONFIG_MAP['auth_password'],
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('san_thin_provision', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_private_key', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_clustername', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_ssh_port', '22',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('san_is_local', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('ssh_conn_timeout', '30',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('ssh_min_pool_conn', '1',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('ssh_max_pool_conn', '5',
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_replication_status_check_short_interval',
                             5, group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_status_check_long_interval',
                             10 * 60,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_status_check_timeout',
                             24 * 60 * 60,
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_replication_number', 0,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_pair_target_number', 0,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_rest_pair_target_ports',
                             [CONFIG_MAP['port_id']],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_quorum_disk_id', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_copy_speed', 3,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_storage_id', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_pool', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_snap_pool', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_ldev_range', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_target_ports', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_compute_target_ports', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_pair_target_number', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_rest_user', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_rest_password', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_rest_api_ip', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_rest_api_port', '',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_set_mirror_reserve_attribute', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_path_group_id', 0,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_rest_pair_target_ports', [],
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_auth_password', None,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_auth_user', None,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_use_chap_auth', False,
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_mirror_ssl_cert_verify', False,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_mirror_ssl_cert_path', '',
                             group=conf.SHARED_CONF_GROUP)

        self.override_config('hitachi_replication_mun', 1,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_journal_size', '10',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_journal_overflow_tolerance',
                             60, group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_journal_use_cache', True,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_journal_transfer_speed', 256,
                             group=conf.SHARED_CONF_GROUP)
        self.override_config('hitachi_replication_journal_creation_speed', 'L',
                             group=conf.SHARED_CONF_GROUP)
        self.override_config(
            'hitachi_replication_journal_path_failure_tolerance', 5,
            group=conf.SHARED_CONF_GROUP)

        replication_device = [
            {'backend_id': 'backend2',
             'storage_id': REMOTE_CONFIG_MAP['serial'],
             'pool': '40',
             'snap_pool': None,
             'ldev_range': '2-3',
             'target_ports': REMOTE_CONFIG_MAP['port_id'],
             'compute_target_ports': REMOTE_CONFIG_MAP['port_id'],
             'pair_target_number': '0',
             'san_login': REMOTE_CONFIG_MAP['user_id'],
             'san_password': REMOTE_CONFIG_MAP['user_pass'],
             'rest_pair_target_ports': REMOTE_CONFIG_MAP['port_id'],
             'san_ip': REMOTE_CONFIG_MAP['rest_server_ip_addr'],
             'san_api_port': REMOTE_CONFIG_MAP['rest_server_ip_port']}
        ]
        self.override_config('replication_device', replication_device,
                             group=conf.SHARED_CONF_GROUP)

        CONF = cfg.CONF
        CONF.my_ip = CONFIG_MAP['my_ip']

    def _fake_safe_get(self, value):
        """Retrieve a configuration value avoiding throwing an exception."""
        try:
            val = getattr(self.configuration, value)
        except AttributeError:
            val = None
        return val

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def _setup_driver(
            self, brick_get_connector_properties=None, request=None):
        """Set up the driver environment."""
        self.driver = hbsd_fc.HBSDFCDriver(
            configuration=self.configuration)

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method == 'POST':
                    return FakeResponse(200, POST_SESSIONS_RESULT)
                elif '/ports' in url:
                    return FakeResponse(200, GET_PORTS_RESULT)
                elif '/host-wwns' in url:
                    return FakeResponse(200, GET_HOST_WWNS_RESULT)
                elif '/host-groups' in url:
                    return FakeResponse(200, GET_HOST_GROUPS_RESULT_PAIR)
            else:
                if method == 'POST':
                    return FakeResponse(200, REMOTE_POST_SESSIONS_RESULT)
                elif '/ports' in url:
                    return FakeResponse(200, REMOTE_GET_PORTS_RESULT)
                elif '/host-wwns' in url:
                    return FakeResponse(200, REMOTE_GET_HOST_WWNS_RESULT)
                elif '/host-groups' in url:
                    return FakeResponse(
                        200, REMOTE_GET_HOST_GROUPS_RESULT_PAIR)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.do_setup(None)
        self.driver.check_for_setup_error()
        self.driver.local_path(None)
        self.driver.create_export(None, None, None)
        self.driver.ensure_export(None, None)
        self.driver.remove_export(None, None)
        self.driver.create_export_snapshot(None, None, None)
        self.driver.remove_export_snapshot(None, None)
        # stop the Loopingcall within the do_setup treatment
        self.driver.common.rep_primary.client.keep_session_loop.stop()
        self.driver.common.rep_secondary.client.keep_session_loop.stop()

    def tearDown(self):
        self.client = None
        super(PF9GroupReplicationFCTest, self).tearDown()

    # ---- Phase 6 helpers -------------------------------------------------

    _GROUP_SPEC = {'consistent_group_replication_enabled': '<is> True'}

    def _group(self, specs=None, group_id=None):
        """Build a group whose type carries the given extra specs."""
        group = fake_group.fake_group_obj(
            self.ctxt, id=group_id or GROUP_ID)
        self.mock_object(
            group_types, 'get_group_type_specs',
            side_effect=lambda gtid, key=None: (specs or {}).get(key))
        return group

    def _repl_group(self, group_id=None):
        return self._group(self._GROUP_SPEC, group_id)

    def _members(self, count, first=0):
        """Volumes already carrying a P-VOL provider_location."""
        volumes = []
        for i in range(count):
            vol = fake_volume.fake_volume_obj(
                self.ctxt,
                id='00000000-0000-0000-0000-{0:012d}'.format(200 + first + i),
                size=128, provider_location=str(10 + first + i))
            vol.volume_type = fake_volume.fake_volume_type_obj(self.ctxt)
            volumes.append(vol)
        return volumes

    def _stub_common(self, copy_grps=None, svol_start=100):
        """Stub the two clients so only driver decisions are exercised."""
        common = self.driver.common
        common.rep_primary.client.get_remote_copy_grps = mock.Mock(
            return_value=copy_grps if copy_grps is not None else [])
        common.rep_primary.client.add_remote_copypair = mock.Mock()
        common.rep_primary.client.delete_remote_copypair = mock.Mock()
        common.rep_secondary.client.takeover_remote_copy_grp = mock.Mock()
        common.rep_secondary.client.resync_remote_copy_grp = mock.Mock()
        common.rep_secondary.create_ldev = mock.Mock(
            side_effect=itertools.count(svol_start))
        common.rep_secondary.delete_ldev = mock.Mock()
        common.rep_secondary.get_volume_extra_specs = mock.Mock(
            return_value={})
        common.rep_primary.get_volume_extra_specs = mock.Mock(
            return_value={})
        # QoS lookups would otherwise hit the volume-type tables.
        self.mock_object(hbsd_utils, 'get_qos_specs_from_volume',
                         return_value=None)
        # A2: an LDEV scan must never be needed to find a free S-VOL.
        common.rep_secondary.client.get_ldevs = mock.Mock(
            side_effect=AssertionError('get_ldevs scan must not be used'))
        common.rep_primary.client.get_ldevs = mock.Mock(
            side_effect=AssertionError('get_ldevs scan must not be used'))
        return common

    def _pair_bodies(self, common):
        return [c.args[1] for c in
                common.rep_primary.client.add_remote_copypair.call_args_list]

    # ---- H5 / H6: pair body, B1, A1, A2 ----------------------------------

    def test_enable_replication_pair_body(self):
        common = self._stub_common()
        group = self._repl_group()
        volumes = self._members(3)

        model_update, vol_updates = self.driver.enable_replication(
            self.ctxt, group, volumes)

        cg = common._create_group_copy_group_name(group.id)
        bodies = self._pair_bodies(common)
        self.assertEqual(3, len(bodies))
        for i, body in enumerate(bodies):
            self.assertEqual(cg, body['copyGroupName'])
            self.assertEqual(cg + 'P', body['localDeviceGroupName'])
            self.assertEqual(cg + 'S', body['remoteDeviceGroupName'])
            self.assertEqual(hbsd_utils.REP_TYPE_ASYNC,
                             body['replicationType'])
            self.assertEqual('ASYNC', body['fenceLevel'])
            self.assertEqual(
                self.configuration.hitachi_replication_mun, body['muNumber'])
            self.assertEqual(10 + i, body['pvolLdevId'])
            self.assertEqual(100 + i, body['svolLdevId'])
            # A1: journal IDs are deliberately absent from group pairs.
            self.assertNotIn('journalId', body)
            self.assertNotIn('masterJournalId', body)
            self.assertNotIn('restoreJournalId', body)
        # A2: S-VOLs come from create_ldev, never from an allocator scan.
        self.assertEqual(3, common.rep_secondary.create_ldev.call_count)
        self.assertEqual(
            fields.ReplicationStatus.ENABLED,
            model_update['replication_status'])
        for i, update in enumerate(vol_updates):
            self.assertEqual(
                fields.ReplicationStatus.ENABLED,
                update['replication_status'])
            self.assertEqual(
                {'pldev': 10 + i, 'sldev': 100 + i},
                json.loads(update['provider_location']))

    def test_enable_replication_new_group_flag_from_group_list(self):
        """B1: only the first pair creates the copy group."""
        common = self._stub_common()
        self.driver.enable_replication(
            self.ctxt, self._repl_group(), self._members(3))

        flags = [b['isNewGroupCreation'] for b in self._pair_bodies(common)]
        self.assertEqual([True, False, False], flags)
        # B3: the group list is queried once, not once per member.
        self.assertEqual(
            1, common.rep_primary.client.get_remote_copy_grps.call_count)

    def test_enable_replication_existing_copy_group_never_new(self):
        """B1: an existing copy group is joined, not recreated."""
        group = self._repl_group()
        cg = self.driver.common._create_group_copy_group_name(group.id)
        common = self._stub_common(copy_grps=[{'copyGroupName': cg}])

        self.driver.enable_replication(self.ctxt, group, self._members(2))

        flags = [b['isNewGroupCreation'] for b in self._pair_bodies(common)]
        self.assertEqual([False, False], flags)

    def test_disable_replication_deletes_pairs_not_copy_group(self):
        """B1: the array removes the copy group with its last pair."""
        common = self._stub_common()
        group = self._repl_group()
        volumes = self._members(2)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})

        model_update, vol_updates = self.driver.disable_replication(
            self.ctxt, group, volumes)

        self.assertEqual(
            2, common.rep_primary.client.delete_remote_copypair.call_count)
        self.assertEqual(
            fields.ReplicationStatus.DISABLED,
            model_update['replication_status'])
        for update in vol_updates:
            self.assertEqual(fields.ReplicationStatus.DISABLED,
                             update['replication_status'])
        self.assertFalse(
            hasattr(common.rep_primary.client, 'delete_remote_copy_grp'))

    def test_disable_then_enable_replication_cycle(self):
        """B1: re-enable works after the array auto-removed the group."""
        group = self._repl_group()
        volumes = self._members(2)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})
        common = self._stub_common()
        self.driver.disable_replication(self.ctxt, group, volumes)

        # The copy group is gone, so the next enable must recreate it.
        common = self._stub_common(copy_grps=[])
        self.driver.enable_replication(self.ctxt, group, volumes)
        flags = [b['isNewGroupCreation'] for b in self._pair_bodies(common)]
        self.assertEqual([True, False], flags)

    def test_enable_replication_one_member_fails(self):
        """One failure marks that member and the group ERROR only."""
        common = self._stub_common()
        common.rep_secondary.create_ldev = mock.Mock(
            side_effect=[100,
                         exception.VolumeDriverException(data='boom'),
                         102])

        model_update, vol_updates = self.driver.enable_replication(
            self.ctxt, self._repl_group(), self._members(3))

        statuses = [u['replication_status'] for u in vol_updates]
        self.assertEqual(
            [fields.ReplicationStatus.ENABLED,
             fields.ReplicationStatus.ERROR,
             fields.ReplicationStatus.ENABLED], statuses)
        self.assertEqual(
            fields.ReplicationStatus.ERROR,
            model_update['replication_status'])
        # The loop is not aborted: the third member still got its pair.
        self.assertEqual(
            2, common.rep_primary.client.add_remote_copypair.call_count)

    # ---- H7: return type and a single group takeover ---------------------

    def _stub_wait(self, common):
        common._wait_pair_status_change = mock.Mock()

    def test_failover_replication_returns_dict_and_list(self):
        """§5.2: manager.py calls .get() on the first element."""
        common = self._stub_common()
        self._stub_wait(common)
        volumes = self._members(3)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})

        ret = self.driver.failover_replication(
            self.ctxt, self._repl_group(), volumes,
            secondary_backend_id='backend2')

        self.assertIsInstance(ret, tuple)
        self.assertIsInstance(ret[0], dict)
        self.assertIsInstance(ret[1], list)
        # The exact call the manager makes; a bare string would raise here.
        self.assertEqual(fields.ReplicationStatus.FAILED_OVER,
                         ret[0].get('replication_status'))
        self.assertEqual(3, len(ret[1]))

    def test_failover_replication_one_group_takeover(self):
        """B2: one takeover for the group, not one per pair."""
        common = self._stub_common()
        self._stub_wait(common)
        volumes = self._members(4)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})
        group = self._repl_group()

        self.driver.failover_replication(
            self.ctxt, group, volumes, secondary_backend_id='backend2')

        takeover = common.rep_secondary.client.takeover_remote_copy_grp
        self.assertEqual(1, takeover.call_count)
        self.assertEqual(
            common._create_group_copy_group_name(group.id),
            takeover.call_args.args[1])

    def test_failback_replication_returns_dict_and_list(self):
        """§5.2 applies to the failback path too."""
        common = self._stub_common()
        self._stub_wait(common)
        volumes = self._members(2)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})

        ret = self.driver.failover_replication(
            self.ctxt, self._repl_group(), volumes,
            secondary_backend_id='default')

        self.assertIsInstance(ret[0], dict)
        self.assertIsInstance(ret[1], list)
        self.assertEqual(fields.ReplicationStatus.ENABLED,
                         ret[0].get('replication_status'))
        # A4: failback is a swap resync on the group, no takeover.
        resync = common.rep_secondary.client.resync_remote_copy_grp
        self.assertEqual(1, resync.call_count)
        self.assertTrue(resync.call_args.kwargs['swap'])
        self.assertTrue(resync.call_args.kwargs['is_secondary'])
        self.assertEqual(
            0, common.rep_secondary.client.takeover_remote_copy_grp.call_count)

    # ---- H8: configured backend_id, not the array serial -----------------

    def test_list_replication_targets_uses_configured_backend_id(self):
        """§5.6: the backend_id, never remoteStorageDeviceId."""
        group = self._repl_group()
        cg = self.driver.common._create_group_copy_group_name(group.id)
        common = self._stub_common(copy_grps=[{'copyGroupName': cg}])

        ret = self.driver.list_replication_targets(self.ctxt, group)

        expected = self.configuration.replication_device[0]['backend_id']
        self.assertEqual(
            {'replication_targets': [{'backend_id': expected}]}, ret)
        self.assertNotEqual(expected, common.rep_secondary.storage_id)
        self.assertNotIn(REMOTE_CONFIG_MAP['serial'], json.dumps(ret))

    def test_list_replication_targets_empty_when_group_absent(self):
        self._stub_common(copy_grps=[])
        ret = self.driver.list_replication_targets(
            self.ctxt, self._repl_group())
        self.assertEqual({'replication_targets': []}, ret)

    # ---- H1-H3: non-replicated objects reach the upstream body (§5.8) ----

    def _plain_volume(self, group_id=None):
        vol = fake_volume.fake_volume_obj(
            self.ctxt, id='00000000-0000-0000-0000-000000000300',
            size=128, provider_location='1')
        vol.volume_type = fake_volume.fake_volume_type_obj(self.ctxt)
        vol.group_id = group_id
        return vol

    def test_manage_existing_non_replicated_uses_upstream(self):
        common = self.driver.common
        common.rep_primary.manage_existing = mock.Mock(
            return_value={'provider_location': '1'})
        common._group_repl_manage_existing = mock.Mock(
            side_effect=AssertionError('group path must not run'))

        self.driver.manage_existing(self._plain_volume(), {'source-id': '1'})

        common.rep_primary.manage_existing.assert_called_once()

    def test_manage_existing_get_size_non_replicated_uses_upstream(self):
        common = self.driver.common
        common.rep_primary.manage_existing_get_size = mock.Mock(
            return_value=128)
        common._has_rep_pair = mock.Mock(return_value=False)
        common._group_repl_manage_existing_get_size = mock.Mock(
            side_effect=AssertionError('group path must not run'))

        size = self.driver.manage_existing_get_size(
            self._plain_volume(), {'source-id': '1'})

        self.assertEqual(128, size)
        common.rep_primary.manage_existing_get_size.assert_called_once()

    def test_unmanage_non_replicated_uses_upstream(self):
        common = self.driver.common
        common._verify_ldev = mock.Mock()
        common._has_rep_pair = mock.Mock(return_value=False)
        common.rep_primary.unmanage = mock.Mock()
        common._group_repl_unmanage = mock.Mock(
            side_effect=AssertionError('group path must not run'))

        self.driver.unmanage(self._plain_volume())

        common.rep_primary.unmanage.assert_called_once()

    def test_update_group_non_replicated_uses_upstream(self):
        common = self.driver.common
        common._verify_ldev = mock.Mock()
        common._has_rep_pair = mock.Mock(return_value=False)
        common.rep_primary.update_group = mock.Mock(
            return_value=(None, None, None))
        common._group_repl_update_group = mock.Mock(
            side_effect=AssertionError('group path must not run'))
        # A plain consistency group, not a group-replication one.
        group = self._group({'consistent_group_snapshot_enabled': '<is> True'})

        self.driver.update_group(
            self.ctxt, group, add_volumes=[self._plain_volume()],
            remove_volumes=[])

        common.rep_primary.update_group.assert_called_once()

    def test_create_group_snapshot_non_replicated_uses_upstream(self):
        common = self.driver.common
        common._verify_ldev = mock.Mock()
        common.rep_primary.create_group_snapshot = mock.Mock(
            return_value=(None, []))
        common._group_repl_create_group_snapshot = mock.Mock(
            side_effect=AssertionError('group path must not run'))
        group_snapshot = self._group_snapshot(
            self._group({'consistent_group_snapshot_enabled': '<is> True'}))

        self.driver.create_group_snapshot(self.ctxt, group_snapshot, [])

        common.rep_primary.create_group_snapshot.assert_called_once()

    def test_guards_route_group_replication_objects(self):
        """The same calls on a replicated group take the group path."""
        common = self.driver.common
        group = self._repl_group()
        vol = self._plain_volume(group_id=group.id)
        vol.group = group
        common._group_repl_unmanage = mock.Mock(return_value=None)
        common.rep_primary.unmanage = mock.Mock(
            side_effect=AssertionError('upstream path must not run'))

        self.driver.unmanage(vol)

        common._group_repl_unmanage.assert_called_once_with(vol)

    # ---- H4: one split per group snapshot, delete removes everything -----

    def _group_snapshot(self, group):
        group_snapshot = fake_group_snapshot.fake_group_snapshot_obj(
            self.ctxt, id=GROUP_SNAPSHOT_ID, group_id=group.id)
        group_snapshot.group = group
        return group_snapshot

    def _snapshots(self, volumes):
        snapshots = []
        for i, vol in enumerate(volumes):
            snap = fake_snapshot.fake_snapshot_obj(
                self.ctxt,
                id='10000000-0000-0000-0000-{0:012d}'.format(400 + i),
                volume_id=vol.id, volume_size=128)
            snap.volume = vol
            snapshots.append(snap)
        return snapshots

    def _stub_snapshot_paths(self, common):
        secondary = common.rep_secondary
        secondary.create_ldev = mock.Mock(side_effect=itertools.count(500))
        secondary.modify_ldev_name = mock.Mock()
        secondary.delete_ldev = mock.Mock()
        secondary.get_volume_extra_specs = mock.Mock(return_value={})
        secondary._get_snap_pool_id = mock.Mock(return_value=40)
        secondary._wait_copy_pair_status = mock.Mock()
        secondary.client.add_snapshot = mock.Mock()
        secondary.client.split_snapshotgroup = mock.Mock()
        # Upstream's rollback queries the pairs it has to tear down.
        secondary.client.get_snapshots = mock.Mock(return_value=[])
        return secondary

    def test_create_group_snapshot_one_split_for_the_group(self):
        """B4: N add_snapshot calls but a single group split."""
        common = self._stub_common()
        secondary = self._stub_snapshot_paths(common)
        group = self._repl_group()
        volumes = self._members(4)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})
        snapshots = self._snapshots(volumes)

        model_update, snap_updates = self.driver.create_group_snapshot(
            self.ctxt, self._group_snapshot(group), snapshots)

        self.assertEqual(4, secondary.client.add_snapshot.call_count)
        self.assertEqual(1, secondary.client.split_snapshotgroup.call_count)
        bodies = [c.args[0] for c in
                  secondary.client.add_snapshot.call_args_list]
        # 4.1: all members share one CTG-flagged snapshot group.
        self.assertEqual(1, len({b['snapshotGroupName'] for b in bodies}))
        for body in bodies:
            self.assertTrue(body['isConsistencyGroup'])
            # 4.2/4.3: the group is split once, never per member.
            self.assertNotIn('autoSplit', body)
        self.assertEqual(
            bodies[0]['snapshotGroupName'],
            secondary.client.split_snapshotgroup.call_args.args[0])
        # 4.4: sldev locations are what let delete find these on the secondary.
        self.assertIsNone(model_update)
        for update in snap_updates:
            loc = json.loads(update['provider_location'])
            self.assertIn('sldev', loc)
            self.assertNotIn('pldev', loc)

    def test_create_group_snapshot_snapshots_taken_on_secondary(self):
        common = self._stub_common()
        secondary = self._stub_snapshot_paths(common)
        common.rep_primary.create_ldev = mock.Mock(
            side_effect=AssertionError('snapshots belong on the secondary'))
        volumes = self._members(2)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})

        self.driver.create_group_snapshot(
            self.ctxt, self._group_snapshot(self._repl_group()),
            self._snapshots(volumes))

        # The Thin Image P-VOL is the replication S-VOL on the secondary.
        pvols = [c.args[0]['pvolLdevId'] for c in
                 secondary.client.add_snapshot.call_args_list]
        self.assertEqual([100, 101], pvols)

    def test_delete_group_snapshot_removes_everything(self):
        """B4: every S-VOL taken on the secondary is deleted again."""
        common = self._stub_common()
        secondary = common.rep_secondary
        secondary.delete_snapshot = mock.Mock()
        common.rep_primary.delete_snapshot = mock.Mock(
            side_effect=AssertionError('deletes belong on the secondary'))
        volumes = self._members(3)
        snapshots = self._snapshots(volumes)
        for i, snap in enumerate(snapshots):
            snap.provider_location = json.dumps({'sldev': 500 + i})
        group_snapshot = self._group_snapshot(self._repl_group())

        model_update, snap_updates = self.driver.delete_group_snapshot(
            self.ctxt, group_snapshot, snapshots)

        self.assertEqual(3, secondary.delete_snapshot.call_count)
        deleted = {c.args[0].id for c in
                   secondary.delete_snapshot.call_args_list}
        self.assertEqual({s.id for s in snapshots}, deleted)
        for update in snap_updates:
            self.assertEqual('deleted', update['status'])

    # ---- A2: no free-LDEV scan anywhere in the group paths ---------------

    def test_no_get_ldevs_scan_in_group_replication_code(self):
        """A2: create_ldev allocates; nothing scans for a free LDEV."""
        source = inspect.getsource(hbsd_replication)
        for name in ('_group_repl_add_volume',
                     '_group_repl_create_group_snapshot',
                     '_group_repl_manage_existing'):
            start = source.index('def %s' % name)
            end = source.index('\n    def ', start + 1)
            body = source[start:end]
            self.assertNotIn('get_ldevs', body)
        self.assertIn('create_ldev', source)

    # ---- Guard cost and robustness (found by the upstream suite) ---------

    def test_group_snapshot_guard_does_not_load_the_group(self):
        """The guard must key on group_type_id, never lazy-load .group."""
        common = self.driver.common
        common._group_repl_create_group_snapshot = mock.Mock(
            return_value=(None, []))
        self.mock_object(
            group_types, 'get_group_type_specs',
            side_effect=lambda gtid, key=None: self._GROUP_SPEC.get(key))
        group_snapshot = fake_group_snapshot.fake_group_snapshot_obj(
            self.ctxt, id=GROUP_SNAPSHOT_ID, group_id=GROUP_ID)
        group_snapshot.obj_reset_changes(fields=['group'])
        if group_snapshot.obj_attr_is_set('group'):
            delattr(group_snapshot, '_obj_group')

        self.driver.create_group_snapshot(self.ctxt, group_snapshot, [])

        common._group_repl_create_group_snapshot.assert_called_once()
        # A lazy load here is a database round trip on every group snapshot.
        self.assertFalse(group_snapshot.obj_attr_is_set('group'))

    def test_guard_falls_through_when_group_type_missing(self):
        """An unreadable group type must not break the upstream path."""
        common = self.driver.common
        common._verify_ldev = mock.Mock()
        common._has_rep_pair = mock.Mock(return_value=False)
        common.rep_primary.update_group = mock.Mock(
            return_value=(None, None, None))
        common._group_repl_update_group = mock.Mock(
            side_effect=AssertionError('group path must not run'))
        self.mock_object(
            group_types, 'get_group_type_specs',
            side_effect=exception.GroupTypeNotFound(group_type_id='gone'))

        self.driver.update_group(
            self.ctxt, fake_group.fake_group_obj(self.ctxt, id=GROUP_ID),
            add_volumes=[self._plain_volume()], remove_volumes=[])

        common.rep_primary.update_group.assert_called_once()

    # ---- H1-H3 group paths (the positive side of the guards) -------------

    def _member_volume(self, group, provider_location='{"sldev": 100}'):
        vol = self._plain_volume(group_id=group.id)
        vol.group = group
        vol.provider_location = provider_location
        return vol

    def test_group_repl_update_group_adds_and_removes(self):
        """H3: adds join the existing copy group, removes drop their pairs."""
        group = self._repl_group()
        cg = self.driver.common._create_group_copy_group_name(group.id)
        common = self._stub_common(copy_grps=[{'copyGroupName': cg}])
        add = self._members(2)
        remove = self._members(1, first=50)
        for vol in remove:
            vol.provider_location = json.dumps({'pldev': 60, 'sldev': 160})

        model_update, add_up, rm_up = self.driver.update_group(
            self.ctxt, group, add_volumes=add, remove_volumes=remove)

        bodies = self._pair_bodies(common)
        self.assertEqual(2, len(bodies))
        # The copy group already exists, so nothing recreates it.
        self.assertEqual([False, False],
                         [b['isNewGroupCreation'] for b in bodies])
        self.assertEqual(
            1, common.rep_primary.client.delete_remote_copypair.call_count)
        self.assertEqual(fields.GroupStatus.AVAILABLE, model_update['status'])
        self.assertEqual(2, len(add_up))
        self.assertEqual(1, len(rm_up))

    def test_group_repl_update_group_marks_group_error_on_failure(self):
        group = self._repl_group()
        cg = self.driver.common._create_group_copy_group_name(group.id)
        common = self._stub_common(copy_grps=[{'copyGroupName': cg}])
        common.rep_secondary.create_ldev = mock.Mock(
            side_effect=exception.VolumeDriverException(data='boom'))

        model_update, add_up, _rm = self.driver.update_group(
            self.ctxt, group, add_volumes=self._members(1), remove_volumes=[])

        self.assertEqual(fields.GroupStatus.ERROR, model_update['status'])
        self.assertEqual(fields.ReplicationStatus.ERROR,
                         add_up[0]['replication_status'])

    def test_group_repl_manage_existing_adopts_svol(self):
        """H1: the promoted S-VOL is adopted on the secondary."""
        common = self._stub_common()
        secondary = common.rep_secondary
        secondary.check_ldev_manageability = mock.Mock()
        secondary.modify_ldev_name = mock.Mock()
        secondary.get_qos_specs_from_ldev = mock.Mock(return_value=None)
        secondary.change_qos_specs = mock.Mock()
        common.rep_primary.manage_existing = mock.Mock(
            side_effect=AssertionError('upstream path must not run'))
        volume = self._member_volume(self._repl_group())

        ret = self.driver.manage_existing(volume, {'source-id': '77'})

        self.assertEqual({'sldev': 77},
                         json.loads(ret['provider_location']))
        secondary.check_ldev_manageability.assert_called_once()
        secondary.modify_ldev_name.assert_called_once_with(
            77, volume['id'].replace('-', ''))
        # §3.4: adoption must not export the volume.
        self.assertFalse(hasattr(secondary, 'map_ldev_called'))

    def test_group_repl_manage_existing_get_size_uses_secondary(self):
        common = self._stub_common()
        common.rep_secondary.get_ldev_size_in_gigabyte = mock.Mock(
            return_value=128)
        common.rep_primary.manage_existing_get_size = mock.Mock(
            side_effect=AssertionError('upstream path must not run'))

        size = self.driver.manage_existing_get_size(
            self._member_volume(self._repl_group()), {'source-id': '77'})

        self.assertEqual(128, size)
        common.rep_secondary.get_ldev_size_in_gigabyte.assert_called_once_with(
            77, {'source-id': '77'})

    def test_group_repl_manage_existing_bad_ref_raises_before_size(self):
        """The LDEV must resolve before blockCapacity is ever read."""
        common = self._stub_common()
        common.rep_secondary.get_ldev_size_in_gigabyte = mock.Mock(
            side_effect=AssertionError('size read before the ref resolved'))

        self.assertRaises(
            exception.ManageExistingInvalidReference,
            self.driver.manage_existing_get_size,
            self._member_volume(self._repl_group()),
            {'source-id': 'not-a-num'})

    def test_group_repl_unmanage_clears_nickname_keeps_pair(self):
        """H2: Cinder's claim is released, the copy pair is left alone."""
        common = self._stub_common()
        secondary = common.rep_secondary
        secondary.modify_ldev_name = mock.Mock()
        common.rep_primary.unmanage = mock.Mock(
            side_effect=AssertionError('upstream path must not run'))

        self.driver.unmanage(self._member_volume(self._repl_group()))

        secondary.modify_ldev_name.assert_called_once_with(100, '')
        self.assertEqual(
            0, common.rep_primary.client.delete_remote_copypair.call_count)
        self.assertEqual(0, secondary.delete_ldev.call_count)

    def test_group_repl_unmanage_survives_nickname_failure(self):
        common = self._stub_common()
        common.rep_secondary.modify_ldev_name = mock.Mock(
            side_effect=exception.VolumeDriverException(data='boom'))

        # A nickname that cannot be cleared is warned about, not fatal.
        self.driver.unmanage(self._member_volume(self._repl_group()))

    def test_create_group_snapshot_rolls_back_svols_on_failure(self):
        """4.4: a failed group snapshot must not leak S-VOLs."""
        common = self._stub_common()
        secondary = self._stub_snapshot_paths(common)
        secondary.client.split_snapshotgroup = mock.Mock(
            side_effect=exception.VolumeDriverException(data='boom'))
        volumes = self._members(3)
        for i, vol in enumerate(volumes):
            vol.provider_location = json.dumps(
                {'pldev': 10 + i, 'sldev': 100 + i})

        model_update, snap_updates = self.driver.create_group_snapshot(
            self.ctxt, self._group_snapshot(self._repl_group()),
            self._snapshots(volumes))

        self.assertEqual(fields.GroupSnapshotStatus.ERROR,
                         model_update['status'])
        for update in snap_updates:
            self.assertEqual(fields.SnapshotStatus.ERROR, update['status'])
        # Every S-VOL created for the attempt is deleted again.
        self.assertEqual(3, secondary.delete_ldev.call_count)

    def test_create_group_snapshot_errors_when_pvol_missing(self):
        common = self._stub_common()
        self._stub_snapshot_paths(common)
        volumes = self._members(1)
        volumes[0].provider_location = None

        model_update, _snap = self.driver.create_group_snapshot(
            self.ctxt, self._group_snapshot(self._repl_group()),
            self._snapshots(volumes))

        self.assertEqual(fields.GroupSnapshotStatus.ERROR,
                         model_update['status'])

    def test_delete_group_snapshot_reports_failure(self):
        common = self._stub_common()
        common.rep_secondary._delete_group = mock.Mock(
            side_effect=exception.VolumeDriverException(data='boom'))

        self.assertRaises(
            exception.VolumeDriverException,
            self.driver.delete_group_snapshot,
            self.ctxt, self._group_snapshot(self._repl_group()), [])
