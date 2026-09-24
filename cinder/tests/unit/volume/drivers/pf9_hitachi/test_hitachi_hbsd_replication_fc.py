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
"""Unit tests for Hitachi HBSD Driver."""

from datetime import timedelta
import json
import os
import tempfile
import types as pytypes
from unittest import mock

import ddt
import futurist
from oslo_config import cfg
import requests

from cinder import context as cinder_context
from cinder.db import api as sqlalchemy_api
from cinder import exception
from cinder import objects
from cinder.objects import fields
from cinder.objects import group_snapshot as obj_group_snap
from cinder.objects import snapshot as obj_snap
from cinder.tests.unit import fake_group
from cinder.tests.unit import fake_group_snapshot
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder.tests.unit import test
from cinder.volume import configuration as conf
from cinder.volume import driver
from cinder.volume.drivers.hitachi import hbsd_common
from cinder.volume.drivers.hitachi import hbsd_fc
from cinder.volume.drivers.hitachi import hbsd_replication
from cinder.volume.drivers.hitachi import hbsd_rest
from cinder.volume.drivers.hitachi import hbsd_rest_api
from cinder.volume.drivers.hitachi import hbsd_rest_fc
from cinder.volume.drivers.hitachi import hbsd_utils
from cinder.volume import group_types
from cinder.volume import volume_types
from cinder.volume import volume_utils
from cinder.zonemanager import utils as fczm_utils

# Configuration parameter values
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

GET_LDEV_RESULT_DRS = {
    "emulationType": "OPEN-V-CVS",
    "blockCapacity": 2097152,
    "attributes": ["CVS", "HDP", "DRS"],
    "status": "NML",
    "poolId": 30,
    "dataReductionStatus": "ENABLED",
    "dataReductionMode": "compression_deduplication",
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


@ddt.ddt
class HBSDREPLICATIONFCDriverTest(test.TestCase):
    """Unit test class for HBSD REPLICATION interface fibre channel module."""

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
        super(HBSDREPLICATIONFCDriverTest, self).setUp()
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
            "cinder.volume.drivers.hitachi.hbsd_fc.HBSDFCDriver",
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
        super(HBSDREPLICATIONFCDriverTest, self).tearDown()

    # API test cases
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def test_do_setup(self, brick_get_connector_properties, request):
        drv = hbsd_fc.HBSDFCDriver(
            configuration=self.configuration)
        self._setup_config()

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
        drv.do_setup(None)
        self.assertEqual(
            {CONFIG_MAP['port_id']: CONFIG_MAP['target_wwn']},
            drv.common.rep_primary.storage_info['wwns'])
        self.assertEqual(
            {REMOTE_CONFIG_MAP['port_id']: REMOTE_CONFIG_MAP['target_wwn']},
            drv.common.rep_secondary.storage_info['wwns'])
        self.assertEqual(2, brick_get_connector_properties.call_count)
        self.assertEqual(8, request.call_count)
        # stop the Loopingcall within the do_setup treatment
        drv.common.rep_primary.client.keep_session_loop.stop()
        drv.common.rep_secondary.client.keep_session_loop.stop()

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_create_volume(
            self, get_volume_type_extra_specs,
            get_volume_type_qos_specs, is_replicated, request):
        is_replicated.return_value = False
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {}
        request.return_value = FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_volume(TEST_VOLUME[8])
        actual = {'provider_location': json.dumps({'pldev': 1}),
                  'replication_status': fields.ReplicationStatus.DISABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(2, request.call_count)
        args, kwargs = request.call_args
        self.assertNotIn(
            'Job-Mode-Wait-Configuration-Change', kwargs['headers'])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_volume_replication(
            self, get_volume_type_qos_specs, is_replicated,
            get_volume_type_extra_specs, request):
        is_replicated.return_value = True
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_volume(TEST_VOLUME[8])
        actual = {
            'provider_location': json.dumps(
                {'pldev': 1, 'sldev': 2,
                 'remote-copy': hbsd_utils.REP_TYPE_ASYNC}),
            'replication_status': fields.ReplicationStatus.ENABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(23, request.call_count)
        for args, kwargs in request.call_args_list:
            if args[0] == 'POST' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual('U', kwargs['json']['copyGroupName'][-3])
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                break
        else:
            self.fail('no create pair api')

    @mock.patch.object(requests.Session, "request")
    def test_delete_volume(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.delete_volume(TEST_VOLUME[0])
        self.assertEqual(5, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_delete_volume_replication(self, request):
        self.copygroup_count = 0
        self.ldev_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copygroups/' in url:
                        if self.copygroup_count < 2:
                            self.copygroup_count = self.copygroup_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYGROUP_RESULT)
                        else:
                            return FakeResponse(
                                500, GET_REMOTE_MIRROR_COPYGROUP_RESULT_ERROR,
                                headers={'Content-Type': 'json'})
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        if self.ldev_count == 0:
                            self.ldev_count = self.ldev_count + 1
                            return FakeResponse(200, GET_LDEV_RESULT_REP)
                        else:
                            return FakeResponse(200, GET_LDEV_RESULT_SPLIT)
                    elif '/journals/' in url:
                        return FakeResponse(200, GET_JOURNAL_RESULT)
            else:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/journals/' in url:
                        return FakeResponse(200, REMOTE_GET_JOURNAL_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_SPLIT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.delete_volume(TEST_VOLUME[4])
        self.assertEqual(29, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_delete_volume_primary_is_invalid_ldev(self, request):
        request.return_value = FakeResponse(200, GET_LDEV_RESULT_LABEL)
        self.driver.delete_volume(TEST_VOLUME[0])
        self.assertEqual(1, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_delete_volume_primary_secondary_is_invalid_ldev(self, request):
        request.return_value = FakeResponse(200, GET_LDEV_RESULT_REP_LABEL)
        self.driver.delete_volume(TEST_VOLUME[4])
        self.assertEqual(2, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_delete_volume_secondary_is_invalid_ldev(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_REP_LABEL),
                               FakeResponse(200, GET_LDEV_RESULT_REP),
                               FakeResponse(200, GET_LDEV_RESULT_REP),
                               FakeResponse(200, GET_LDEV_RESULT_REP),
                               FakeResponse(200, GET_LDEV_RESULT_REP),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.delete_volume(TEST_VOLUME[4])
        self.assertEqual(6, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_extend_volume(self, get_volume_type_qos_specs, request):
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.extend_volume(TEST_VOLUME[0], 256)
        self.assertEqual(6, request.call_count)
        body = request.call_args_list[5][1]['json']
        self.assertNotIn('enhancedExpansion', body['parameters'])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_extend_volume_replication(
            self, get_volume_type_extra_specs, get_volume_type_qos_specs,
            request):
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        self.ldev_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copygroups/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT)
                    elif '/remote-mirror-copygroups' in url:
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        if self.ldev_count < 2:
                            self.ldev_count = self.ldev_count + 1
                            return FakeResponse(200, GET_LDEV_RESULT_REP)
                        else:
                            return FakeResponse(200, GET_LDEV_RESULT)
                    elif '/journals/' in url:
                        return FakeResponse(200, GET_JOURNAL_RESULT)
            else:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/journals/' in url:
                        return FakeResponse(200, REMOTE_GET_JOURNAL_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.extend_volume(TEST_VOLUME[4], 256)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(27, request.call_count)
        body = request.call_args_list[14][1]['json']
        self.assertNotIn('enhancedExpansion', body['parameters'])
        body = request.call_args_list[19][1]['json']
        self.assertNotIn('enhancedExpansion', body['parameters'])
        for args, kwargs in request.call_args_list:
            if args[0] == 'POST' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                isDataReductionForceCopy = (
                    kwargs['json']['isDataReductionForceCopy'])
                break
        else:
            self.fail('no create pair api')
        self.assertFalse(isDataReductionForceCopy)

    @mock.patch.object(hbsd_common.HBSDCommon, "delete_pair")
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(requests.Session, "request")
    def test_extend_volume_enable_having_snapshots(
            self, request, get_volume_type_extra_specs,
            get_volume_type_qos_specs, delete_pair):
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        self.override_config('hitachi_extend_snapshot_volumes',
                             True, group=conf.SHARED_CONF_GROUP)
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        self.ldev_count = 0
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copygroups/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT)
                    elif '/remote-mirror-copygroups' in url:
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        if self.ldev_count < 2:
                            self.ldev_count = self.ldev_count + 1
                            return FakeResponse(200, GET_LDEV_RESULT_REP_PAIR)
                        else:
                            return FakeResponse(200, GET_LDEV_RESULT)
            else:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.extend_volume(TEST_VOLUME[4], 256)
        self.assertEqual(24, request.call_count)
        body = request.call_args_list[12][1]['json']
        self.assertTrue(body['parameters']['enhancedExpansion'])
        body = request.call_args_list[16][1]['json']
        self.assertTrue(body['parameters']['enhancedExpansion'])

    @ddt.data('deduplication_compression', 'compression')
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_extend_pair_volume_capacity_saving_dedup_compression(
            self, csv, get_volume_type_extra_specs, get_volume_type_qos_specs,
            request):
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'hbsd:capacity_saving': csv}
        self.ldev_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copygroups/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT)
                    elif '/remote-mirror-copygroups' in url:
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        if self.ldev_count < 2:
                            self.ldev_count = self.ldev_count + 1
                            return FakeResponse(200, GET_LDEV_RESULT_REP)
                        else:
                            return FakeResponse(200, GET_LDEV_RESULT)
                    elif '/journals/' in url:
                        return FakeResponse(200, GET_JOURNAL_RESULT)
            else:
                if method in ('POST', 'PUT', 'DELETE'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/journals/' in url:
                        return FakeResponse(200, REMOTE_GET_JOURNAL_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.extend_volume(TEST_VOLUME[4], 256)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(27, request.call_count)
        body = request.call_args_list[14][1]['json']
        self.assertNotIn('enhancedExpansion', body['parameters'])
        body = request.call_args_list[19][1]['json']
        self.assertNotIn('enhancedExpansion', body['parameters'])
        for args, kwargs in request.call_args_list:
            if args[0] == 'POST' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                isDataReductionForceCopy = (
                    kwargs['json']['isDataReductionForceCopy'])
                break
        else:
            self.fail('no create pair api')
        self.assertTrue(isDataReductionForceCopy)

    @mock.patch.object(driver.FibreChannelDriver, "get_goodness_function")
    @mock.patch.object(driver.FibreChannelDriver, "get_filter_function")
    @mock.patch.object(requests.Session, "request")
    def test_get_volume_stats(
            self, request, get_filter_function, get_goodness_function):
        request.return_value = FakeResponse(200, GET_POOLS_RESULT)
        get_filter_function.return_value = None
        get_goodness_function.return_value = None
        stats = self.driver.get_volume_stats(True)
        self.assertEqual('Hitachi', stats['vendor_name'])
        self.assertTrue(stats["pools"][0]['multiattach'])
        # B1: hitachi_replication_report_pair_status defaults to False, so
        # this no longer makes the extra get_remote_copy_grps call - just
        # the one GET_POOLS_RESULT request.
        self.assertEqual(1, request.call_count)
        self.assertEqual(1, get_filter_function.call_count)
        self.assertEqual(1, get_goodness_function.call_count)
        self.assertTrue(stats['consistent_group_replication_enabled'])
        self.assertTrue(stats['group_replication_enabled'])
        pool = stats['pools'][0]
        self.assertTrue(pool['consistent_group_replication_enabled'])
        self.assertTrue(pool['group_replication_enabled'])
        self.assertTrue(pool[hbsd_replication._PAIR_STATUS_PEER_KEY])
        # B1: group_replication_pairs itself is only populated when
        # hitachi_replication_report_pair_status is enabled (see
        # test_update_volume_stats_pair_status_when_enabled and the
        # _pair_status_capabilities cache tests).
        self.assertNotIn(hbsd_replication._PAIR_STATUS_KEY, pool)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(sqlalchemy_api, 'volume_get', side_effect=_volume_get)
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_snapshot(
            self, get_volume_type_qos_specs, volume_get,
            get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_snapshot(TEST_SNAPSHOT[0])
        actual = {'provider_location': json.dumps({'pldev': 1})}
        self.assertEqual(actual, ret)
        self.assertEqual(5, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_delete_snapshot(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_PAIR),
                               FakeResponse(200, NOTFOUND_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.delete_snapshot(TEST_SNAPSHOT[0])
        self.assertEqual(14, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch(
        'cinder.volume.drivers.hitachi.hbsd_rest.HBSDREST.'
        '_wait_copy_pair_status')
    def test_create_cloned_volume(
            self, mock_wait_copy_pair_status, get_volume_type_qos_specs,
            is_replicated, get_volume_type_extra_specs, get_volume_type,
            request):
        mock_wait_copy_pair_status.return_value = None
        is_replicated.return_value = False
        get_volume_type_extra_specs.return_value = {}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if method == 'GET':
                if '/ldevs/' in url:
                    return FakeResponse(200, GET_LDEV_RESULT)
                elif '/snapshots' in url:
                    return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                else:
                    return FakeResponse(200, NOTFOUND_RESULT)
            elif method in ('POST', 'PUT'):
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            elif method == 'DELETE':
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            return FakeResponse(500, ERROR_RESULT,
                                headers={'Content-Type': 'json'})

        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_cloned_volume(TEST_VOLUME[0], TEST_VOLUME[1])
        actual = {'provider_location': json.dumps({'pldev': 1}),
                  'replication_status': fields.ReplicationStatus.DISABLED}
        self.assertEqual(actual, ret)
        self.assertGreater(request.call_count, 0)  # Just verify requests
        # Check that the last call doesn't have
        # Job-Mode-Wait-Configuration-Change header
        if request.call_count > 0:
            args, kwargs = request.call_args_list[-1]
            if 'headers' in kwargs:
                self.assertNotIn(
                    'Job-Mode-Wait-Configuration-Change', kwargs['headers'])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_cloned_volume_replication(
            self, get_volume_type_qos_specs, is_replicated,
            get_volume_type_extra_specs, get_volume_type, request):
        is_replicated.return_value = True
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        self.snapshot_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/snapshots' in url:
                        if self.snapshot_count < 1:
                            self.snapshot_count = self.snapshot_count + 1
                            return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                        else:
                            return FakeResponse(200, NOTFOUND_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_cloned_volume(TEST_VOLUME[4], TEST_VOLUME[5])
        actual = {
            'provider_location': json.dumps(
                {'pldev': 1, 'sldev': 2,
                 'remote-copy': hbsd_utils.REP_TYPE_ASYNC}),
            'replication_status': fields.ReplicationStatus.ENABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(2, get_volume_type_extra_specs.call_count)
        self.assertEqual(34, request.call_count)
        for args, kwargs in request.call_args_list:
            if args[0] == 'POST' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                isDataReductionForceCopy = (
                    kwargs['json']['isDataReductionForceCopy'])
                break
        else:
            self.fail('no create pair api')
        self.assertFalse(isDataReductionForceCopy)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch(
        'cinder.volume.drivers.hitachi.hbsd_rest.HBSDREST.'
        '_wait_copy_pair_status')
    def test_create_volume_from_snapshot(
            self, mock_wait_copy_pair_status, get_volume_type_qos_specs,
            is_replicated, get_volume_type_extra_specs, get_volume_type,
            request):
        mock_wait_copy_pair_status.return_value = None
        is_replicated.return_value = False
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if method == 'GET':
                if '/ldevs/' in url:
                    return FakeResponse(200, GET_LDEV_RESULT)
                elif '/snapshots' in url:
                    return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                else:
                    return FakeResponse(200, NOTFOUND_RESULT)
            elif method in ('POST', 'PUT'):
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            elif method == 'DELETE':
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            return FakeResponse(500, ERROR_RESULT,
                                headers={'Content-Type': 'json'})

        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_volume_from_snapshot(
            TEST_VOLUME[0], TEST_SNAPSHOT[0])
        actual = {'provider_location': json.dumps({'pldev': 1}),
                  'replication_status': fields.ReplicationStatus.DISABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(2, get_volume_type_extra_specs.call_count)
        self.assertEqual(6, request.call_count)
        args, kwargs = request.call_args_list[5]
        self.assertNotIn(
            'Job-Mode-Wait-Configuration-Change', kwargs['headers'])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch(
        'cinder.volume.drivers.hitachi.hbsd_rest.HBSDREST.'
        '_wait_copy_pair_status')
    @mock.patch(
        'cinder.volume.drivers.hitachi.hbsd_rest.HBSDREST.'
        '_wait_copy_pair_deleting')
    def test_create_volume_from_snapshot_replication(
            self, mock_wait_copy_pair_deleting, mock_wait_copy_pair_status,
            get_volume_type_qos_specs, is_replicated,
            get_volume_type_extra_specs, get_volume_type, request):
        mock_wait_copy_pair_deleting.return_value = None
        mock_wait_copy_pair_status.return_value = None
        is_replicated.return_value = True
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if method == 'GET':
                if '/ldevs/' in url:
                    if '456789' in url:  # Remote storage
                        return FakeResponse(200, GET_LDEV_RESULT)
                    else:  # Primary storage
                        return FakeResponse(200, GET_LDEV_RESULT_PAIR)
                elif '/snapshots' in url:
                    # For delete_pair operations, return NOTFOUND to
                    # indicate no pairs
                    if params and 'pvolLdevId' in params:
                        return FakeResponse(200, NOTFOUND_RESULT)
                    else:
                        return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                elif '/remote-mirror-copygroups/' in url:
                    return FakeResponse(
                        200,
                        GET_REMOTE_MIRROR_COPYGROUPS_RESULT_PAIR)
                elif '/remote-mirror-copypairs/' in url:
                    return FakeResponse(200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                elif '/journals' in url:
                    return FakeResponse(200, NOTFOUND_RESULT)
                else:
                    return FakeResponse(200, NOTFOUND_RESULT)
            elif method in ('POST', 'PUT'):
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            elif method == 'DELETE':
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            return FakeResponse(500, ERROR_RESULT,
                                headers={'Content-Type': 'json'})

        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_volume_from_snapshot(
            TEST_VOLUME[4], TEST_SNAPSHOT[4])
        actual = {
            'provider_location': json.dumps(
                {'pldev': 1, 'sldev': 1,
                 'remote-copy': hbsd_utils.REP_TYPE_ASYNC}),
            'replication_status': fields.ReplicationStatus.ENABLED}
        self.assertEqual(actual, ret)
        self.assertGreater(request.call_count, 0)  # Just verify requests
        # Check that one of the calls has the expected
        # Job-Mode-Wait-Configuration-Change header
        found_no_wait_header = False
        for args, kwargs in request.call_args_list:
            if ('headers' in kwargs and
                    'Job-Mode-Wait-Configuration-Change' in kwargs['headers']):
                if (kwargs['headers']['Job-Mode-Wait-Configuration-Change'] ==
                        "NoWait"):
                    found_no_wait_header = True
                    break
        self.assertTrue(
            found_no_wait_header,
            "Expected Job-Mode-Wait-Configuration-Change: NoWait header "
            "not found")

    @mock.patch.object(fczm_utils, "add_fc_zone")
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_initialize_connection(
            self, get_volume_type_extra_specs, request, add_fc_zone):
        self.override_config('hitachi_zoning_request', True,
                             group=conf.SHARED_CONF_GROUP)
        self.driver.common.lookup_service = FakeLookupService()
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        request.side_effect = [FakeResponse(200, GET_HOST_WWNS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        ret = self.driver.initialize_connection(
            TEST_VOLUME[0], DEFAULT_CONNECTOR)
        self.assertEqual('fibre_channel', ret['driver_volume_type'])
        self.assertEqual([CONFIG_MAP['target_wwn']], ret['data']['target_wwn'])
        self.assertEqual(1, ret['data']['target_lun'])
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(2, request.call_count)
        self.assertEqual(1, add_fc_zone.call_count)

    @mock.patch.object(fczm_utils, "remove_fc_zone")
    @mock.patch.object(requests.Session, "request")
    def test_terminate_connection(self, request, remove_fc_zone):
        self.override_config('hitachi_zoning_request', True,
                             group=conf.SHARED_CONF_GROUP)
        self.driver.common.lookup_service = FakeLookupService()
        request.side_effect = [FakeResponse(200, GET_HOST_WWNS_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT_MAPPED),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, NOTFOUND_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.terminate_connection(TEST_VOLUME[2], DEFAULT_CONNECTOR)
        self.assertEqual(5, request.call_count)
        self.assertEqual(1, remove_fc_zone.call_count)

    @mock.patch.object(fczm_utils, "add_fc_zone")
    @mock.patch.object(requests.Session, "request")
    def test_initialize_connection_snapshot(self, request, add_fc_zone):
        self.override_config('hitachi_zoning_request', True,
                             group=conf.SHARED_CONF_GROUP)
        self.driver.common.lookup_service = FakeLookupService()
        request.side_effect = [FakeResponse(200, GET_HOST_WWNS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        ret = self.driver.initialize_connection_snapshot(
            TEST_SNAPSHOT[0], DEFAULT_CONNECTOR)
        self.assertEqual('fibre_channel', ret['driver_volume_type'])
        self.assertEqual([CONFIG_MAP['target_wwn']], ret['data']['target_wwn'])
        self.assertEqual(1, ret['data']['target_lun'])
        self.assertEqual(2, request.call_count)
        self.assertEqual(1, add_fc_zone.call_count)

    @mock.patch.object(fczm_utils, "remove_fc_zone")
    @mock.patch.object(requests.Session, "request")
    def test_terminate_connection_snapshot(self, request, remove_fc_zone):
        self.override_config('hitachi_zoning_request', True,
                             group=conf.SHARED_CONF_GROUP)
        self.driver.common.lookup_service = FakeLookupService()
        request.side_effect = [FakeResponse(200, GET_HOST_WWNS_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT_MAPPED),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, NOTFOUND_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.terminate_connection_snapshot(
            TEST_SNAPSHOT[0], DEFAULT_CONNECTOR)
        self.assertEqual(5, request.call_count)
        self.assertEqual(1, remove_fc_zone.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_manage_existing(
            self, get_volume_type_qos_specs, get_volume_type, request):
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEVS_RESULT)]
        ret = self.driver.manage_existing(
            TEST_VOLUME[0], self.test_existing_ref)
        actual = {'provider_location': json.dumps({'pldev': 1}),
                  'replication_status': fields.ReplicationStatus.DISABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(0, get_volume_type.call_count)
        self.assertEqual(3, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(objects.Volume, 'is_replicated')
    def test_manage_existing_get_size(
            self, is_replicated, request):
        is_replicated.return_value = False
        request.return_value = FakeResponse(200, GET_LDEV_RESULT)
        self.driver.manage_existing_get_size(
            TEST_VOLUME[0], self.test_existing_ref)
        self.assertEqual(2, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_unmanage(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT)]
        self.driver.unmanage(TEST_VOLUME[0])
        self.assertEqual(3, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_copy_image_to_volume(self, request):
        image_service = 'fake_image_service'
        image_id = 'fake_image_id'
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, COMPLETED_SUCCEEDED_RESULT)]
        with mock.patch.object(driver.VolumeDriver, 'copy_image_to_volume') \
                as mock_copy_image:
            self.driver.copy_image_to_volume(
                self.ctxt, TEST_VOLUME[0], image_service, image_id)
        mock_copy_image.assert_called_with(
            self.ctxt, TEST_VOLUME[0], image_service, image_id,
            disable_sparse=False)
        self.assertEqual(2, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_update_migrated_volume(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, COMPLETED_SUCCEEDED_RESULT)]
        ret = self.driver.update_migrated_volume(
            self.ctxt, TEST_VOLUME[0], TEST_VOLUME[1], "available")
        self.assertEqual(2, request.call_count)
        actual = ({'_name_id': TEST_VOLUME[1]['id'],
                   'provider_location': TEST_VOLUME[1]['provider_location']})
        self.assertEqual(actual, ret)

    def test_unmanage_snapshot(self):
        """The driver don't support unmange_snapshot."""
        self.assertRaises(
            NotImplementedError,
            self.driver.unmanage_snapshot,
            TEST_SNAPSHOT[0])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(obj_snap.SnapshotList, 'get_all_for_volume')
    @mock.patch.object(objects.VolumeType, 'is_replicated')
    def test_retype(self, is_replicated, get_all_for_volume,
                    get_volume_type_extra_specs, request):
        extra_specs = {'hbsd:test': 'test',
                       'hbsd:target_ports': 'CL2-A'}
        get_volume_type_extra_specs.return_value = extra_specs
        get_all_for_volume.return_value = True
        is_replicated.return_value = False

        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT)]

        old_specs = {'hbsd:target_ports': 'CL1-A'}
        new_specs = {'hbsd:target_ports': 'CL2-A'}
        old_type_ref = volume_types.create(self.ctxt, 'old', old_specs)
        new_type_ref = volume_types.create(self.ctxt, 'new', new_specs)
        new_type = objects.VolumeType.get_by_id(self.ctxt, new_type_ref['id'])

        diff = volume_types.volume_types_diff(self.ctxt, old_type_ref['id'],
                                              new_type_ref['id'])[0]
        host = {
            'capabilities': {
                'location_info': {
                    'pool_id': 30,
                },
            },
        }

        ret = self.driver.retype(
            self.ctxt, TEST_VOLUME[0], new_type, diff, host)
        self.assertEqual(2, request.call_count)
        self.assertFalse(ret)

    @mock.patch.object(requests.Session, "request")
    def test_retype_replication(self, request):
        extra_specs = {'test1': 'aaa'}

        request.return_value = FakeResponse(200, GET_LDEV_RESULT_REP)

        new_type_ref = volume_types.create(self.ctxt, 'new', extra_specs)
        new_type = objects.VolumeType.get_by_id(self.ctxt, new_type_ref['id'])
        diff = {}
        host = {
            'capabilities': {
                'location_info': {
                    'pool_id': 30,
                },
            },
        }
        ret = self.driver.retype(
            self.ctxt, TEST_VOLUME[0], new_type, diff, host)
        self.assertEqual(1, request.call_count)
        self.assertFalse(ret)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(objects.VolumeType, 'is_replicated')
    def test_retype_new_specs_replicated(self, is_replicated, request):
        is_replicated.return_value = True

        request.return_value = FakeResponse(200, GET_LDEV_RESULT)

        new_specs = {'replication_enabled': '<is> True'}
        new_type_ref = volume_types.create(self.ctxt, 'new', new_specs)
        new_type = objects.VolumeType.get_by_id(self.ctxt, new_type_ref['id'])
        diff = {}
        host = {
            'capabilities': {
                'location_info': {
                    'pool_id': 30,
                },
            },
        }
        ret = self.driver.retype(
            self.ctxt, TEST_VOLUME[0], new_type, diff, host)
        self.assertEqual(1, request.call_count)
        self.assertFalse(ret)

    @mock.patch.object(requests.Session, "request")
    def test_migrate_volume(
            self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT)]
        host = {
            'capabilities': {
                'location_info': {
                    'storage_id': CONFIG_MAP['serial'],
                    'pool_id': 30,
                    'execution_site': hbsd_utils.PRIMARY_STR,
                },
            },
        }
        ret = self.driver.migrate_volume(self.ctxt, TEST_VOLUME[0], host)
        self.assertEqual(3, request.call_count)
        actual = (True, None)
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_migrate_volume_diff_pool(
            self, get_volume_type_qos_specs,
            get_volume_type_extra_specs, request):
        extra_specs = {"test1": "aaa"}
        get_volume_type_extra_specs.return_value = extra_specs
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT_SMPL),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(200, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, COMPLETED_SUCCEEDED_RESULT)]
        host = {
            'capabilities': {
                'location_info': {
                    'storage_id': CONFIG_MAP['serial'],
                    'pool_id': 40,
                    'execution_site': hbsd_utils.PRIMARY_STR,
                },
            },
        }
        ret = self.driver.migrate_volume(self.ctxt, TEST_VOLUME[0], host)
        self.assertEqual(15, request.call_count)
        actual = (True,
                  {'provider_location': json.dumps({'pldev': 1}),
                   'replication_status': fields.ReplicationStatus.DISABLED})
        self.assertTupleEqual(actual, ret)

    @ddt.data('deduplication_compression', 'compression')
    @mock.patch.object(hbsd_rest.HBSDREST, "_copy_ldev_by_shadow_image")
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_migrate_volume_diff_pool_drs(
            self, csv, get_volume_type_qos_specs, get_volume_type_extra_specs,
            request, copy_ldev_by_shadow_image):
        """Test migrate_volume for a DRS volume to a different pool.

        When the source LDEV is a DRS (deduplication/compression) volume and
        the target pool differs from the source pool, migrate_volume must
        choose the ShadowImage-based copy path (_copy_ldev_by_shadow_image)
        instead of the normal ThinImage copy path (copy_on_storage).
        """
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {
            'hbsd:capacity_saving': csv,
            'hbsd:drs': '<is> True',
        }
        copy_ldev_by_shadow_image.return_value = 1
        # REST call sequence (copy path is mocked away):
        #  1. GET  - replication.migrate_volume -> _get_rep_pair_info
        #             -> _has_rep_pair -> get_ldev_info (no REP attr => False)
        #  2. GET  - base migrate_volume -> get_pair_info -> get_ldev_info
        #  3. GET  - pvol_ldev_info (has DRS attr => pvol_is_drs=True)
        #  4. 202  - modify_ldev_name for svol
        #  5. GET  - delete_ldev -> delete_pair -> get_pair_info
        #  6. GET  - delete_ldev -> unmap_ldev_from_storage -> get_ldev_info
        #  7. GET  - delete_ldev -> delete_ldev_from_storage -> get_ldev_info
        #  8. 202  - delete_ldev -> client.delete_ldev
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        host = {
            'capabilities': {
                'location_info': {
                    'storage_id': CONFIG_MAP['serial'],
                    'pool_id': 40,
                    'execution_site': hbsd_utils.PRIMARY_STR,
                },
            },
        }
        ret = self.driver.migrate_volume(self.ctxt, TEST_VOLUME[0], host)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(1, get_volume_type_qos_specs.call_count)
        # _copy_ldev_by_shadow_image must be called for DRS+pool-change
        copy_ldev_by_shadow_image.assert_called_once()
        self.assertEqual(8, request.call_count)
        actual = (True,
                  {'provider_location': json.dumps({'pldev': 1}),
                   'replication_status': fields.ReplicationStatus.DISABLED})
        self.assertTupleEqual(actual, ret)

    @ddt.data('deduplication_compression', 'compression')
    @mock.patch.object(hbsd_rest.HBSDREST, "_copy_ldev_by_shadow_image")
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_retype_diff_pool_drs(
            self, csv, get_volume_type_qos_specs, get_volume_type_extra_specs,
            request, copy_ldev_by_shadow_image):
        """Test migrate_volume for a DRS volume migrating to a different pool.

        Because the source LDEV is DRS and the pools differ, migrate_volume
        must choose _copy_ldev_by_shadow_image over copy_on_storage.
        """
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {
            'hbsd:capacity_saving': csv,
            'hbsd:drs': '<is> True',
        }
        copy_ldev_by_shadow_image.return_value = 1
        # REST call sequence (copy path is mocked away):
        #  1. GET  - replication.migrate_volume -> _get_rep_pair_info
        #             -> _has_rep_pair -> get_ldev_info (no REP attr => False)
        #  2. GET  - base migrate_volume -> get_pair_info -> get_ldev_info
        #  3. GET  - pvol_ldev_info (has DRS attr => pvol_is_drs=True)
        #  4. 202  - modify_ldev_name for svol
        #  5. GET  - delete_ldev -> delete_pair -> get_pair_info
        #  6. GET  - delete_ldev -> unmap_ldev_from_storage -> get_ldev_info
        #  7. GET  - delete_ldev -> delete_ldev_from_storage -> get_ldev_info
        #  8. 202  - delete_ldev -> client.delete_ldev
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(200, GET_LDEV_RESULT_DRS),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        host = {
            'capabilities': {
                'location_info': {
                    'storage_id': CONFIG_MAP['serial'],
                    'pool_id': 40,
                    'execution_site': hbsd_utils.PRIMARY_STR,
                },
            },
        }
        ret = self.driver.migrate_volume(self.ctxt, TEST_VOLUME[0], host)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        # _copy_ldev_by_shadow_image must be called for DRS+pool-change
        copy_ldev_by_shadow_image.assert_called_once()
        self.assertEqual(8, request.call_count)
        actual = (True,
                  {'provider_location': json.dumps({'pldev': 1}),
                   'replication_status': fields.ReplicationStatus.DISABLED})
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(requests.Session, "request")
    def test_revert_to_snapshot(self, request):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_PAIR),
                               FakeResponse(200, GET_LDEV_RESULT_PAIR),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT)]
        self.driver.revert_to_snapshot(
            self.ctxt, TEST_VOLUME[0], TEST_SNAPSHOT[0])
        self.assertEqual(6, request.call_count)
        args, kwargs = request.call_args_list[4]
        self.assertNotIn(
            'Job-Mode-Wait-Configuration-Change', kwargs['headers'])

    def test_create_group(self):
        ret = self.driver.create_group(self.ctxt, TEST_GROUP[0])
        self.assertIsNone(ret)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    def test_delete_group(self, request, get_group_type_specs):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        ret = self.driver.delete_group(
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        self.assertEqual(4, request.call_count)
        actual = (
            {'status': TEST_GROUP[0]['status']},
            [{'id': TEST_VOLUME[0]['id'], 'status': 'deleted'}]
        )
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_group_from_src_volume(
            self, get_volume_type_qos_specs, get_volume_type_extra_specs,
            get_volume_type, request, get_group_type_specs):

        get_volume_type_extra_specs.return_value = {}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if method == 'GET':
                if '/ldevs/' in url:
                    return FakeResponse(200, GET_LDEV_RESULT)
                elif '/snapshots' in url:
                    return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                else:
                    return FakeResponse(200, NOTFOUND_RESULT)
            elif method in ('POST', 'PUT'):
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            elif method == 'DELETE':
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            return FakeResponse(500, ERROR_RESULT,
                                headers={'Content-Type': 'json'})

        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_group_from_src(
            self.ctxt, TEST_GROUP[1], [TEST_VOLUME[1]],
            source_group=TEST_GROUP[0], source_vols=[TEST_VOLUME[0]]
        )
        self.assertGreater(request.call_count, 0)
        actual = (
            None,
            [{'id': TEST_VOLUME[1]['id'],
              'provider_location': json.dumps({'pldev': 1}),
              'replication_status': fields.ReplicationStatus.DISABLED}])
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch(
        'cinder.volume.drivers.hitachi.hbsd_rest.HBSDREST.'
        '_wait_copy_pair_status')
    def test_create_group_from_src_snapshot(
            self, mock_wait_copy_pair_status, get_volume_type_qos_specs,
            get_volume_type_extra_specs, get_volume_type, request,
            get_group_type_specs):
        mock_wait_copy_pair_status.return_value = None
        get_volume_type_extra_specs.return_value = {}
        get_volume_type.return_value = {}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if method == 'GET':
                if '/ldevs/' in url:
                    return FakeResponse(200, GET_LDEV_RESULT)
                elif '/snapshots' in url:
                    return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                else:
                    return FakeResponse(200, NOTFOUND_RESULT)
            elif method in ('POST', 'PUT'):
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            elif method == 'DELETE':
                return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
            return FakeResponse(500, ERROR_RESULT,
                                headers={'Content-Type': 'json'})

        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_group_from_src(
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]],
            group_snapshot=TEST_GROUP_SNAP[0], snapshots=[TEST_SNAPSHOT[0]]
        )
        self.assertGreater(request.call_count, 0)
        actual = (
            None,
            [{'id': TEST_VOLUME[0]['id'],
              'provider_location': json.dumps({'pldev': 1}),
              'replication_status': fields.ReplicationStatus.DISABLED}])
        self.assertTupleEqual(actual, ret)

    def _create_group_from_src_dispatch(self, source_vols):
        """Run create_group_from_src with both of its paths mocked out."""
        common = self._common()
        volumes = [TEST_VOLUME[1]] * len(source_vols)
        with mock.patch.object(
                common, '_group_repl_create_group_from_src',
                return_value=(None, [])) as group_repl, \
            mock.patch.object(
                common, '_get_active_backend') as backend:
            backend.return_value.create_group_from_src.return_value = (
                None, [])
            common.create_group_from_src(
                self.ctxt, TEST_GROUP[0], volumes, source_vols=source_vols)
        return group_repl, backend.return_value.create_group_from_src

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_create_group_from_src_group_replication_secondary_resident(
            self, get_group_type_specs):
        group_repl, generic = self._create_group_from_src_dispatch(
            [self._svol_only_volume(10), self._svol_only_volume(
                11, volume_id='00000000-0000-0000-0000-000000000098')])
        group_repl.assert_called_once()
        generic.assert_not_called()

    @ddt.data([TEST_VOLUME[0]], [TEST_VOLUME[0], TEST_VOLUME[4]])
    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_create_group_from_src_group_replication_not_secondary_resident(
            self, source_vols, get_group_type_specs):
        group_repl, generic = self._create_group_from_src_dispatch(
            source_vols)
        group_repl.assert_not_called()
        generic.assert_called_once()

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_create_group_from_src_group_replication_mixed_sites(
            self, get_group_type_specs):
        exc = self.assertRaises(
            exception.VolumeDriverException,
            self._create_group_from_src_dispatch,
            [TEST_VOLUME[0], self._svol_only_volume(10)])
        self.assertIn('exists in the other site', str(exc))
        self.assertNotIn('source to be replicated was not found', str(exc))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    def test_create_group_from_src_non_replicated_now_generic(
            self, get_group_type_specs):
        common = self._common()
        with mock.patch.object(
                common, '_group_repl_create_group_from_src') as group_repl:
            self.assertRaises(
                exception.VolumeDriverException,
                common.create_group_from_src,
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[1]],
                source_vols=[self._svol_only_volume(10)])
        group_repl.assert_not_called()

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_create_group_from_src_group_replication_failed_over(
            self, get_group_type_specs):
        self._common()._active_backend_id = 'test'
        group_repl, generic = self._create_group_from_src_dispatch(
            [self._svol_only_volume(10)])
        group_repl.assert_not_called()
        generic.assert_called_once()

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(volume_utils, 'is_group_a_cg_snapshot_type')
    @mock.patch.object(requests.Session, "request")
    def test_update_group(self, request, is_group_a_cg_snapshot_type,
                          get_group_type_specs):
        request.return_value = FakeResponse(200, GET_LDEV_RESULT)
        is_group_a_cg_snapshot_type.return_value = False
        ret = self.driver.update_group(
            self.ctxt, TEST_GROUP[0], add_volumes=[TEST_VOLUME[0]])
        self.assertTupleEqual((None, None, None), ret)
        self.assertEqual(1, request.call_count)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(sqlalchemy_api, 'volume_get', side_effect=_volume_get)
    @mock.patch.object(volume_utils, 'is_group_a_cg_snapshot_type')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_group_snapshot_non_cg(
            self, get_volume_type_qos_specs, is_group_a_cg_snapshot_type,
            volume_get, get_volume_type_extra_specs, request,
            get_group_type_specs):
        is_group_a_cg_snapshot_type.return_value = False
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_group_snapshot(
            self.ctxt, TEST_GROUP_SNAP[0], [TEST_SNAPSHOT[0]]
        )
        self.assertEqual(5, request.call_count)
        actual = (
            {'status': 'available'},
            [{'id': TEST_SNAPSHOT[0]['id'],
              'provider_location': json.dumps({'pldev': 1}),
              'status': 'available'}]
        )
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    def test_delete_group_snapshot(self, request, get_group_type_specs):
        request.side_effect = [FakeResponse(200, GET_LDEV_RESULT_PAIR),
                               FakeResponse(200, NOTFOUND_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(200, GET_SNAPSHOTS_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(200, GET_LDEV_RESULT),
                               FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)]
        ret = self.driver.delete_group_snapshot(
            self.ctxt, TEST_GROUP_SNAP[0], [TEST_SNAPSHOT[0]])
        self.assertEqual(14, request.call_count)
        actual = (
            {'status': TEST_GROUP_SNAP[0]['status']},
            [{'id': TEST_SNAPSHOT[0]['id'], 'status': 'deleted'}]
        )
        self.assertTupleEqual(actual, ret)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_failover_host(self, get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        ret = self.driver.failover_host(
            self.ctxt, [TEST_VOLUME[5]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])
        actual = (
            self.configuration.replication_device[0]['backend_id'],
            [{'volume_id': TEST_VOLUME[5]['id'],
              'updates': {
                  'replication_status':
                      fields.ReplicationStatus.FAILED_OVER}}],
            [])
        self.assertTupleEqual(actual, ret)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(4, request.call_count)
        self.assertEqual(
            self.driver.common.rep_secondary.backend_id,
            self.driver.common._active_backend_id)
        self.driver.common._active_backend_id = ''
        for args, kwargs in request.call_args_list:
            if args[0] == 'PUT' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                break
        else:
            self.fail('no swap pair api')

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def test_failover_host_failback(
            self, brick_get_connector_properties, request):
        self.driver.common._active_backend_id = \
            self.driver.common.rep_secondary.backend_id
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count == 0:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SPLIT)
                        elif self.copypair_count == 2:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_PSUS)
                        else:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif ('/remote-mirror-copygroups/' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT_SSWS)
                    elif ('/remote-mirror-copygroups' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUPS_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/ports' in url:
                        return FakeResponse(200, GET_PORTS_RESULT)
                    elif '/host-wwns' in url:
                        return FakeResponse(200, GET_HOST_WWNS_RESULT)
                    elif '/host-groups' in url:
                        return FakeResponse(200, GET_HOST_GROUPS_RESULT_PAIR)
            else:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, REMOTE_POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(
                            202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
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
        ret = self.driver.failover_host(
            self.ctxt, [TEST_VOLUME[6]],
            secondary_id=hbsd_replication._REP_FAILBACK)
        actual = (
            hbsd_replication._REP_FAILBACK,
            [{'volume_id': TEST_VOLUME[6]['id'],
              'updates': {
                  'replication_status':
                      fields.ReplicationStatus.ENABLED}}],
            [])
        self.assertTupleEqual(actual, ret)
        self.assertEqual(13, request.call_count)
        self.assertEqual('', self.driver.common._active_backend_id)
        for args, kwargs in request.call_args_list:
            if args[0] == 'PUT' and 'remote-mirror-copygroups' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                break
        else:
            self.fail('no resync pair api')

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_failover(self, get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True'}
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        ret = self.driver.failover(
            self.ctxt, [TEST_VOLUME[5]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])
        actual = (
            self.configuration.replication_device[0]['backend_id'],
            [{'volume_id': TEST_VOLUME[5]['id'],
              'updates': {
                  'replication_status':
                      fields.ReplicationStatus.FAILED_OVER}}],
            [])
        self.assertTupleEqual(actual, ret)
        self.assertEqual(1, get_volume_type_extra_specs.call_count)
        self.assertEqual(4, request.call_count)
        for args, kwargs in request.call_args_list:
            if args[0] == 'PUT' and 'remote-mirror-copypairs' in args[1]:
                self.assertEqual(
                    kwargs['headers']['Job-Mode-Wait-Configuration-Change'],
                    "NoWait")
                break
        else:
            self.fail('no swap pair api')

    def test_failover_completed(self):
        self.driver.failover_completed(
            self.ctxt,
            active_backend_id=
            self.configuration.replication_device[0]['backend_id'])
        self.assertEqual(
            self.driver.common.rep_secondary.backend_id,
            self.driver.common._active_backend_id)
        self.driver.common._active_backend_id = ''

    @mock.patch.object(requests.Session, "request")
    def test_get_ldev_site_sldev_in_loc(self, request):
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.delete_snapshot,
                          TEST_SNAPSHOT[5])
        self.assertEqual(0, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_get_ldev_site_pldev_in_loc(self, request):
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.delete_snapshot,
                          TEST_SNAPSHOT[3])
        self.assertEqual(1, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_verify_ldev_have_active_backend_id(self, request):
        self.driver.common._active_backend_id = 'test'
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.delete_snapshot,
                          TEST_SNAPSHOT[3])
        self.assertEqual(0, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_create_journals_continue(
            self, get_volume_type_extra_specs, get_volume_type_qos_specs,
            is_replicated, request):
        is_replicated.return_value = True
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            req_count = 0
            # for_use_global_scope_counter
            global global_counter
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/journals' in url:
                        if global_counter == 0:
                            req_count += 1
                            global_counter += 1
                            return FakeResponse(
                                200, GET_JOURNAL_RESULT_MESSAGEID_KART40054E,
                                headers={'Content-Type': 'json'})
                        else:
                            return FakeResponse(
                                200, GET_JOURNAL_RESULT,
                                headers={'Content-Type': 'json'})
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/journals' in url:
                        return FakeResponse(
                            200, GET_JOURNAL_RESULT_MESSAGEID_KART40046E,
                            headers={'Content-Type': 'json'})
                    elif '/remote-mirror-copypairs/' in url:
                        # reset_global_counter_of_the_end_of_request
                        global_counter = 0
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
            return FakeResponse(
                400, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        ret = self.driver.create_volume(TEST_VOLUME[8])
        actual = {
            'provider_location': json.dumps(
                {'pldev': 1, 'sldev': 2,
                 'remote-copy': hbsd_utils.REP_TYPE_ASYNC}),
            'replication_status': fields.ReplicationStatus.ENABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(25, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_create_journals_raise_error(
            self, get_volume_type_extra_specs,
            get_volume_type_qos_specs, is_replicated, request):
        is_replicated.return_value = True
        get_volume_type_extra_specs.return_value = {}

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/journals' in url:
                        return FakeResponse(
                            200, GET_JOURNAL_RESULT_MESSAGEID_KART40046E,
                            headers={'Content-Type': 'json'})
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/journals' in url:
                        return FakeResponse(
                            200, GET_JOURNAL_RESULT_MESSAGEID_KART40054E,
                            headers={'Content-Type': 'json'})
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
            return FakeResponse(
                400, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.create_volume,
                          TEST_VOLUME[8])
        self.assertEqual(13, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def test_get_failback_volume_update_snapshot_error(
            self, brick_get_connector_properties, request):
        self.driver.common._active_backend_id = \
            self.driver.common.rep_secondary.backend_id
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count == 0:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SPLIT)
                        elif self.copypair_count == 2:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_PSUS)
                        else:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif ('/remote-mirror-copygroups/' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT_SSWS)
                    elif ('/remote-mirror-copygroups' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUPS_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/ports' in url:
                        return FakeResponse(200, GET_PORTS_RESULT)
                    elif '/host-wwns' in url:
                        return FakeResponse(200, GET_HOST_WWNS_RESULT)
                    elif '/host-groups' in url:
                        return FakeResponse(200, GET_HOST_GROUPS_RESULT_PAIR)
            else:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, REMOTE_POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(
                            202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
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
        self.test_vol_add = TEST_VOLUME[6]
        attrs = {'objects': [TEST_SNAPSHOT[5]]}
        snaplist_obj = objects.SnapshotList(CTXT, **attrs)
        self.test_vol_add['snapshots'] = snaplist_obj
        self.assertRaises(exception.SnapshotNotFound,
                          self.driver.failover_host,
                          self.ctxt,
                          [self.test_vol_add],
                          secondary_id=hbsd_replication._REP_FAILBACK)
        self.assertEqual(13, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def test_failback_copy_group_except(
            self, brick_get_connector_properties, request):
        self.driver.common._active_backend_id = \
            self.driver.common.rep_secondary.backend_id
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count == 0:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SPLIT)
                        elif self.copypair_count == 2:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_PSUS)
                        else:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                500, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif ('/remote-mirror-copygroups/' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT_SSWS)
                    elif ('/remote-mirror-copygroups' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUPS_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/ports' in url:
                        return FakeResponse(200, GET_PORTS_RESULT)
                    elif '/host-wwns' in url:
                        return FakeResponse(200, GET_HOST_WWNS_RESULT)
                    elif '/host-groups' in url:
                        return FakeResponse(200, GET_HOST_GROUPS_RESULT_PAIR)
            else:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, REMOTE_POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(
                            202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
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
        self.driver.failover_host(
            self.ctxt, [TEST_VOLUME[6]],
            secondary_id=hbsd_replication._REP_FAILBACK)
        self.assertEqual(10, request.call_count)
        self.assertEqual('', self.driver.common._active_backend_id)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(
        volume_utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    def test_failback_volume_raise_exception(
            self, brick_get_connector_properties, request):
        self.driver.common._active_backend_id = \
            self.driver.common.rep_secondary.backend_id
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count == 0:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SPLIT)
                        elif self.copypair_count == 2:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_PSUS)
                        else:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                500, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif ('/remote-mirror-copygroups/' in url):
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYGROUP_RESULT_SSWS)
                    elif ('/remote-mirror-copygroups' in url):
                        return FakeResponse(
                            400, GET_REMOTE_MIRROR_COPYGROUPS_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/ports' in url:
                        return FakeResponse(200, GET_PORTS_RESULT)
                    elif '/host-wwns' in url:
                        return FakeResponse(200, GET_HOST_WWNS_RESULT)
                    elif '/host-groups' in url:
                        return FakeResponse(200, GET_HOST_GROUPS_RESULT_PAIR)
            else:
                if method in ('POST', 'PUT'):
                    if '/sessions' in url:
                        return FakeResponse(200, REMOTE_POST_SESSIONS_RESULT)
                    else:
                        return FakeResponse(
                            202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
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
        self.assertRaises(exception.UnableToFailOver,
                          self.driver.failover_host,
                          self.ctxt,
                          [TEST_VOLUME[6]],
                          secondary_id=hbsd_replication._REP_FAILBACK)
        self.assertEqual(4, request.call_count)

    def test_failover_pair_volume_pldev_or_sldev_is_none(self):
        self.copypair_count = 0
        self.driver.failover(
            self.ctxt, [TEST_VOLUME[7]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])

    @mock.patch.object(requests.Session, "request")
    def test_failover_pair_volume_not_pair_info(self, request):
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.failover(
            self.ctxt, [TEST_VOLUME[5]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])
        self.assertEqual(1, request.call_count)

    @mock.patch.object(requests.Session, "request")
    def test_failover_pair_volume_svol_status_not_pair(self, request):
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.failover(
            self.ctxt, [TEST_VOLUME[5]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])
        self.assertEqual(2, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_failover_volume_except(
            self, get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'replication_type': "async"}
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, NOTFOUND_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(400, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.driver.failover(
            self.ctxt, [TEST_VOLUME[5]],
            secondary_id=
            self.configuration.replication_device[0]['backend_id'])
        self.assertEqual(0, get_volume_type_extra_specs.call_count)
        self.assertEqual(1, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_failover_raise_exception_IRT(
            self, get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'replication_type': "split test"}
        self.copypair_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.assertRaises(exception.InvalidReplicationTarget,
                          self.driver.failover,
                          self.ctxt,
                          [TEST_VOLUME[2]],
                          secondary_id='test')
        self.assertEqual(0, get_volume_type_extra_specs.call_count)
        self.assertEqual(0, request.call_count)

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    def test_failover_raise_exception_UTF(
            self, get_volume_type_extra_specs, request):
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'replication_type': "split test"}
        self.copypair_count = 0
        self.driver.common._active_backend_id = 'test'

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/ports' in url:
                        return FakeResponse(200, GET_PORTS_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, REMOTE_COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if '/remote-mirror-copypairs/' in url:
                        if self.copypair_count < 1:
                            self.copypair_count = self.copypair_count + 1
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                        else:
                            return FakeResponse(
                                200, GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            return FakeResponse(
                500, ERROR_RESULT, headers={'Content-Type': 'json'})
        request.side_effect = _request_side_effect
        self.assertRaises(exception.UnableToFailOver,
                          self.driver.failover,
                          self.ctxt,
                          [TEST_VOLUME[2]],
                          secondary_id='default')
        self.assertEqual(0, get_volume_type_extra_specs.call_count)
        self.assertEqual(1, request.call_count)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    def test_update_group_ldev_is_none(self, get_group_type_specs):
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.update_group,
                          self.ctxt,
                          TEST_GROUP[0],
                          add_volumes=[TEST_VOLUME[3]])

    @ddt.data('deduplication_compression', 'compression')
    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type')
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(volume_utils, 'is_group_a_cg_snapshot_type')
    def test_update_group_has_rep_pair_true(
            self, csv, is_group_a_cg_snapshot_type,
            get_volume_type_extra_specs, get_volume_type, request,
            get_group_type_specs):
        self.driver.common._active_backend_id = 'test'
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'hbsd:capacity_saving': csv}
        get_volume_type.return_value = {}
        request.return_value = FakeResponse(200, GET_LDEV_RESULT_REP)
        is_group_a_cg_snapshot_type.return_value = False
        self.assertRaises(exception.VolumeDriverException,
                          self.driver.update_group,
                          self.ctxt,
                          TEST_GROUP[0],
                          add_volumes=[TEST_VOLUME[5]])
        self.assertEqual(1, request.call_count)

    @ddt.data('deduplication_compression', 'compression')
    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_rep_ldev_and_pair_capacity_saving_dedup_compression(
            self, csv, get_volume_type_qos_specs, is_replicated,
            get_volume_type_extra_specs, request):
        self.driver.common.rep_primary._stats = {}
        self.driver.common.rep_primary._stats['pools'] = [
            {'location_info': {'pool_id': 30}}]
        self.driver.common.rep_secondary._stats = {}
        self.driver.common.rep_secondary._stats['pools'] = [
            {'location_info': {'pool_id': 40}}]
        is_replicated.return_value = True
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            'hbsd:capacity_saving': csv}
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        self.snapshot_count = 0

        def _request_side_effect(
                method, url, params, json, headers, auth, timeout, verify):
            if self.configuration.hitachi_storage_id in url:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/remote-mirror-copypairs/' in url:
                        return FakeResponse(
                            200, GET_REMOTE_MIRROR_COPYPAIR_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
                    elif '/snapshots' in url:
                        if self.snapshot_count < 1:
                            self.snapshot_count = self.snapshot_count + 1
                            return FakeResponse(200, GET_SNAPSHOTS_RESULT)
                        else:
                            return FakeResponse(200, NOTFOUND_RESULT)
            else:
                if method in ('POST', 'PUT'):
                    return FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
                elif method == 'GET':
                    if ('/remote-mirror-copygroups' in url or
                            '/journals' in url):
                        return FakeResponse(200, NOTFOUND_RESULT)
                    elif '/ldevs/' in url:
                        return FakeResponse(200, GET_LDEV_RESULT_REP)
            if '/ldevs/' in url:
                return FakeResponse(200, GET_LDEV_RESULT_REP)
            else:
                return FakeResponse(
                    200, COMPLETED_SUCCEEDED_RESULT)
        request.side_effect = _request_side_effect
        ret = self.driver.create_cloned_volume(
            TEST_VOLUME[5], TEST_VOLUME[6])
        actual = {
            'provider_location': json.dumps(
                {'pldev': 1, 'sldev': 1,
                 'remote-copy': hbsd_utils.REP_TYPE_ASYNC}),
            'replication_status': fields.ReplicationStatus.ENABLED}
        self.assertEqual(actual, ret)
        self.assertEqual(2, get_volume_type_extra_specs.call_count)
        self.assertEqual(34, request.call_count)
        isDataReductionForceCopy = None
        for (args, kwargs) in request.call_args_list:
            if (args[0] == 'POST' and
                    '/remote-mirror-copypairs' in args[1]):
                isDataReductionForceCopy = (
                    kwargs['json']['isDataReductionForceCopy'])
                break
        else:
            self.fail('no create pair request')
        self.assertTrue(isDataReductionForceCopy)

    # ------------------------------------------------------------------
    # Group replication
    # ------------------------------------------------------------------

    def _common(self):
        return self.driver.common

    def _svol_only_volume(self, sldev,
                          volume_id='00000000-0000-0000-0000-000000000099'):
        """An adopted S-VOL: provider_location has sldev but no pldev."""
        return fake_volume.fake_volume_obj(
            CTXT, id=volume_id,
            provider_location=json.dumps({'sldev': sldev}))

    @staticmethod
    def _svol_copy_grp(copy_group_name, copy_pairs=None):
        """A copy group as read from the site holding its S side."""
        return {'copyGroupName': copy_group_name,
                'copyPairs': list(copy_pairs or [])}

    @staticmethod
    def _svol_side_not_here():
        """The reply of a site that does not hold a group's S side."""
        return {'messageId':
                hbsd_rest_api.MSGID_SPECIFIED_OBJECT_DOES_NOT_EXIST}

    @staticmethod
    def _message_text(msg):
        """The fixed part of a catalogue message, before its details."""
        return msg.value['msg'].split(' (')[0]

    @staticmethod
    def _label_of(obj):
        return (obj.name_id if hasattr(obj, 'name_id') else
                obj.id).replace('-', '')

    @staticmethod
    def _label_stub(site, answer):
        """Give site's LDEV the label answer, or make reading it raise.

        None stands for an LDEV labelled for some other object.
        """
        if isinstance(answer, Exception):
            return mock.patch.object(
                site, 'get_ldev_info', side_effect=answer)
        return mock.patch.object(
            site, 'get_ldev_info',
            return_value=dict(GET_LDEV_RESULT, label=answer or 'f' * 32))

    @staticmethod
    def _snapshot_of(volume, sldev=None,
                     snapshot_id='10000000-0000-0000-0000-000000000099'):
        """A snapshot of volume whose own LDEV, if given, is sldev."""
        snapshot = fake_snapshot.fake_snapshot_obj(
            CTXT, id=snapshot_id, volume_id=volume.id, volume_size=128,
            provider_location=(None if sldev is None else
                               json.dumps({'sldev': sldev})))
        snapshot.volume = volume
        return snapshot

    @staticmethod
    def _bound_volume(sldev=None, copy_group_name='CGBOUND',
                      volume_id='00000000-0000-0000-0000-000000000097'):
        """A volume bound to a copy group by its metadata.

        fake_volume_obj reads metadata from volume_metadata only; a
        metadata= argument is silently dropped.
        """
        return fake_volume.fake_volume_obj(
            CTXT, id=volume_id,
            provider_location=(None if sldev is None else
                               json.dumps({'sldev': sldev})),
            volume_metadata=[{'key': hbsd_replication._MD_COPY_GROUP,
                              'value': copy_group_name}])

    def test_create_group_copy_group_name(self):
        common = self._common()
        name = common._create_group_copy_group_name(TEST_GROUP[0].id)
        prefix = common.driver_info['target_prefix']
        self.assertTrue(name.startswith(prefix))
        self.assertEqual(
            prefix + TEST_GROUP[0].id.replace('-', '').upper()[
                :hbsd_replication._MAX_GROUP_COPY_GROUP_NAME - len(prefix)],
            name)

    def test_create_group_copy_group_name_leaves_room_for_journal(self):
        common = self._common()
        name = common._create_group_copy_group_name(TEST_GROUP[0].id)
        label = hbsd_replication._JOURNAL_VOLUME_LABEL % name
        self.assertLessEqual(len(label), hbsd_rest._MAX_LDEV_LABEL)

    def test_create_group_copy_group_name_is_stable(self):
        common = self._common()
        self.assertEqual(
            common._create_group_copy_group_name(TEST_GROUP[0].id),
            common._create_group_copy_group_name(TEST_GROUP[0].id))

    def test_resolve_copy_group_name_derived(self):
        common = self._common()
        self.assertEqual(
            common._create_group_copy_group_name(TEST_GROUP[0].id),
            common._resolve_copy_group_name(TEST_GROUP[0], []))

    def test_resolve_copy_group_name_from_binding(self):
        common = self._common()
        volume = mock.Mock()
        volume.metadata = {hbsd_replication._MD_COPY_GROUP: 'BOUNDCG'}
        self.assertEqual(
            'BOUNDCG',
            common._resolve_copy_group_name(TEST_GROUP[0], [volume]))

    def test_resolve_copy_group_name_binding_conflict(self):
        common = self._common()
        first = mock.Mock()
        first.metadata = {hbsd_replication._MD_COPY_GROUP: 'CGONE'}
        second = mock.Mock()
        second.metadata = {hbsd_replication._MD_COPY_GROUP: 'CGTWO'}
        self.assertRaises(
            exception.VolumeDriverException,
            common._resolve_copy_group_name, TEST_GROUP[0], [first, second])

    def test_resolve_copy_group_name_explicit_prefix(self):
        common = self._common()
        group = mock.Mock()
        group.id = TEST_GROUP[0].id
        group.name = (hbsd_replication._GROUP_NAME_BINDING_PREFIX +
                      '  EXISTINGCG  ')
        self.assertEqual(
            'EXISTINGCG', common._resolve_copy_group_name(group, []))

    def test_resolve_copy_group_name_empty_explicit_prefix(self):
        common = self._common()
        group = mock.Mock()
        group.id = TEST_GROUP[0].id
        group.name = hbsd_replication._GROUP_NAME_BINDING_PREFIX
        self.assertEqual(
            common._create_group_copy_group_name(TEST_GROUP[0].id),
            common._resolve_copy_group_name(group, []))

    def _adopt_svol_instance(self, common, pair_targets=None,
                             pair_target_name='HBSD-pair00'):
        # The site _check_adopted_svol_manageability checks by default.
        instance = common.rep_secondary
        instance._pair_targets = (
            [(CONFIG_MAP['port_id'], 5)] if pair_targets is None
            else pair_targets)
        instance._PAIR_TARGET_NAME = pair_target_name
        return instance

    @staticmethod
    def _adopt_ldev_info(ports=None, **overrides):
        info = {
            'emulationType': 'OPEN-V-CVS',
            'attributes': ['CVS', 'HDP', hbsd_rest.REP_ATTR],
            'status': hbsd_rest.NORMAL_STS,
            'numOfPorts': len(ports or []),
            'ports': list(ports or []),
        }
        info.update(overrides)
        return info

    @staticmethod
    def _port(host_group_number=5, host_group_name='HBSD-pair00',
              port_id=None, lun=0):
        return {
            'portId': CONFIG_MAP['port_id'] if port_id is None else port_id,
            'hostGroupNumber': host_group_number,
            'hostGroupName': host_group_name,
            'lun': lun,
        }

    def test_check_adopted_svol_manageability_allows_pair_target_path(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(ports=[self._port()])
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertIsNone(
                common._check_adopted_svol_manageability(
                    1, self.test_existing_ref))

    def test_check_adopted_svol_manageability_matches_pair_target_by_name(
            self):
        common = self._common()
        instance = self._adopt_svol_instance(common, pair_targets=[])
        ldev_info = self._adopt_ldev_info(ports=[self._port()])
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertIsNone(
                common._check_adopted_svol_manageability(
                    1, self.test_existing_ref))

    def test_check_adopted_svol_manageability_requests_ports(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(ports=[self._port()])
        with mock.patch.object(
                instance, 'get_ldev_info',
                return_value=ldev_info) as get_ldev_info:
            common._check_adopted_svol_manageability(
                1, self.test_existing_ref)
        self.assertIn('ports', get_ldev_info.call_args[0][0])

    def test_check_adopted_svol_manageability_unmapped(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        with mock.patch.object(
                instance, 'get_ldev_info',
                return_value=self._adopt_ldev_info()):
            self.assertIsNone(
                common._check_adopted_svol_manageability(
                    1, self.test_existing_ref))

    def test_check_adopted_svol_manageability_rejects_host_path(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(
            ports=[self._port(host_group_number=0,
                              host_group_name=CONFIG_MAP['host_grp_name'])])
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertRaises(
                exception.ManageExistingInvalidReference,
                common._check_adopted_svol_manageability,
                1, self.test_existing_ref)

    def test_check_adopted_svol_manageability_rejects_mixed_paths(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(
            ports=[self._port(),
                   self._port(host_group_number=0,
                              host_group_name=CONFIG_MAP['host_grp_name'])])
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertRaises(
                exception.ManageExistingInvalidReference,
                common._check_adopted_svol_manageability,
                1, self.test_existing_ref)

    def test_check_adopted_svol_manageability_rejects_undetailed_ports(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(numOfPorts=1, ports=None)
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertRaises(
                exception.ManageExistingInvalidReference,
                common._check_adopted_svol_manageability,
                1, self.test_existing_ref)

    def test_check_adopted_svol_manageability_bad_attributes(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        ldev_info = self._adopt_ldev_info(
            ports=[self._port()], attributes=['CVS'])
        with mock.patch.object(
                instance, 'get_ldev_info', return_value=ldev_info):
            self.assertRaises(
                exception.ManageExistingInvalidReference,
                common._check_adopted_svol_manageability,
                1, self.test_existing_ref)

    def test_foreign_ldev_ports_no_paths(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        self.assertEqual(
            [],
            common._foreign_ldev_ports(
                instance, {'numOfPorts': 0, 'ports': []}))

    def test_foreign_ldev_ports_undetailed(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        self.assertIsNone(
            common._foreign_ldev_ports(
                instance, {'numOfPorts': 2, 'ports': []}))

    def test_is_pair_target_port_other_port_same_gid(self):
        common = self._common()
        instance = self._adopt_svol_instance(common)
        self.assertFalse(
            common._is_pair_target_port(
                instance,
                self._port(port_id='CL9-Z', host_group_name='HBSD-other')))

    def test_group_repl_aggregate_status_success(self):
        common = self._common()
        updates = [{'replication_status': fields.ReplicationStatus.ENABLED}]
        self.assertEqual(
            fields.ReplicationStatus.ENABLED,
            common._group_repl_aggregate_status(
                updates, fields.ReplicationStatus.ENABLED))

    def test_group_repl_aggregate_status_one_error_fails_the_group(self):
        common = self._common()
        updates = [{'replication_status': fields.ReplicationStatus.ENABLED},
                   {'replication_status': fields.ReplicationStatus.ERROR}]
        self.assertEqual(
            fields.ReplicationStatus.ERROR,
            common._group_repl_aggregate_status(
                updates, fields.ReplicationStatus.ENABLED))

    def test_group_repl_aggregate_status_empty(self):
        common = self._common()
        self.assertEqual(
            fields.ReplicationStatus.DISABLED,
            common._group_repl_aggregate_status(
                [], fields.ReplicationStatus.DISABLED))

    def _svol_side_on_peer(self, common):
        """rep_primary does not hold the S side, so listing decides."""
        return mock.patch.object(
            common.rep_primary.client, 'get_remote_copy_grp',
            return_value=self._svol_side_not_here())

    def test_list_replication_targets_found(self):
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with self._svol_side_on_peer(common), mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[{'copyGroupName': copy_group_name}]):
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        self.assertEqual(
            {'replication_targets': [
                {'backend_id': common.rep_secondary_backend_id}]}, ret)

    def test_list_replication_targets_not_found(self):
        common = self._common()
        with self._svol_side_on_peer(common), mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[{'copyGroupName': 'SOMEOTHERCG'}]):
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        self.assertEqual({'replication_targets': []}, ret)

    def test_list_replication_targets_empty_response(self):
        common = self._common()
        with self._svol_side_on_peer(common), mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=None):
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        self.assertEqual({'replication_targets': []}, ret)

    def test_list_replication_targets_query_failure(self):
        common = self._common()
        with self._svol_side_on_peer(common), mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='x')):
            self.assertRaises(
                exception.VolumeDriverException,
                common.list_replication_targets, self.ctxt, TEST_GROUP[0])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_enable_replication_new_copy_group(self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0], TEST_VOLUME[1]]
        enabled = fields.ReplicationStatus.ENABLED
        with mock.patch.object(
                common, '_group_repl_adopted_members', return_value=False), \
            mock.patch.object(
                common, '_group_repl_copy_grp_exists', return_value=False), \
            mock.patch.object(
                common, '_group_repl_add_volume',
                side_effect=lambda volume, *a, **k: {
                    'id': volume.id,
                    'replication_status': enabled}) as add_volume, \
            mock.patch.object(
                common, '_wait_pair_status_change') as wait_pair:
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        wait_pair.assert_not_called()
        self.assertEqual({'replication_status': enabled}, model_update)
        self.assertEqual(2, add_volume.call_count)
        # Only the first add may create the copy group.
        self.assertTrue(add_volume.call_args_list[0][0][2])
        self.assertFalse(add_volume.call_args_list[1][0][2])
        self.assertEqual(
            [enabled] * 2,
            [update['replication_status'] for update in volumes_update])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_enable_replication_keeps_creating_after_a_failed_add(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0], TEST_VOLUME[1]]
        statuses = [fields.ReplicationStatus.ERROR,
                    fields.ReplicationStatus.ENABLED]
        with mock.patch.object(
                common, '_group_repl_adopted_members', return_value=False), \
            mock.patch.object(
                common, '_group_repl_copy_grp_exists', return_value=False), \
            mock.patch.object(
                common, '_group_repl_add_volume',
                side_effect=[{'id': volumes[0].id,
                              'replication_status': statuses[0]},
                             {'id': volumes[1].id,
                              'replication_status': statuses[1]}]
        ) as add_volume:
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        self.assertTrue(add_volume.call_args_list[1][0][2])
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ERROR},
            model_update)
        self.assertEqual(
            statuses,
            [update['replication_status'] for update in volumes_update])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_enable_replication_resyncs_suspended_members(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0]]
        enabled = fields.ReplicationStatus.ENABLED
        with mock.patch.object(
                common, '_group_repl_adopted_members', return_value=False), \
            mock.patch.object(
                common, '_group_repl_copy_grp_exists', return_value=True), \
            mock.patch.object(
                common, '_group_repl_classify_members',
                return_value=(volumes, [], [])), \
            mock.patch.object(
                common, '_group_repl_resync_members',
                return_value=[{'id': volumes[0].id,
                               'replication_status': enabled}]) as resync, \
            mock.patch.object(
                common, '_group_repl_add_volume') as add_volume:
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        resync.assert_called_once()
        add_volume.assert_not_called()
        self.assertEqual({'replication_status': enabled}, model_update)
        self.assertEqual(1, len(volumes_update))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_enable_replication_adopts_on_target_backend(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0]]
        enabled = fields.ReplicationStatus.ENABLED
        adopted = [{'id': volumes[0].id, 'replication_status': enabled}]
        with mock.patch.object(
                common, '_group_repl_adopted_members', return_value=True), \
            mock.patch.object(
                common, '_group_repl_adopt_members',
                return_value=adopted) as adopt, \
            mock.patch.object(
                common, '_group_repl_add_volume') as add_volume:
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        adopt.assert_called_once()
        add_volume.assert_not_called()
        self.assertEqual({'replication_status': enabled}, model_update)
        self.assertEqual(adopted, volumes_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    def test_enable_replication_not_replicated_raises(
            self, request, get_group_type_specs):
        self.assertRaises(
            NotImplementedError,
            self._common().enable_replication,
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        request.assert_not_called()

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_disable_replication(self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0], TEST_VOLUME[1]]
        disabled = fields.ReplicationStatus.DISABLED
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common, '_group_repl_journal_ids',
                return_value=(0, 1)) as journal_ids, \
            mock.patch.object(
                common, '_group_repl_delete_volume',
                side_effect=lambda volume, *a: {
                    'id': volume.id,
                    'replication_status': disabled}) as delete_volume, \
            mock.patch.object(
                common, '_group_repl_delete_journals') as delete_journals:
            model_update, volumes_update = common.disable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        journal_ids.assert_called_once_with(copy_group_name)
        self.assertEqual(2, delete_volume.call_count)
        # The journals are read before the pairs go and deleted after.
        delete_journals.assert_called_once_with(copy_group_name, (0, 1))
        self.assertEqual({'replication_status': disabled}, model_update)
        self.assertEqual(
            [disabled] * 2,
            [update['replication_status'] for update in volumes_update])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_disable_replication_reports_a_failed_member(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0], TEST_VOLUME[1]]
        with mock.patch.object(
                common, '_group_repl_journal_ids', return_value=None), \
            mock.patch.object(
                common, '_group_repl_delete_volume',
                side_effect=[
                    {'id': volumes[0].id,
                     'replication_status': fields.ReplicationStatus.DISABLED},
                    {'id': volumes[1].id,
                     'replication_status': fields.ReplicationStatus.ERROR}]), \
                mock.patch.object(common, '_group_repl_delete_journals'):
            model_update, _ = common.disable_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ERROR},
            model_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    def test_disable_replication_not_replicated_raises(
            self, request, get_group_type_specs):
        self.assertRaises(
            NotImplementedError,
            self._common().disable_replication,
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        request.assert_not_called()

    def _failover_patches(self, common):
        # The S side is the peer, as for a backend that made the pairs.
        return mock.patch.multiple(
            common,
            _wait_pair_status_change=mock.DEFAULT,
            _get_ldevs=mock.DEFAULT,
            _copy_group_svol_side=mock.Mock(
                return_value=(common.rep_secondary, None)))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_emergency_takes_over_from_secondary(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0]]
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with self._failover_patches(common) as patches, \
            mock.patch.object(
                common.rep_secondary.client,
                'takeover_remote_copy_grp') as takeover, \
            mock.patch.object(
                common.rep_primary.client,
                'split_remote_copy_grp') as split:
            patches['_get_ldevs'].return_value = (1, 2)
            model_update, volumes_update = common.failover_replication(
                self.ctxt, TEST_GROUP[0], volumes)
        takeover.assert_called_once_with(None, copy_group_name)
        split.assert_not_called()
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.FAILED_OVER},
            model_update)
        self.assertEqual(
            [fields.ReplicationStatus.FAILED_OVER],
            [update['replication_status'] for update in volumes_update])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_graceful_splits_from_primary(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0]]
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with self._failover_patches(common) as patches, \
            mock.patch.object(
                common.rep_secondary.client,
                'takeover_remote_copy_grp') as takeover, \
            mock.patch.object(
                common.rep_primary.client,
                'split_remote_copy_grp') as split:
            patches['_get_ldevs'].return_value = (1, 2)
            common.failover_replication(
                self.ctxt, TEST_GROUP[0], volumes,
                secondary_backend_id='remote:graceful')
        split.assert_called_once_with(
            common.rep_secondary.client, copy_group_name,
            common.driver_info['rep_type_async'])
        takeover.assert_not_called()

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_failback_resyncs_with_swap(
            self, get_group_type_specs):
        common = self._common()
        volumes = [TEST_VOLUME[0]]
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with self._failover_patches(common) as patches, \
            mock.patch.object(
                common.rep_secondary.client,
                'resync_remote_copy_grp') as resync:
            patches['_get_ldevs'].return_value = (1, 2)
            model_update, _ = common.failover_replication(
                self.ctxt, TEST_GROUP[0], volumes,
                secondary_backend_id=hbsd_replication._REP_FAILBACK)
        resync.assert_called_once_with(
            common.rep_primary.client, copy_group_name,
            common.driver_info['rep_type_async'], swap=True,
            is_secondary=True)
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ENABLED},
            model_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_failback_rejects_a_mode(
            self, get_group_type_specs):
        common = self._common()
        self.assertRaises(
            exception.InvalidReplicationTarget,
            common.failover_replication,
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]],
            secondary_backend_id=(
                hbsd_replication._REP_FAILBACK + ':graceful'))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_takeover_failure(self, get_group_type_specs):
        common = self._common()
        with self._failover_patches(common) as patches, \
            mock.patch.object(
                common.rep_secondary.client, 'takeover_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='x')):
            patches['_get_ldevs'].return_value = (1, 2)
            self.assertRaises(
                exception.UnableToFailOver,
                common.failover_replication,
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_unpaired_volume_is_an_error(
            self, get_group_type_specs):
        common = self._common()
        with self._failover_patches(common) as patches, \
                mock.patch.object(
                    common.rep_secondary.client, 'takeover_remote_copy_grp'):
            patches['_get_ldevs'].return_value = (None, None)
            model_update, volumes_update = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ERROR},
            model_update)
        self.assertEqual(
            fields.ReplicationStatus.ERROR,
            volumes_update[0]['replication_status'])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_remembers_the_copy_group(
            self, get_group_type_specs):
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with self._failover_patches(common) as patches, \
                mock.patch.object(
                    common.rep_secondary.client, 'takeover_remote_copy_grp'):
            patches['_get_ldevs'].return_value = (1, 2)
            common.failover_replication(
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        self.assertIn(copy_group_name, common._known_copy_groups)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value=False)
    @mock.patch.object(requests.Session, "request")
    def test_failover_replication_not_replicated_raises(
            self, request, get_group_type_specs):
        self.assertRaises(
            NotImplementedError,
            self._common().failover_replication,
            self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        request.assert_not_called()

    # ------------------------------------------------------------------
    # An sldev-only volume or snapshot lives on the site whose LDEV carries
    # its label. rep_primary's own methods read the pldev key and find
    # nothing, so these assert on the LDEV-level calls.
    # ------------------------------------------------------------------

    def test_ac12_delete_volume_deletes_a_local_svol_on_the_local_array(
            self):
        """rep_primary.delete_volume stays real: it would delete nothing."""
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.object(
                    common.rep_secondary, 'delete_ldev') as remote_delete:
            common.delete_volume(volume)
        local_delete.assert_called_once_with(9, mock.ANY)
        remote_delete.assert_not_called()

    def test_delete_volume_source_role_still_uses_rep_secondary(self):
        """An sldev-only volume labelled on rep_secondary is deleted there."""
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, None), \
            self._label_stub(common.rep_secondary, self._label_of(volume)), \
            mock.patch.object(
                common.rep_primary, 'delete_volume') as local_delete, \
            mock.patch.object(
                common.rep_secondary, 'delete_volume') as remote_delete:
            common.delete_volume(volume)
        remote_delete.assert_called_once_with(volume)
        local_delete.assert_not_called()

    def test_ac13_group_delete_removes_a_local_svol_member_locally(self):
        """get_ldev stays real: with no pldev, no pair is deleted.

        The member's sldev is found on rep_primary by its label.
        """
        common = self._common()
        volume = self._svol_only_volume(20)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary.client,
                    'delete_remote_copypair') as delete_pair, \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.object(
                    common.rep_secondary, 'delete_ldev') as remote_delete:
            update = common._group_repl_delete_group_volume(
                TEST_GROUP[0], volume, 'CGTEST')
        self.assertEqual({'id': volume.id, 'status': 'deleted'}, update)
        delete_pair.assert_not_called()
        local_delete.assert_called_once_with(20, mock.ANY)
        remote_delete.assert_not_called()

    def _snapshot_peer_patches(self, common):
        return mock.patch.multiple(
            common.rep_secondary, create_ldev=mock.DEFAULT,
            get_volume_extra_specs=mock.DEFAULT,
            modify_ldev_name=mock.DEFAULT,
            _create_ctg_snap_pair=mock.DEFAULT)

    def test_ac14_group_snapshot_snaps_local_svols_on_the_local_array(self):
        """get_ldev stays real: on rep_primary it reads the pldev key.

        It would reject the member as having no LDEV.
        """
        common = self._common()
        member = self._svol_only_volume(11)
        snapshot = self._snapshot_of(member)
        with self._label_stub(common.rep_primary, self._label_of(member)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'get_volume_extra_specs',
                    return_value={}), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                mock.patch.object(
                    common.rep_primary, 'create_ldev',
                    return_value=22) as local_create, \
                mock.patch.object(common.rep_primary, 'modify_ldev_name'), \
                mock.patch.object(
                    common.rep_primary, '_create_ctg_snap_pair') as snap, \
                self._snapshot_peer_patches(common) as peer:
            peer['create_ldev'].return_value = 99
            model_update, snapshots_update = (
                common._group_repl_create_group_snapshot(
                    self.ctxt, TEST_GROUP_SNAP[0], [snapshot]))
        for method in peer.values():
            method.assert_not_called()
        self.assertIsNone(model_update)
        local_create.assert_called_once()
        self.assertEqual(
            [{'snapshot': snapshot, 'pvol': 11, 'svol': 22}],
            snap.call_args[0][0])
        self.assertEqual(
            [{'id': snapshot.id, 'status': fields.SnapshotStatus.AVAILABLE,
              'provider_location': json.dumps({'sldev': 22})}],
            snapshots_update)

    def test_ac15_delete_group_snapshot_removes_local_svols_locally(self):
        """rep_primary._delete_group stays real: it would delete nothing.

        Its delete_snapshot reads the pldev key.
        """
        common = self._common()
        snapshot = self._snapshot_of(TEST_VOLUME[0], sldev=33)
        with self._label_stub(
                common.rep_primary, self._label_of(snapshot)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.multiple(
                    common.rep_secondary, delete_ldev=mock.DEFAULT,
                    _delete_group=mock.DEFAULT) as peer:
            peer['_delete_group'].return_value = (None, [])
            model_update, snapshots_update = (
                common._group_repl_delete_group_snapshot(
                    TEST_GROUP_SNAP[0], [snapshot]))
        for method in peer.values():
            method.assert_not_called()
        local_delete.assert_called_once_with(33, mock.ANY)
        self.assertEqual(
            {'status': TEST_GROUP_SNAP[0].status}, model_update)
        self.assertEqual(
            [{'id': snapshot.id, 'status': 'deleted'}], snapshots_update)

    # ------------------------------------------------------------------
    # A3: an adopted S-VOL ({"sldev": N}, no pldev) whose copy group's S
    # side is local must be promotable, not stuck in ERROR.
    # ------------------------------------------------------------------

    def test_ac10_get_ldevs_missing_pvol_is_expected_for_a_local_svol(self):
        common = self._common()
        volume = self._svol_only_volume(30)
        with mock.patch.object(
                common.rep_primary, 'output_log') as primary_log, \
            mock.patch.object(
                common.rep_secondary, 'output_log') as secondary_log:
            pldev, sldev = common._get_ldevs(
                volume, svol_site=common.rep_primary)
        self.assertIsNone(pldev)
        self.assertEqual(30, sldev)
        primary_log.assert_not_called()
        secondary_log.assert_not_called()

    def test_get_ldevs_source_role_missing_pvol_still_warns(self):
        """Without svol_site, as in host failover, a missing P-VOL warns."""
        common = self._common()
        volume = self._svol_only_volume(30)
        with mock.patch.object(
                common.rep_primary, 'output_log') as primary_log:
            common._get_ldevs(volume)
        primary_log.assert_called_once()

    def test_ac10_get_ldevs_missing_svol_still_warns_for_a_local_svol_side(
            self):
        common = self._common()
        volume = TEST_VOLUME[3]  # provider_location is None
        with mock.patch.object(
                common.rep_primary, 'output_log') as primary_log:
            common._get_ldevs(volume, svol_site=common.rep_primary)
        primary_log.assert_called_once()

    @staticmethod
    def _peer_unreachable(common, *names):
        """Patch the named rep_secondary client calls; callers make them fail.

        The mocks are yielded so a test can assert none was reached.
        """
        return mock.patch.multiple(
            common.rep_secondary.client,
            **{name: mock.DEFAULT for name in names})

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac4_failover_replication_takes_over_a_local_svol_side_locally(
            self, get_group_type_specs):
        """rep_secondary is down for the whole failover."""
        common = self._common()
        volume = self._svol_only_volume(40)
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        ssws = dict(GET_REMOTE_MIRROR_COPYPAIR_RESULT_SSWS, svolLdevId=40)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp(copy_group_name)), \
                mock.patch.object(
                    common.rep_primary.client,
                    'takeover_remote_copy_grp') as takeover, \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copypair',
                    return_value=ssws) as poll, \
                self._peer_unreachable(
                    common, 'get_remote_copy_grp', 'get_remote_copypair',
                    'takeover_remote_copy_grp') as peer:
            for method in peer.values():
                method.side_effect = exception.VolumeDriverException(
                    data='peer down')
            model_update, volumes_update = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        for method in peer.values():
            method.assert_not_called()
        takeover.assert_called_once_with(None, copy_group_name)
        poll.assert_called_once_with(
            None, copy_group_name, None, 40, is_secondary=True)
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.FAILED_OVER},
            model_update)
        self.assertEqual(
            fields.ReplicationStatus.FAILED_OVER,
            volumes_update[0]['replication_status'])

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_failover_replication_source_role_svol_only_still_errors(
            self, get_group_type_specs):
        """With the S side on the peer, a member still needs both LDEVs."""
        common = self._common()
        with self._failover_patches(common) as patches, \
            mock.patch.object(
                common.rep_secondary.client, 'takeover_remote_copy_grp'):
            patches['_get_ldevs'].return_value = (None, 40)
            model_update, volumes_update = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[0]])
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ERROR},
            model_update)
        self.assertEqual(
            fields.ReplicationStatus.ERROR,
            volumes_update[0]['replication_status'])

    # ------------------------------------------------------------------
    # B4: an S-VOL with no matching pair in the copy group must not be
    # silently re-paired (which orphans the old LDEV) - it belongs in
    # wrong_state so the operator sees ERROR.
    # ------------------------------------------------------------------

    def test_group_repl_classify_members_orphaned_svol_is_wrong_state(self):
        common = self._common()
        volume = TEST_VOLUME[4]  # pldev=4, sldev=4: _PRIMARY_SECONDARY site
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value={'copyPairs': []}), \
            mock.patch.object(
                common.rep_primary, 'get_ldev', return_value=4):
            suspended, replicating, wrong_state = (
                common._group_repl_classify_members('CG', [volume]))
        self.assertEqual([], suspended)
        self.assertEqual([], replicating)
        self.assertEqual(1, len(wrong_state))
        self.assertEqual(volume, wrong_state[0][0])

    # ------------------------------------------------------------------
    # B5: enable_replication / _group_repl_update_group report ENABLED on
    # pair creation and no longer block on _WAIT_PAIR.
    # ------------------------------------------------------------------

    def test_group_repl_update_group_does_not_wait_for_pair(self):
        common = self._common()
        volume = TEST_VOLUME[0]
        enabled = fields.ReplicationStatus.ENABLED
        with mock.patch.object(
                common, '_resolve_copy_group_name', return_value='CG'), \
            mock.patch.object(
                common, '_group_repl_copy_grp_exists', return_value=False), \
            mock.patch.object(
                common, '_group_repl_add_volume',
                return_value={'id': volume.id,
                              'replication_status': enabled}), \
            mock.patch.object(
                common, '_wait_pair_status_change') as wait_pair:
            model_update, add_update, remove_update = (
                common._group_repl_update_group(TEST_GROUP[0], [volume], []))
        wait_pair.assert_not_called()
        self.assertEqual(
            [enabled], [u['replication_status'] for u in add_update])

    # ------------------------------------------------------------------
    # B1: per-copy-group pair status reporting is opt-in and, once on,
    # cached with a TTL rather than read from the array on every poll.
    # ------------------------------------------------------------------

    def test_update_volume_stats_pair_status_off_by_default(self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary.client,
                'get_remote_copy_grps') as list_grps, \
            mock.patch.object(
                common.rep_primary, 'update_volume_stats',
                return_value={'pools': [{'location_info': {}}]}):
            stats = common.update_volume_stats()
        list_grps.assert_not_called()
        self.assertTrue(
            stats['pools'][0][hbsd_replication._PAIR_STATUS_PEER_KEY])

    def test_update_volume_stats_pair_status_when_enabled(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[]) as list_grps, \
            mock.patch.object(
                common.rep_primary, 'update_volume_stats',
                return_value={'pools': [{'location_info': {}}]}):
            common.update_volume_stats()
        list_grps.assert_called_once()

    def test_pair_status_capabilities_caches_within_ttl(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[]) as list_grps:
            common._pair_status_capabilities()
            common._pair_status_capabilities()
        self.assertEqual(1, list_grps.call_count)

    def test_pair_status_capabilities_refreshes_after_ttl(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[]) as list_grps:
            common._pair_status_capabilities()
            common._pair_status_cache['time'] = (
                common._pair_status_cache['time'] - timedelta(hours=1))
            common._pair_status_capabilities()
        self.assertEqual(2, list_grps.call_count)

    def test_pair_status_capabilities_serves_stale_on_array_error(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=[]):
            first = common._pair_status_capabilities()
        common._pair_status_cache['time'] = (
            common._pair_status_cache['time'] - timedelta(hours=1))
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='down')):
            second = common._pair_status_capabilities()
        self.assertEqual(
            first[hbsd_replication._PAIR_STATUS_UPDATED_KEY],
            second[hbsd_replication._PAIR_STATUS_UPDATED_KEY])
        self.assertEqual(
            first[hbsd_replication._PAIR_STATUS_KEY],
            second[hbsd_replication._PAIR_STATUS_KEY])

    # ------------------------------------------------------------------
    # B2: truncation past the copy-group cap is operator-visible and
    # marks the report as not fully enumerated.
    # ------------------------------------------------------------------

    def test_pair_status_capabilities_truncation_marks_not_enumerated(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        names = ['CG%d' % i for i in
                 range(hbsd_replication._PAIR_STATUS_MAX_COPY_GROUPS + 1)]
        grps = [{'copyGroupName': n} for n in names]
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=grps), \
            mock.patch.object(
                common, '_journals_by_id', return_value={}), \
            mock.patch.object(
                common, '_copy_grp_pair_state', return_value={}), \
            mock.patch.object(
                hbsd_replication.LOG, 'warning') as warn:
            capabilities = common._pair_status_capabilities()
        self.assertFalse(
            capabilities[hbsd_replication._PAIR_STATUS_ENUMERATED_KEY])
        warn.assert_called()

    # ------------------------------------------------------------------
    # B3: per-group read failures are reported once, not once per group.
    # ------------------------------------------------------------------

    def test_pair_status_capabilities_batches_group_read_failures(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        grps = [{'copyGroupName': 'CG1'}, {'copyGroupName': 'CG2'}]
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=grps), \
            mock.patch.object(
                common, '_journals_by_id', return_value={}), \
            mock.patch.object(
                common, '_copy_grp_pair_state',
                side_effect=exception.VolumeDriverException(data='x')), \
            mock.patch.object(
                hbsd_replication.LOG, 'warning') as warn:
            common._pair_status_capabilities()
        self.assertEqual(1, warn.call_count)

    def test_replication_metadata_keys(self):
        self.assertEqual('replication_pvol_id', hbsd_replication._MD_PVOL)
        self.assertEqual('replication_svol_id', hbsd_replication._MD_SVOL)
        self.assertEqual(
            'replication_copy_group', hbsd_replication._MD_COPY_GROUP)

    def test_group_repl_add_volume_stamps_the_contract_keys(self):
        common = self._common()
        volume = TEST_VOLUME[0]
        volume.metadata = {}
        with mock.patch.object(
                common.rep_primary, 'get_ldev', return_value=11), \
            mock.patch.object(
                common, '_has_rep_pair', return_value=False), \
            mock.patch.object(
                common.rep_primary, 'get_volume_extra_specs',
                return_value={}), \
            mock.patch.object(
                hbsd_utils, 'get_qos_specs_from_volume', return_value=None), \
            mock.patch.object(
                common.rep_secondary, 'create_ldev', return_value=22), \
            mock.patch.object(
                common.rep_secondary, 'initialize_pair_connection'), \
            mock.patch.object(
                common.rep_primary, 'initialize_pair_connection'), \
            mock.patch.object(common.rep_secondary, 'modify_ldev_name'), \
            mock.patch.object(
                common, '_group_repl_create_pair'):
            volume_update = common._group_repl_add_volume(
                volume, 'CGTEST', False, 'enable replication')
        self.assertEqual(
            {'replication_pvol_id': '11',
             'replication_svol_id': '22',
             'replication_copy_group': 'CGTEST'},
            volume_update['metadata'])

    def test_pair_status_skips_journal_read_when_array_reports_usage(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        grps = [{'copyGroupName': 'CG1'}, {'copyGroupName': 'CG2'}]
        detail = {'copyGroupName': 'CG1', 'pairStatus': 'PAIR',
                  'journalUsageRate': 12, 'copyPairs': []}
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=grps), \
            mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=detail), \
            mock.patch.object(
                common, '_journals_by_id', return_value={}) as journals:
            capabilities = common._pair_status_capabilities()
        journals.assert_not_called()
        pairs = json.loads(capabilities[hbsd_replication._PAIR_STATUS_KEY])
        self.assertEqual(12, pairs['CG1']['journal_usage_rate'])

    def test_pair_status_reads_journals_once_for_many_groups(self):
        common = self._common()
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)
        grps = [{'copyGroupName': 'CG%d' % i} for i in range(4)]
        # No journalUsageRate, so every group falls back to the journal list.
        detail = {'pairStatus': 'PAIR', 'copyPairs': []}
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=grps), \
            mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=detail), \
            mock.patch.object(
                common, '_journals_by_id', return_value={}) as journals:
            common._pair_status_capabilities()
        self.assertEqual(1, journals.call_count)

    def _delete_group_with_copy_grps(self, remaining):
        common = self._common()
        with mock.patch.object(
                common, '_resolve_copy_group_name', return_value='CGTEST'), \
            mock.patch.object(
                common, '_group_repl_journal_ids', return_value=(0, 1)), \
            mock.patch.object(
                common, '_group_repl_delete_group_volume',
                return_value={'id': TEST_VOLUME[0].id,
                              'status': 'deleted'}), \
            mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=remaining), \
            mock.patch.object(
                common, '_delete_journals') as delete_journals:
            common._group_repl_delete_group(TEST_GROUP[0], [TEST_VOLUME[0]])
        return delete_journals

    def test_group_repl_delete_group_reclaims_journals_after_last_pair(self):
        delete_journals = self._delete_group_with_copy_grps([])
        delete_journals.assert_called_once_with((0, 1))

    def test_group_repl_delete_group_keeps_journals_while_pairs_remain(self):
        delete_journals = self._delete_group_with_copy_grps(
            [{'copyGroupName': 'CGTEST'}])
        delete_journals.assert_not_called()

    def test_group_repl_delete_group_keeps_journals_when_list_fails(self):
        common = self._common()
        with mock.patch.object(
                common, '_resolve_copy_group_name', return_value='CGTEST'), \
            mock.patch.object(
                common, '_group_repl_journal_ids', return_value=(0, 1)), \
            mock.patch.object(
                common, '_group_repl_delete_group_volume',
                return_value={'id': TEST_VOLUME[0].id,
                              'status': 'deleted'}), \
            mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='x')), \
            mock.patch.object(
                common, '_delete_journals') as delete_journals:
            common._group_repl_delete_group(TEST_GROUP[0], [TEST_VOLUME[0]])
        delete_journals.assert_not_called()

    def test_pool_id_for_local_array_resolves_from_the_volume_host(self):
        common = self._common()
        common.rep_primary._stats = {'pools': []}
        volume = TEST_VOLUME[0]
        with mock.patch.object(
                common.rep_primary, 'get_pool_id_of_volume',
                return_value=31) as resolve:
            self.assertEqual(31, common._pool_id_for(common.rep_primary,
                                                     volume))
        resolve.assert_called_once_with(volume)

    def test_pool_id_for_local_array_falls_back_when_host_matches_nothing(
            self):
        common = self._common()
        common.rep_primary.storage_info['pool_id'] = [30, 31]
        common.rep_primary._stats = {'pools': []}
        with mock.patch.object(
                common.rep_primary, 'get_pool_id_of_volume',
                return_value=None) as resolve:
            self.assertEqual(
                30, common._pool_id_for(common.rep_primary, TEST_VOLUME[0]))
        resolve.assert_called_once()

    def test_pool_id_for_local_array_falls_back_before_the_first_stats_poll(
            self):
        common = self._common()
        common.rep_primary.storage_info['pool_id'] = [30, 31]
        common.rep_primary._stats = {}
        self.assertEqual(
            30, common._pool_id_for(common.rep_primary, TEST_VOLUME[0]))

    def test_pool_id_for_local_array_falls_back_when_never_polled(self):
        common = self._common()
        common.rep_primary.storage_info['pool_id'] = [30, 31]
        if hasattr(common.rep_primary, '_stats'):
            del common.rep_primary._stats
        self.assertEqual(
            30, common._pool_id_for(common.rep_primary, TEST_VOLUME[0]))

    def test_pool_id_for_remote_array_uses_the_configured_pool(self):
        common = self._common()
        common.rep_secondary.storage_info['pool_id'] = [40, 41]
        with mock.patch.object(
                common.rep_secondary, 'get_pool_id_of_volume') as resolve:
            self.assertEqual(
                40, common._pool_id_for(common.rep_secondary, TEST_VOLUME[0]))
        resolve.assert_not_called()

    def test_ac14_group_snapshot_uses_the_local_pool_for_a_local_svol(self):
        common = self._common()
        member = self._svol_only_volume(11)
        common.rep_primary.storage_info['pool_id'] = [30, 31]
        common.rep_primary._stats = {'pools': []}
        with self._label_stub(common.rep_primary, self._label_of(member)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'get_volume_extra_specs',
                    return_value={}), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                mock.patch.object(
                    common.rep_primary, 'get_pool_id_of_volume',
                    return_value=31), \
                mock.patch.object(
                    common.rep_primary, 'create_ldev',
                    return_value=22) as local_create, \
                mock.patch.object(common.rep_primary, 'modify_ldev_name'), \
                mock.patch.object(
                    common.rep_primary, '_create_ctg_snap_pair'), \
                self._snapshot_peer_patches(common) as peer:
            peer['create_ldev'].return_value = 99
            common._group_repl_create_group_snapshot(
                self.ctxt, TEST_GROUP_SNAP[0], [self._snapshot_of(member)])
        local_create.assert_called_once()
        self.assertEqual(31, local_create.call_args[0][2])

    @ddt.data('<is> True', '  <is> True  ')
    def test_pairs_at_create_time_false_for_a_group_replication_type(
            self, spec):
        common = self._common()
        self.assertFalse(common._pairs_at_create_time(
            TEST_VOLUME[0],
            {hbsd_replication._GROUP_REPL_VOLUME_SPEC: spec}))

    @ddt.data({}, {hbsd_replication._GROUP_REPL_VOLUME_SPEC: '<is> False'},
              {'other': 'x'}, None)
    def test_pairs_at_create_time_true_for_a_plain_replicated_type(
            self, extra_specs):
        common = self._common()
        with mock.patch.object(
                common.rep_primary, 'get_volume_extra_specs',
                return_value={}):
            self.assertTrue(
                common._pairs_at_create_time(TEST_VOLUME[0], extra_specs))

    def test_pairs_at_create_time_reads_the_type_when_not_given_specs(self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary, 'get_volume_extra_specs',
                return_value={
                    hbsd_replication._GROUP_REPL_VOLUME_SPEC: '<is> True'}):
            self.assertFalse(common._pairs_at_create_time(TEST_VOLUME[0]))

    def test_no_backend_wide_group_only_option(self):
        self.assertEqual(
            [], [opt.name for opt in hbsd_replication.COMMON_REPLICATION_OPTS
                 if 'group_only' in opt.name])

    @mock.patch.object(requests.Session, "request")
    @mock.patch.object(volume_types, 'get_volume_type_extra_specs')
    @mock.patch.object(objects.Volume, 'is_replicated')
    @mock.patch.object(volume_types, 'get_volume_type_qos_specs')
    def test_create_volume_group_replication_type_does_not_pair(
            self, get_volume_type_qos_specs, is_replicated,
            get_volume_type_extra_specs, request):
        is_replicated.return_value = True
        get_volume_type_qos_specs.return_value = {'qos_specs': None}
        get_volume_type_extra_specs.return_value = {
            'replication_enabled': '<is> True',
            hbsd_replication._GROUP_REPL_VOLUME_SPEC: '<is> True'}
        request.return_value = FakeResponse(202, COMPLETED_SUCCEEDED_RESULT)
        self.driver.common.rep_primary._stats = {
            'pools': [{'location_info': {'pool_id': 30}}]}
        self.driver.common.rep_secondary._stats = {
            'pools': [{'location_info': {'pool_id': 40}}]}
        ret = self.driver.create_volume(TEST_VOLUME[8])
        self.assertEqual(
            {'provider_location': json.dumps({'pldev': 1}),
             'replication_status': fields.ReplicationStatus.DISABLED},
            ret)

    # ------------------------------------------------------------------
    # hitachi_replication_role is gone. Which site holds a copy group's S
    # side, and which holds an sldev-only object, is read from the
    # storage systems when it is needed.
    # ------------------------------------------------------------------

    def test_ac1_no_replication_role_option_or_role_helpers(self):
        self.assertEqual(
            [], [opt.name for opt in hbsd_replication.COMMON_REPLICATION_OPTS
                 if opt.name == 'hitachi_replication_role'])
        common = self._common()
        for name in ('_is_target_role', '_svol_instance',
                     '_require_svol_instance'):
            self.assertFalse(hasattr(common, name), name)

    def test_ac2_leftover_role_line_in_cinder_conf_is_ignored(self):
        fd, path = tempfile.mkstemp(suffix='.conf')
        self.addCleanup(os.remove, path)
        with os.fdopen(fd, 'w') as conf_file:
            conf_file.write('[hitachi_dr]\n'
                            'hitachi_replication_role = target\n'
                            'hitachi_replication_mun = 2\n')
        parsed = cfg.ConfigOpts()
        parsed.register_opts(
            hbsd_replication.COMMON_REPLICATION_OPTS, group='hitachi_dr')
        parsed(args=[], default_config_files=[path])
        self.assertEqual(2, parsed.hitachi_dr.hitachi_replication_mun)
        self.assertRaises(
            cfg.NoSuchOptError, getattr, parsed.hitachi_dr,
            'hitachi_replication_role')

    def test_ac3_svol_side_is_local_when_rep_primary_holds_it(self):
        common = self._common()
        grp = self._svol_copy_grp('CGL')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=grp) as local, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer:
            self.assertEqual(
                (common.rep_primary, grp),
                common._copy_group_svol_side('CGL'))
        local.assert_called_once_with(
            None, 'CGL', is_secondary=True,
            ignore_message_id=[
                hbsd_replication._MSGID_SPECIFIED_OBJECT_DOES_NOT_EXIST])
        peer.assert_not_called()

    def test_ac3_svol_side_not_on_rep_primary_is_the_peer_unasked(self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()) as local, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer:
            self.assertEqual(
                (common.rep_secondary, None),
                common._copy_group_svol_side('CGP'))
        local.assert_called_once()
        peer.assert_not_called()

    def test_ac3_svol_side_asks_the_peer_when_rep_primary_errors(self):
        common = self._common()
        grp = self._svol_copy_grp('CGP')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='x')), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    return_value=grp) as peer:
            self.assertEqual(
                (common.rep_secondary, grp),
                common._copy_group_svol_side('CGP'))
        peer.assert_called_once_with(None, 'CGP', is_secondary=True)

    def test_ac3_svol_side_is_unknown_when_neither_site_confirms(self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='x')), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    side_effect=exception.VolumeDriverException(data='y')):
            exc = self.assertRaises(
                exception.VolumeDriverException,
                common._copy_group_svol_side, 'CG')
        self.assertIn(
            self._message_text(
                hbsd_utils.HBSDMsg.GROUP_REPLICATION_SIDE_UNKNOWN),
            str(exc))

    def test_ac3_svol_side_is_unknown_when_the_peer_is_not_initialized(self):
        common = self._common()
        common.rep_secondary = None
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='x')):
            self.assertRaises(
                exception.VolumeDriverException,
                common._copy_group_svol_side, 'CG')

    def test_ac3_svol_side_of_a_failed_over_backend_is_the_peer(self):
        common = self._common()
        common._active_backend_id = common.rep_secondary.backend_id
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp') as local, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer:
            self.assertEqual(
                (common.rep_secondary, None),
                common._copy_group_svol_side('CG'))
        local.assert_not_called()
        peer.assert_not_called()

    @mock.patch.object(requests.Session, "request")
    def test_ac3_not_found_reply_is_returned_not_raised(self, request):
        """The REST client hands KART30013-E back rather than raising it."""
        request.return_value = FakeResponse(404, dict(
            ERROR_RESULT,
            messageId=hbsd_rest_api.MSGID_SPECIFIED_OBJECT_DOES_NOT_EXIST))
        common = self._common()
        self.assertEqual(
            (common.rep_secondary, None),
            common._copy_group_svol_side('CG'))
        self.assertEqual(1, request.call_count)
        self.assertIn('/remote-mirror-copygroups/', request.call_args[0][1])

    def test_ac3_not_found_message_id_matches_the_rest_client(self):
        self.assertEqual(
            hbsd_rest_api.MSGID_SPECIFIED_OBJECT_DOES_NOT_EXIST,
            hbsd_replication._MSGID_SPECIFIED_OBJECT_DOES_NOT_EXIST)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac5_failover_replication_takes_over_the_peer_with_local_down(
            self, get_group_type_specs):
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='down')), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    return_value=self._svol_copy_grp(copy_group_name)), \
                mock.patch.object(
                    common.rep_secondary.client,
                    'takeover_remote_copy_grp') as takeover, \
                mock.patch.object(
                    common, '_wait_pair_status_change') as wait:
            model_update, _ = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[4]])
        takeover.assert_called_once_with(None, copy_group_name)
        self.assertIs(common.rep_secondary, wait.call_args[1]['instance'])
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.FAILED_OVER},
            model_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac20_failover_replication_raises_when_the_side_is_unknown(
            self, get_group_type_specs):
        common = self._common()
        down = exception.VolumeDriverException(data='down')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=down), \
                mock.patch.object(
                    common.rep_primary.client,
                    'takeover_remote_copy_grp') as local_takeover, \
                self._peer_unreachable(
                    common, 'get_remote_copy_grp',
                    'takeover_remote_copy_grp') as peer:
            for method in peer.values():
                method.side_effect = down
            self.assertRaises(
                exception.UnableToFailOver, common.failover_replication,
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[4]])
        local_takeover.assert_not_called()
        peer['takeover_remote_copy_grp'].assert_not_called()

    def test_ac6_list_replication_targets_reads_a_local_svol_side(self):
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp(copy_group_name)), \
                mock.patch.object(
                    common.rep_primary.client,
                    'get_remote_copy_grps') as list_grps:
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        list_grps.assert_not_called()
        self.assertEqual(
            {'replication_targets': [
                {'backend_id': common.rep_secondary_backend_id}]}, ret)

    def test_ac6_list_replication_targets_lists_via_the_peer_otherwise(self):
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        listing = [{'copyGroupName': copy_group_name}]
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grps',
                    return_value=listing) as list_grps:
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        list_grps.assert_called_once_with(common.rep_secondary.client)
        self.assertEqual(
            {'replication_targets': [
                {'backend_id': common.rep_secondary_backend_id}]}, ret)

    def test_ac6_list_replication_targets_raises_when_the_lookup_errors(
            self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=exception.VolumeDriverException(data='x')), \
                mock.patch.object(
                    common.rep_primary.client,
                    'get_remote_copy_grps') as list_grps:
            exc = self.assertRaises(
                exception.VolumeDriverException,
                common.list_replication_targets, self.ctxt, TEST_GROUP[0])
        list_grps.assert_not_called()
        self.assertIn(
            self._message_text(
                hbsd_utils.HBSDMsg.GROUP_REPLICATION_TARGETS_QUERY_FAILED),
            str(exc))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac7_enable_replication_adopts_from_a_local_svol_side(
            self, get_group_type_specs):
        common = self._common()
        volume = self._svol_only_volume(40)
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        enabled = fields.ReplicationStatus.ENABLED
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp(
                    copy_group_name, [{'svolLdevId': 40}])) as local, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer, \
                mock.patch.object(
                    common, '_group_repl_add_volume') as add_volume:
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        peer.assert_not_called()
        # One read tells both which site it is and what it pairs.
        local.assert_called_once()
        add_volume.assert_not_called()
        self.assertEqual({'replication_status': enabled}, model_update)
        self.assertEqual(
            [{'id': volume.id, 'replication_status': enabled}],
            volumes_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac7_enable_replication_adopts_from_a_peer_svol_side(
            self, get_group_type_specs):
        common = self._common()
        volume = self._svol_only_volume(40)
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    return_value=self._svol_copy_grp(
                        copy_group_name, [{'svolLdevId': 40}])) as peer:
            model_update, _ = common.enable_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        peer.assert_called_once_with(None, copy_group_name, is_secondary=True)
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ENABLED},
            model_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac20_enable_replication_adopt_marks_members_error_if_unknown(
            self, get_group_type_specs):
        common = self._common()
        volume = self._svol_only_volume(40)
        down = exception.VolumeDriverException(data='down')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                side_effect=down), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    side_effect=down):
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        error = fields.ReplicationStatus.ERROR
        self.assertEqual({'replication_status': error}, model_update)
        self.assertEqual(
            [{'id': volume.id, 'replication_status': error}],
            volumes_update)

    def _pair_status_on(self):
        self.override_config(
            'hitachi_replication_report_pair_status', True,
            group=conf.SHARED_CONF_GROUP)

    def test_ac8_pair_status_reads_each_group_from_its_listed_side(self):
        """One listing holds copy groups in both directions."""
        common = self._common()
        self._pair_status_on()
        rows = [{'copyGroupName': 'CGP', 'localDeviceGroupName': 'CGPP'},
                {'copyGroupName': 'CGS', 'localDeviceGroupName': 'CGSS'}]
        detail = {'pairStatus': 'PAIR', 'journalUsageRate': 1,
                  'copyPairs': []}
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=rows), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grp',
                    return_value=detail) as read, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer_read:
            capabilities = common._pair_status_capabilities()
        self.assertEqual(
            [mock.call(common.rep_secondary.client, 'CGP'),
             mock.call(None, 'CGS', is_secondary=True)],
            read.call_args_list)
        peer_read.assert_not_called()
        self.assertTrue(
            capabilities[hbsd_replication._PAIR_STATUS_ENUMERATED_KEY])
        self.assertEqual(
            {'CGP', 'CGS'},
            set(json.loads(capabilities[hbsd_replication._PAIR_STATUS_KEY])))

    def test_ac8_pair_status_journal_side_follows_the_group_side(self):
        common = self._common()
        self._pair_status_on()
        rows = [{'copyGroupName': 'CGS', 'localDeviceGroupName': 'CGSS'}]
        detail = {'pairStatus': 'PAIR',
                  'copyPairs': [{'pvolJournalId': 1, 'svolJournalId': 2}]}
        journals = {1: {'journalId': 1}, 2: {'journalId': 2}}
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=rows), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grp',
                    return_value=detail), \
                mock.patch.object(
                    common, '_journals_by_id',
                    return_value=journals) as journals_by_id:
            capabilities = common._pair_status_capabilities()
        journals_by_id.assert_called_once_with(common.rep_primary)
        state = json.loads(
            capabilities[hbsd_replication._PAIR_STATUS_KEY])['CGS']
        self.assertEqual(2, state['journal_id'])
        self.assertEqual(hbsd_utils.SECONDARY_STR, state['journal_side'])

    def test_ac8_pair_status_reads_local_svol_groups_when_listing_fails(
            self):
        common = self._common()
        self._pair_status_on()
        rows = [{'copyGroupName': 'CGS', 'localDeviceGroupName': 'CGSS'}]
        detail = {'pairStatus': 'SSWS', 'journalUsageRate': 0,
                  'copyPairs': []}
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                return_value=rows), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grp',
                    return_value=detail):
            common._pair_status_capabilities()
        common._pair_status_cache['time'] = (
            common._pair_status_cache['time'] - timedelta(hours=1))
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='down')), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grp',
                    return_value=detail) as read:
            capabilities = common._pair_status_capabilities()
        read.assert_called_once_with(None, 'CGS', is_secondary=True)
        self.assertFalse(
            capabilities[hbsd_replication._PAIR_STATUS_ENUMERATED_KEY])
        self.assertEqual(
            'SSWS',
            json.loads(capabilities[hbsd_replication._PAIR_STATUS_KEY])[
                'CGS']['pair_status'])

    def test_ac8_pair_status_of_a_failed_over_backend_reads_the_peer(self):
        """A failed-over backend reads only the copy groups it knows."""
        common = self._common()
        self._pair_status_on()
        common._active_backend_id = common.rep_secondary.backend_id
        common._known_copy_groups.add('CG1')
        detail = {'pairStatus': 'SSWS', 'journalUsageRate': 0,
                  'copyPairs': []}
        with mock.patch.object(
                common.rep_primary.client,
                'get_remote_copy_grps') as list_grps, \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    return_value=detail) as read:
            capabilities = common._pair_status_capabilities()
        list_grps.assert_not_called()
        read.assert_called_once_with(None, 'CG1', is_secondary=True)
        self.assertFalse(
            capabilities[hbsd_replication._PAIR_STATUS_ENUMERATED_KEY])

    def test_ac22_pair_status_never_raises_when_a_local_read_fails(self):
        common = self._common()
        self._pair_status_on()
        common._known_copy_groups.add('CGS')
        common._local_svol_copy_groups.add('CGS')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='peer')), \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grp',
                    side_effect=exception.VolumeDriverException(data='me')):
            capabilities = common._pair_status_capabilities()
        self.assertFalse(
            capabilities[hbsd_replication._PAIR_STATUS_ENUMERATED_KEY])
        self.assertEqual(
            {}, json.loads(capabilities[hbsd_replication._PAIR_STATUS_KEY]))

    def test_ac9_ssws_wait_polls_the_site_it_is_given(self):
        common = self._common()
        common.rep_secondary = None
        params = common._get_wait_pair_status_change_params(
            hbsd_replication._WAIT_SSWS, common.rep_primary)
        self.assertIs(common.rep_primary, params['instance'])
        self.assertIsNone(params['remote_client'])

    def test_ac9_ssws_wait_defaults_to_the_peer(self):
        """Per-volume failover relies on this default."""
        common = self._common()
        params = common._get_wait_pair_status_change_params(
            hbsd_replication._WAIT_SSWS)
        self.assertIs(common.rep_secondary, params['instance'])

    @ddt.data(True, False)
    def test_ac11_sldev_owner_is_the_site_carrying_its_label(self, local):
        common = self._common()
        volume = self._svol_only_volume(9)
        label = self._label_of(volume)
        with self._label_stub(
                common.rep_primary, label if local else None) as local_info, \
                self._label_stub(
                    common.rep_secondary, None if local else label):
            site, ldev_info = common._resolve_sldev_owner(volume)
        self.assertIs(
            common.rep_primary if local else common.rep_secondary, site)
        self.assertEqual(label, ldev_info['label'])
        local_info.assert_called_once_with(None, 9)

    def test_ac11_sldev_owner_of_a_paired_volume_reads_no_label(self):
        common = self._common()
        with mock.patch.object(
                common.rep_primary, 'get_ldev_info') as local_info, \
                mock.patch.object(
                    common.rep_secondary, 'get_ldev_info') as peer_info:
            self.assertEqual(
                (common.rep_secondary, None),
                common._resolve_sldev_owner(TEST_VOLUME[4]))
        local_info.assert_not_called()
        peer_info.assert_not_called()

    def test_ac11_sldev_owner_of_a_snapshot_matches_the_snapshot_id(self):
        common = self._common()
        snapshot = self._snapshot_of(TEST_VOLUME[0], sldev=5)
        with self._label_stub(
                common.rep_primary, self._label_of(snapshot)), \
                self._label_stub(
                    common.rep_secondary, self._label_of(TEST_VOLUME[0])):
            site, _ = common._resolve_sldev_owner(snapshot)
        self.assertIs(common.rep_primary, site)

    def test_ac11_sldev_owner_takes_a_match_when_the_other_site_is_down(
            self):
        """The disaster case: the peer is gone, the adopted S-VOL is here."""
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(
                    common.rep_secondary,
                    exception.VolumeDriverException(data='down')):
            site, _ = common._resolve_sldev_owner(volume)
        self.assertIs(common.rep_primary, site)

    def test_ac12_delete_volume_busy_local_svol_raises_volume_is_busy(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev',
                    side_effect=exception.VolumeDriverException(
                        hbsd_utils.BUSY_MESSAGE)):
            self.assertRaises(
                exception.VolumeIsBusy, common.delete_volume, volume)

    def _delete_patches(self, common):
        return mock.patch.multiple(
            common.rep_secondary, delete_ldev=mock.DEFAULT,
            delete_volume=mock.DEFAULT)

    def test_ac19_delete_volume_on_neither_site_skips_every_time(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                self._delete_patches(common) as peer:
            common.delete_volume(volume)
            common.delete_volume(volume)
        local_delete.assert_not_called()
        for method in peer.values():
            method.assert_not_called()

    def test_ac20_delete_volume_raises_and_writes_nothing_if_unknown(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(
                common.rep_primary,
                exception.VolumeDriverException(data='down')), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                self._delete_patches(common) as peer:
            exc = self.assertRaises(
                exception.VolumeDriverException, common.delete_volume,
                volume)
        self.assertIn(
            self._message_text(
                hbsd_utils.HBSDMsg.GROUP_REPLICATION_SVOL_UNRESOLVED),
            str(exc))
        local_delete.assert_not_called()
        for method in peer.values():
            method.assert_not_called()

    def test_ac21_delete_volume_raises_when_both_sites_claim_the_ldev(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        label = self._label_of(volume)
        with self._label_stub(common.rep_primary, label), \
                self._label_stub(common.rep_secondary, label), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                self._delete_patches(common) as peer:
            exc = self.assertRaises(
                exception.VolumeDriverException, common.delete_volume,
                volume)
        self.assertIn(
            self._message_text(
                hbsd_utils.HBSDMsg.GROUP_REPLICATION_SVOL_UNRESOLVED),
            str(exc))
        local_delete.assert_not_called()
        for method in peer.values():
            method.assert_not_called()

    def test_ac20_group_delete_marks_an_unresolvable_member_error(self):
        common = self._common()
        volume = self._svol_only_volume(20)
        down = exception.VolumeDriverException(data='down')
        with self._label_stub(common.rep_primary, down), \
                self._label_stub(common.rep_secondary, down), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.object(
                    common.rep_secondary, 'delete_ldev') as remote_delete:
            update = common._group_repl_delete_group_volume(
                TEST_GROUP[0], volume, 'CGTEST')
        self.assertEqual({'id': volume.id, 'status': 'error'}, update)
        local_delete.assert_not_called()
        remote_delete.assert_not_called()

    def test_ac14_group_snapshot_member_on_neither_site_is_an_error(self):
        common = self._common()
        member = self._svol_only_volume(11)
        snapshot = self._snapshot_of(member)
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'create_ldev') as local_create, \
                self._snapshot_peer_patches(common) as peer:
            model_update, snapshots_update = (
                common._group_repl_create_group_snapshot(
                    self.ctxt, TEST_GROUP_SNAP[0], [snapshot]))
        for method in peer.values():
            method.assert_not_called()
        local_create.assert_not_called()
        self.assertEqual(
            {'status': fields.GroupSnapshotStatus.ERROR}, model_update)
        self.assertEqual(
            [{'id': snapshot.id, 'status': fields.SnapshotStatus.ERROR}],
            snapshots_update)

    def test_ac15_delete_group_snapshot_on_the_peer_uses_its_delete_group(
            self):
        common = self._common()
        snapshot = self._snapshot_of(TEST_VOLUME[0], sldev=33)
        expected = ({'status': TEST_GROUP_SNAP[0].status},
                    [{'id': snapshot.id, 'status': 'deleted'}])
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(
                    common.rep_secondary, self._label_of(snapshot)), \
                mock.patch.object(
                    common.rep_secondary, '_delete_group',
                    return_value=expected) as remote_delete_group, \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete:
            self.assertEqual(
                expected, common._group_repl_delete_group_snapshot(
                    TEST_GROUP_SNAP[0], [snapshot]))
        remote_delete_group.assert_called_once_with(
            TEST_GROUP_SNAP[0], [snapshot], True)
        local_delete.assert_not_called()

    def test_ac20_delete_group_snapshot_marks_an_unknown_member_error(self):
        common = self._common()
        snapshot = self._snapshot_of(TEST_VOLUME[0], sldev=33)
        down = exception.VolumeDriverException(data='down')
        with self._label_stub(common.rep_primary, down), \
                self._label_stub(common.rep_secondary, down), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.multiple(
                    common.rep_secondary, delete_ldev=mock.DEFAULT,
                    _delete_group=mock.DEFAULT) as peer:
            peer['_delete_group'].return_value = (None, [])
            model_update, snapshots_update = (
                common._group_repl_delete_group_snapshot(
                    TEST_GROUP_SNAP[0], [snapshot]))
        for method in peer.values():
            method.assert_not_called()
        local_delete.assert_not_called()
        self.assertEqual({'status': 'error'}, model_update)
        self.assertEqual(
            [{'id': snapshot.id, 'status': 'error'}], snapshots_update)

    def _clone_peer_patches(self, common):
        return mock.patch.multiple(
            common.rep_secondary, create_cloned_volume=mock.DEFAULT,
            create_volume_from_snapshot=mock.DEFAULT,
            copy_on_storage=mock.DEFAULT, modify_ldev_name=mock.DEFAULT)

    def test_ac16_create_group_from_src_clones_a_local_source_locally(self):
        """rep_secondary's LDEV with the source's number is another object's.

        Only its label is read; cloning it would copy the other's data.
        """
        common = self._common()
        source = self._svol_only_volume(10)
        volume = TEST_VOLUME[1]
        with self._label_stub(common.rep_primary, self._label_of(source)), \
                self._label_stub(common.rep_secondary, None) as peer_info, \
                mock.patch.object(
                    common.rep_primary, 'get_volume_extra_specs',
                    return_value={}), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                mock.patch.object(
                    common.rep_primary, 'copy_on_storage',
                    return_value=50) as local_copy, \
                mock.patch.object(
                    common.rep_primary, 'modify_ldev_name') as local_label, \
                self._clone_peer_patches(common) as peer:
            model_update, volumes_update = (
                common._group_repl_create_group_from_src(
                    self.ctxt, TEST_GROUP[0], [volume], None, [source]))
        for method in peer.values():
            method.assert_not_called()
        peer_info.assert_called_once_with(None, 10)
        local_copy.assert_called_once()
        self.assertEqual(10, local_copy.call_args[0][0])
        local_label.assert_called_once_with(50, volume.id.replace('-', ''))
        self.assertIsNone(model_update)
        self.assertEqual(
            [{'id': volume.id,
              'provider_location': json.dumps({'sldev': 50}),
              'replication_status': fields.ReplicationStatus.DISABLED}],
            volumes_update)

    def test_ac16_create_group_from_src_clones_a_peer_source_as_today(self):
        common = self._common()
        source = self._svol_only_volume(10)
        volume = TEST_VOLUME[1]
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(
                    common.rep_secondary, self._label_of(source)), \
                mock.patch.object(
                    common.rep_primary, 'copy_on_storage') as local_copy, \
                mock.patch.object(
                    common.rep_secondary, 'create_cloned_volume',
                    return_value={'provider_location': '51'}) as clone:
            _, volumes_update = common._group_repl_create_group_from_src(
                self.ctxt, TEST_GROUP[0], [volume], None, [source])
        clone.assert_called_once_with(volume, source)
        local_copy.assert_not_called()
        self.assertEqual(
            json.dumps({'sldev': 51}), volumes_update[0]['provider_location'])

    def test_ac16_create_group_from_src_source_on_neither_site_raises(self):
        common = self._common()
        source = self._svol_only_volume(10)
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'copy_on_storage') as local_copy, \
                self._clone_peer_patches(common) as peer:
            self.assertRaises(
                exception.VolumeDriverException,
                common._group_repl_create_group_from_src,
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[1]], None, [source])
        local_copy.assert_not_called()
        for method in peer.values():
            method.assert_not_called()

    def _manageable_on(self, site):
        """Make site report an LDEV that the adoption checks accept."""
        site._pair_targets = [(CONFIG_MAP['port_id'], 5)]
        site._PAIR_TARGET_NAME = 'HBSD-pair00'
        return mock.patch.object(
            site, 'get_ldev_info',
            return_value=self._adopt_ldev_info(ports=[self._port()]))

    def _manage_peer_patches(self, common):
        return mock.patch.multiple(
            common.rep_secondary, get_ldev_by_name=mock.DEFAULT,
            get_ldev_info=mock.DEFAULT, modify_ldev_name=mock.DEFAULT,
            get_ldev_size_in_gigabyte=mock.DEFAULT)

    def test_ac17_group_manage_adopts_from_a_local_svol_side(self):
        common = self._common()
        volume = self._bound_volume()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp('CGBOUND')), \
                mock.patch.object(
                    common.rep_secondary.client,
                    'get_remote_copy_grp') as peer_read, \
                mock.patch.object(
                    common.rep_primary, 'get_ldev_by_name',
                    return_value=7) as by_name, \
                self._manageable_on(common.rep_primary), \
                mock.patch.object(
                    common.rep_primary, 'modify_ldev_name') as relabel, \
                mock.patch.object(
                    common.rep_primary, 'get_qos_specs_from_ldev',
                    return_value=None), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                self._manage_peer_patches(common) as peer:
            model_update = common.manage_existing(
                volume, self.test_existing_ref_name)
        for method in peer.values():
            method.assert_not_called()
        peer_read.assert_not_called()
        by_name.assert_called_once_with(
            self.test_existing_ref_name['source-name'].replace('-', ''))
        relabel.assert_called_once_with(7, volume.id.replace('-', ''))
        self.assertEqual(
            json.dumps({'sldev': 7}), model_update['provider_location'])

    def test_ac17_group_manage_adopts_from_a_confirmed_peer_svol_side(self):
        common = self._common()
        volume = self._bound_volume()
        peer_grp = self._svol_copy_grp('CGBOUND')
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    return_value=peer_grp) as peer_read, \
                mock.patch.object(
                    common.rep_secondary, 'get_ldev_by_name',
                    return_value=7), \
                self._manageable_on(common.rep_secondary), \
                mock.patch.object(
                    common.rep_secondary, 'modify_ldev_name') as relabel, \
                mock.patch.object(
                    common.rep_secondary, 'get_qos_specs_from_ldev',
                    return_value=None), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                mock.patch.object(
                    common.rep_primary, 'modify_ldev_name') as local_relabel:
            common.manage_existing(volume, self.test_existing_ref_name)
        peer_read.assert_called_once_with(
            None, 'CGBOUND', is_secondary=True)
        relabel.assert_called_once_with(7, volume.id.replace('-', ''))
        local_relabel.assert_not_called()

    @ddt.data(
        exception.VolumeDriverException(data='not on this array'),
        None)
    def test_ac17_group_manage_raises_unless_a_site_holds_the_copy_group(
            self, rep_primary_error):
        """Found on neither array (None), or neither array answers."""
        common = self._common()
        volume = self._bound_volume()
        local = (mock.patch.object(
            common.rep_primary.client, 'get_remote_copy_grp',
            side_effect=rep_primary_error) if rep_primary_error else
            mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()))
        with local, \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    side_effect=exception.VolumeDriverException(
                        data='not here')), \
                mock.patch.object(
                    common.rep_primary, 'modify_ldev_name') as local_relabel, \
                self._manage_peer_patches(common) as peer:
            self.assertRaises(
                exception.ManageExistingInvalidReference,
                common.manage_existing, volume, self.test_existing_ref_name)
        for method in peer.values():
            method.assert_not_called()
        local_relabel.assert_not_called()

    def test_ac17_group_manage_get_size_reads_the_local_svol_side(self):
        common = self._common()
        volume = self._bound_volume()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp('CGBOUND')), \
                mock.patch.object(
                    common.rep_primary, 'get_ldev_by_name', return_value=7), \
                mock.patch.object(
                    common.rep_primary, 'get_ldev_size_in_gigabyte',
                    return_value=10) as size, \
                self._manage_peer_patches(common) as peer:
            self.assertEqual(
                10, common.manage_existing_get_size(
                    volume, self.test_existing_ref_name))
        for method in peer.values():
            method.assert_not_called()
        size.assert_called_once_with(7, self.test_existing_ref_name)

    def _unmanage_patches(self, common):
        return (
            mock.patch.object(common.rep_primary, 'modify_ldev_name'),
            mock.patch.object(common.rep_secondary, 'modify_ldev_name'))

    def test_ac18_group_unmanage_clears_a_local_svol_nickname_locally(self):
        common = self._common()
        volume = self._bound_volume(sldev=8)
        local_patch, peer_patch = self._unmanage_patches(common)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                local_patch as local_clear, peer_patch as remote_clear:
            common.unmanage(volume)
        local_clear.assert_called_once_with(8, '')
        remote_clear.assert_not_called()

    def test_ac19_group_unmanage_on_neither_site_skips_the_clear(self):
        common = self._common()
        volume = self._bound_volume(sldev=8)
        local_patch, peer_patch = self._unmanage_patches(common)
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(common.rep_secondary, None), \
                local_patch as local_clear, peer_patch as remote_clear:
            common.unmanage(volume)
        local_clear.assert_not_called()
        remote_clear.assert_not_called()

    def test_ac20_group_unmanage_raises_when_the_lookup_errors(self):
        common = self._common()
        volume = self._bound_volume(sldev=8)
        down = exception.VolumeDriverException(data='down')
        local_patch, peer_patch = self._unmanage_patches(common)
        with self._label_stub(common.rep_primary, down), \
                self._label_stub(common.rep_secondary, down), \
                local_patch as local_clear, peer_patch as remote_clear:
            self.assertRaises(
                exception.VolumeDriverException, common.unmanage, volume)
        local_clear.assert_not_called()
        remote_clear.assert_not_called()

    def test_ac18_group_unmanage_failed_clear_is_logged_not_raised(self):
        common = self._common()
        volume = self._bound_volume(sldev=8)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'modify_ldev_name',
                    side_effect=exception.VolumeDriverException(
                        data='x')) as local_clear, \
                mock.patch.object(
                    common.rep_secondary, 'modify_ldev_name') as remote_clear:
            common.unmanage(volume)
        local_clear.assert_called_once_with(8, '')
        remote_clear.assert_not_called()

    def test_ac25_psr_dr_contract_is_unchanged(self):
        self.assertEqual(
            'group_replication_pairs', hbsd_replication._PAIR_STATUS_KEY)
        self.assertEqual(
            'group_replication_pairs_updated_at',
            hbsd_replication._PAIR_STATUS_UPDATED_KEY)
        self.assertEqual(
            'group_replication_peer_initialized',
            hbsd_replication._PAIR_STATUS_PEER_KEY)
        self.assertEqual(
            'group_replication_pairs_enumerated',
            hbsd_replication._PAIR_STATUS_ENUMERATED_KEY)
        for mode in ('graceful', 'emergency'):
            self.assertEqual(
                ('backend2', mode),
                hbsd_replication._parse_failover_target('backend2:' + mode))
        self.assertEqual(
            (hbsd_replication._REP_FAILBACK, None),
            hbsd_replication._parse_failover_target(
                hbsd_replication._REP_FAILBACK))

    def _adopted_here(self, common, volume, paired=False):
        """Label volume's LDEV on rep_primary only, in a pair if paired."""
        attributes = GET_LDEV_RESULT['attributes'] + (
            [hbsd_rest.REP_ATTR] if paired else [])
        return (
            mock.patch.object(
                common.rep_primary, 'get_ldev_info',
                return_value=dict(GET_LDEV_RESULT, attributes=attributes,
                                  label=self._label_of(volume))),
            self._label_stub(common.rep_secondary, None))

    def _local_pair(self, common, svol_status):
        return mock.patch.object(
            common.rep_primary.client, 'get_remote_copy_grp',
            return_value=self._svol_copy_grp(
                'CGBOUND', [{'svolLdevId': 9, 'svolStatus': svol_status}]))

    def test_attaching_a_local_svol_maps_its_ldev_here(self):
        common = self._common()
        volume = self._bound_volume(sldev=9)
        here, peer = self._adopted_here(common, volume)
        with here, peer, mock.patch.object(
                common.rep_primary, 'initialize_connection',
                return_value='conn_info') as attach:
            self.assertEqual(
                'conn_info',
                common.initialize_connection(volume, DEFAULT_CONNECTOR))
        self.assertEqual(
            9, common.rep_primary.get_ldev(attach.call_args[0][0]))
        self.assertEqual(json.dumps({'sldev': 9}), volume.provider_location)

    def test_attaching_a_taken_over_local_svol_maps_it_here(self):
        common = self._common()
        volume = self._bound_volume(sldev=9)
        here, peer = self._adopted_here(common, volume, paired=True)
        with here, peer, self._local_pair(common, 'SSWS'), \
                mock.patch.object(
                    common.rep_primary, 'initialize_connection') as attach:
            common.initialize_connection(volume, DEFAULT_CONNECTOR)
        attach.assert_called_once()

    def test_attaching_a_still_paired_local_svol_raises(self):
        common = self._common()
        volume = self._bound_volume(sldev=9)
        here, peer = self._adopted_here(common, volume, paired=True)
        with here, peer, self._local_pair(common, 'PAIR'), \
                mock.patch.object(
                    common.rep_primary, 'initialize_connection') as attach:
            exc = self.assertRaises(
                exception.VolumeDriverException,
                common.initialize_connection, volume, DEFAULT_CONNECTOR)
        self.assertIn('is in a remote replication pair', str(exc))
        attach.assert_not_called()

    def test_attaching_an_svol_on_the_peer_raises_other_site_error(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, None), \
                self._label_stub(common.rep_secondary,
                                 self._label_of(volume)):
            exc = self.assertRaises(
                exception.VolumeDriverException, common.initialize_connection,
                volume, DEFAULT_CONNECTOR)
        self.assertIn('exists in the other site', str(exc))

    def test_detaching_a_paired_local_svol_unmaps_it_here(self):
        common = self._common()
        volume = self._bound_volume(sldev=9)
        here, peer = self._adopted_here(common, volume, paired=True)
        with here, peer, mock.patch.object(
                common.rep_primary, 'terminate_connection') as detach:
            common.terminate_connection(volume, DEFAULT_CONNECTOR)
        self.assertEqual(
            9, common.rep_primary.get_ldev(detach.call_args[0][0]))

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac26_failback_still_resyncs_through_rep_secondary(
            self, get_group_type_specs):
        """Known gap: failback always resyncs through rep_secondary."""
        common = self._common()
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp(copy_group_name)), \
                mock.patch.object(
                    common.rep_secondary.client,
                    'resync_remote_copy_grp') as resync, \
                mock.patch.object(common, '_wait_pair_status_change'):
            model_update, _ = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[4]],
                secondary_backend_id=hbsd_replication._REP_FAILBACK)
        resync.assert_called_once_with(
            common.rep_primary.client, copy_group_name,
            common.driver_info['rep_type_async'], swap=True,
            is_secondary=True)
        self.assertEqual(
            {'replication_status': fields.ReplicationStatus.ENABLED},
            model_update)

    def _both_directions(self, common):
        """TEST_GROUP[0], whose S side is local, and a group of the peer's.

        TEST_GROUP[1] cannot be the second group: copy group names keep
        only the head of a group id, and its id differs from [0] in the
        tail.
        """
        peer_group = fake_group.fake_group_obj(
            CTXT, id='21000000-0000-0000-0000-000000000001',
            status='available')
        local_cg = common._create_group_copy_group_name(TEST_GROUP[0].id)
        peer_cg = common._create_group_copy_group_name(peer_group.id)

        def svol_side(remote_client, copy_group_name, is_secondary=False,
                      **kwargs):
            if copy_group_name == local_cg:
                return self._svol_copy_grp(copy_group_name)
            return self._svol_side_not_here()
        return peer_group, local_cg, peer_cg, mock.patch.object(
            common.rep_primary.client, 'get_remote_copy_grp',
            side_effect=svol_side)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac28_failover_replication_serves_both_directions(
            self, get_group_type_specs):
        common = self._common()
        peer_group, local_cg, peer_cg, svol_side = self._both_directions(
            common)
        with svol_side, \
                mock.patch.object(
                    common.rep_primary.client,
                    'takeover_remote_copy_grp') as local_takeover, \
                mock.patch.object(
                    common.rep_secondary.client,
                    'takeover_remote_copy_grp') as peer_takeover, \
                mock.patch.object(common, '_wait_pair_status_change'):
            local_update, _ = common.failover_replication(
                self.ctxt, TEST_GROUP[0], [self._svol_only_volume(40)])
            peer_update, _ = common.failover_replication(
                self.ctxt, peer_group, [TEST_VOLUME[4]])
        local_takeover.assert_called_once_with(None, local_cg)
        peer_takeover.assert_called_once_with(None, peer_cg)
        failed_over = {
            'replication_status': fields.ReplicationStatus.FAILED_OVER}
        self.assertEqual(failed_over, local_update)
        self.assertEqual(failed_over, peer_update)

    def test_ac28_list_replication_targets_serves_both_directions(self):
        common = self._common()
        peer_group, local_cg, peer_cg, svol_side = self._both_directions(
            common)
        with svol_side, \
                mock.patch.object(
                    common.rep_primary.client, 'get_remote_copy_grps',
                    return_value=[{'copyGroupName': peer_cg}]) as list_grps:
            local_ret = common.list_replication_targets(
                self.ctxt, TEST_GROUP[0])
            peer_ret = common.list_replication_targets(
                self.ctxt, peer_group)
        targets = {'replication_targets': [
            {'backend_id': common.rep_secondary_backend_id}]}
        self.assertEqual(targets, local_ret)
        self.assertEqual(targets, peer_ret)
        list_grps.assert_called_once_with(common.rep_secondary.client)

    def test_ac28_group_delete_removes_each_member_where_it_lives(self):
        """An adopted local member and a paired member on one backend."""
        common = self._common()
        adopted = self._svol_only_volume(20)
        with self._label_stub(common.rep_primary, self._label_of(adopted)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary.client,
                    'delete_remote_copypair') as delete_pair, \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.object(
                    common.rep_primary, 'delete_volume') as local_volume, \
                mock.patch.object(
                    common.rep_secondary, 'delete_volume') as remote_volume:
            adopted_update = common._group_repl_delete_group_volume(
                TEST_GROUP[0], adopted, 'CGL')
            paired_update = common._group_repl_delete_group_volume(
                TEST_GROUP[1], TEST_VOLUME[4], 'CGP')
        local_delete.assert_called_once_with(20, mock.ANY)
        delete_pair.assert_called_once_with(
            common.rep_secondary.client, 'CGP', 4, 4)
        remote_volume.assert_called_once_with(TEST_VOLUME[4])
        local_volume.assert_called_once_with(TEST_VOLUME[4])
        self.assertEqual('deleted', adopted_update['status'])
        self.assertEqual('deleted', paired_update['status'])

    # ------------------------------------------------------------------
    # Error and edge paths of the two lookups.
    # ------------------------------------------------------------------

    def test_ac11_sldev_owner_found_locally_without_a_peer(self):
        """The peer never initialized; the adopted S-VOL is still here."""
        common = self._common()
        volume = self._svol_only_volume(9)
        common.rep_secondary = None
        with self._label_stub(common.rep_primary, self._label_of(volume)):
            site, _ = common._resolve_sldev_owner(volume)
        self.assertIs(common.rep_primary, site)

    def test_ac20_delete_volume_reraises_a_local_delete_error(self):
        common = self._common()
        volume = self._svol_only_volume(9)
        with self._label_stub(common.rep_primary, self._label_of(volume)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev',
                    side_effect=exception.VolumeDriverException(
                        data='array error')):
            self.assertRaises(
                exception.VolumeDriverException, common.delete_volume,
                volume)

    def test_ac15_delete_group_snapshot_splits_mixed_owners(self):
        """One S-VOL here, one on the peer: each is deleted where it is."""
        common = self._common()
        local = self._snapshot_of(TEST_VOLUME[0], sldev=33)
        remote = self._snapshot_of(
            TEST_VOLUME[1], sldev=34,
            snapshot_id='10000000-0000-0000-0000-000000000098')

        def ldev_info(site_label):
            def answer(keys, ldev):
                return dict(GET_LDEV_RESULT, label=site_label.get(
                    ldev, 'f' * 32))
            return answer
        with mock.patch.object(
                common.rep_primary, 'get_ldev_info',
                side_effect=ldev_info({33: self._label_of(local)})), \
                mock.patch.object(
                    common.rep_secondary, 'get_ldev_info',
                    side_effect=ldev_info({34: self._label_of(remote)})), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev') as local_delete, \
                mock.patch.object(
                    common.rep_secondary,
                    'delete_snapshot') as remote_delete, \
                mock.patch.object(
                    common.rep_secondary, '_delete_group') as remote_group:
            model_update, snapshots_update = (
                common._group_repl_delete_group_snapshot(
                    TEST_GROUP_SNAP[0], [local, remote]))
        local_delete.assert_called_once_with(33, mock.ANY)
        remote_delete.assert_called_once_with(remote)
        remote_group.assert_not_called()
        self.assertEqual(
            {'status': TEST_GROUP_SNAP[0].status}, model_update)
        self.assertEqual(
            [{'id': local.id, 'status': 'deleted'},
             {'id': remote.id, 'status': 'deleted'}], snapshots_update)

    def test_ac15_delete_group_snapshot_reports_a_busy_local_svol(self):
        common = self._common()
        snapshot = self._snapshot_of(TEST_VOLUME[0], sldev=33)
        with self._label_stub(
                common.rep_primary, self._label_of(snapshot)), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev',
                    side_effect=exception.VolumeDriverException(
                        hbsd_utils.BUSY_MESSAGE)):
            model_update, snapshots_update = (
                common._group_repl_delete_group_snapshot(
                    TEST_GROUP_SNAP[0], [snapshot]))
        self.assertEqual({'status': 'error'}, model_update)
        self.assertEqual(
            [{'id': snapshot.id, 'status': 'available'}], snapshots_update)

    def test_ac8_first_poll_with_the_listing_down_reports_nothing(self):
        """No cache and no local S side leaves enumerated false."""
        common = self._common()
        self._pair_status_on()
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grps',
                side_effect=exception.VolumeDriverException(data='down')):
            capabilities = common._pair_status_capabilities()
        self.assertEqual(
            {hbsd_replication._PAIR_STATUS_PEER_KEY: True,
             hbsd_replication._PAIR_STATUS_ENUMERATED_KEY: False},
            capabilities)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac20_enable_replication_adopt_errors_when_the_peer_fails(
            self, get_group_type_specs):
        """rep_primary says no; the peer then cannot be read."""
        common = self._common()
        volume = self._svol_only_volume(40)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_side_not_here()), \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    side_effect=exception.VolumeDriverException(data='x')):
            _, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        self.assertEqual(
            [{'id': volume.id,
              'replication_status': fields.ReplicationStatus.ERROR}],
            volumes_update)

    @mock.patch.object(group_types, 'get_group_type_specs',
                       return_value='<is> True')
    def test_ac7_enable_replication_errors_a_member_not_in_the_group(
            self, get_group_type_specs):
        common = self._common()
        volume = self._svol_only_volume(40)
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp',
                return_value=self._svol_copy_grp(
                    copy_group_name, [{'svolLdevId': 41}])):
            model_update, volumes_update = common.enable_replication(
                self.ctxt, TEST_GROUP[0], [volume])
        error = fields.ReplicationStatus.ERROR
        self.assertEqual({'replication_status': error}, model_update)
        self.assertEqual(
            [{'id': volume.id, 'replication_status': error}],
            volumes_update)

    def test_ac16_create_group_from_src_removes_its_clones_on_failure(self):
        """A failed second clone takes the first one back off its site.

        The cleanup's own failure is logged; the clone error propagates.
        """
        common = self._common()
        sources = [self._svol_only_volume(10), self._svol_only_volume(
            11, volume_id='00000000-0000-0000-0000-000000000098')]

        def ldev_info(keys, ldev):
            source = sources[ldev - 10]
            return dict(GET_LDEV_RESULT, label=self._label_of(source))
        with mock.patch.object(
                common.rep_primary, 'get_ldev_info', side_effect=ldev_info), \
                self._label_stub(common.rep_secondary, None), \
                mock.patch.object(
                    common.rep_primary, 'get_volume_extra_specs',
                    return_value={}), \
                mock.patch.object(
                    hbsd_utils, 'get_qos_specs_from_volume',
                    return_value=None), \
                mock.patch.object(
                    common.rep_primary, 'copy_on_storage',
                    side_effect=[50, exception.VolumeDriverException(
                        'copy failed')]), \
                mock.patch.object(common.rep_primary, 'modify_ldev_name'), \
                mock.patch.object(
                    common.rep_primary, 'delete_ldev',
                    side_effect=exception.VolumeDriverException(
                        'cleanup failed')) as cleanup:
            exc = self.assertRaises(
                exception.VolumeDriverException,
                common._group_repl_create_group_from_src,
                self.ctxt, TEST_GROUP[0], [TEST_VOLUME[1], TEST_VOLUME[2]],
                None, sources)
        cleanup.assert_called_once_with(50)
        self.assertIn('copy failed', str(exc))

    def test_ac19_group_unmanage_of_a_volume_without_svol_asks_nobody(self):
        """No S-VOL, so no site is asked and nothing is raised."""
        common = self._common()
        volume = self._bound_volume()
        with mock.patch.object(
                common.rep_primary, 'get_ldev_info') as local_info, \
                mock.patch.object(
                    common.rep_secondary, 'get_ldev_info') as peer_info:
            common.unmanage(volume)
        local_info.assert_not_called()
        peer_info.assert_not_called()

    @ddt.data(True, False)
    def test_ac6_list_replication_targets_of_a_failed_over_backend(
            self, found):
        """A failed-over backend reads the peer's S side."""
        common = self._common()
        common._active_backend_id = common.rep_secondary.backend_id
        copy_group_name = common._create_group_copy_group_name(
            TEST_GROUP[0].id)
        answer = ({'return_value': self._svol_copy_grp(copy_group_name)}
                  if found else
                  {'side_effect': exception.VolumeDriverException(data='x')})
        with mock.patch.object(
                common.rep_primary.client, 'get_remote_copy_grp') as local, \
                mock.patch.object(
                    common.rep_secondary.client, 'get_remote_copy_grp',
                    **answer) as peer:
            ret = common.list_replication_targets(self.ctxt, TEST_GROUP[0])
        local.assert_not_called()
        peer.assert_called_once_with(None, copy_group_name, is_secondary=True)
        self.assertEqual(
            {'replication_targets': (
                [{'backend_id': common.rep_secondary_backend_id}] if found
                else [])}, ret)


# Shorthand alias
_GreenThreadCompat = hbsd_replication.HBSDREPLICATION.GreenThreadCompat


def _make_resolved_future(value):
    """Return a Future whose result is already set to *value*."""
    f = futurist.Future()
    f.set_result(value)
    return f


def _make_failed_future(exc):
    """Return a Future whose exception is already set to *exc*."""
    f = futurist.Future()
    f.set_exception(exc)
    return f


class TestGreenThreadCompatWait(test.TestCase):
    """Tests for HBSDREPLICATION.GreenThreadCompat.wait()."""

    # ------------------------------------------------------------------
    # (1) wait() returns the callable's result
    # ------------------------------------------------------------------
    def test_wait_returns_callable_result(self):
        """wait() must surface the value produced by the underlying future."""
        expected = 42
        compat = _GreenThreadCompat(_make_resolved_future(expected))

        result = compat.wait()

        self.assertEqual(expected, result)

    def test_wait_returns_none_when_callable_returns_none(self):
        """wait() returns None when the callable returns None (common case)."""
        compat = _GreenThreadCompat(_make_resolved_future(None))

        result = compat.wait()

        self.assertIsNone(result)

    def test_wait_returns_non_trivial_object(self):
        """wait() faithfully returns arbitrary objects, not just scalars."""
        expected = {'ldev': 100, 'port': 'CL1-A'}
        compat = _GreenThreadCompat(_make_resolved_future(expected))

        result = compat.wait()

        self.assertIs(expected, result)

    # ------------------------------------------------------------------
    # (2) wait() propagates an exception raised by the callable
    # ------------------------------------------------------------------
    def test_wait_propagates_exception(self):
        """wait() must re-raise exceptions set on the future."""
        exc = RuntimeError('secondary operation failed')
        compat = _GreenThreadCompat(_make_failed_future(exc))

        self.assertRaises(RuntimeError, compat.wait)

    def test_wait_propagates_exception_message(self):
        """The original exception message is preserved when re-raised."""
        msg = 'disk unavailable'
        exc = IOError(msg)
        compat = _GreenThreadCompat(_make_failed_future(exc))

        raised = self.assertRaises(IOError, compat.wait)
        self.assertIn(msg, str(raised))

    def test_wait_propagates_exception_type_exactly(self):
        """The exact exception type (not a wrapper) is raised by wait()."""

        class _CustomError(Exception):
            pass

        exc = _CustomError('custom')
        compat = _GreenThreadCompat(_make_failed_future(exc))

        self.assertRaises(_CustomError, compat.wait)

    # ------------------------------------------------------------------
    # (3) wait() is still called in a try/finally workflow when the
    #     primary-side operation fails
    #
    # This mirrors the real driver pattern:
    #
    #   thread = self.spawn(secondary_op, ...)
    #   try:
    #       primary_op(...)          # may raise
    #   finally:
    #       thread.wait()            # must always run
    # ------------------------------------------------------------------
    def test_wait_called_in_finally_when_primary_raises(self):
        """Secondary thread is always joined even when primary op fails.

        Simulates:
            thread = spawn(secondary_op)
            try:
                primary_op()     # raises VolumeDriverException
            finally:
                thread.wait()    # must be reached
        """
        secondary_sentinel = object()
        compat = _GreenThreadCompat(
            _make_resolved_future(secondary_sentinel))

        wait_result = None
        primary_exception = exception.VolumeDriverException(
            'primary side failed')

        with self.assertRaises(exception.VolumeDriverException) as ctx:
            try:
                raise primary_exception  # simulate primary-side failure
            finally:
                wait_result = compat.wait()

        # The primary exception propagates out of the with-block
        self.assertIs(primary_exception, ctx.exception)
        # …but wait() was still called and returned the secondary result
        self.assertIs(secondary_sentinel, wait_result)

    def test_wait_called_in_finally_when_primary_raises_and_secondary_fails(
            self):
        """Secondary exception is suppressed by primary exception in finally.

        When both sides fail, Python's try/finally semantics suppress the
        exception raised inside the ``finally`` block and propagate the
        original exception.  This test verifies that wait() is indeed
        called (the secondary future is consumed) even when it itself would
        raise — and that the primary exception still propagates.
        """
        primary_exception = exception.VolumeDriverException(
            'primary side failed')
        secondary_exception = ValueError('secondary side also failed')

        compat = _GreenThreadCompat(_make_failed_future(secondary_exception))

        with self.assertRaises(exception.VolumeDriverException) as ctx:
            try:
                raise primary_exception
            finally:
                # wait() raises secondary_exception here, which Python
                # suppresses in favour of the primary_exception already
                # in flight.
                try:
                    compat.wait()
                except Exception:
                    pass  # secondary error noted; primary still propagates

        self.assertIs(primary_exception, ctx.exception)

    def test_wait_result_used_after_successful_primary_op(self):
        """Normal (no-exception) path: wait() result is returned to caller."""
        with futurist.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(lambda: 'svol-ldev-123')
            compat = _GreenThreadCompat(future)

            primary_result = 'pvol-ldev-456'  # primary op succeeded
            try:
                # primary op (no exception)
                _ = primary_result
            finally:
                secondary_result = compat.wait()

        self.assertEqual('svol-ldev-123', secondary_result)


GROUP_REPL_COPY_GROUP = 'HBSDCG0001'
GROUP_REPL_GROUP_ID = '20000000-0000-0000-0000-000000000000'
GROUP_REPL_VOLUME_ID = '00000000-0000-0000-0000-000000000000'


class FakeGroup(object):
    """The subset of a Group object the helpers under test touch."""

    def __init__(self, group_type_id=None, group_id=GROUP_REPL_GROUP_ID,
                 name=None, volumes=None):
        self.id = group_id
        self.group_type_id = group_type_id
        self.name = name
        self.volumes = volumes


class FakeVolume(dict):
    """A volume that answers both attribute and mapping access."""

    def __init__(self, metadata=None, group_id=None, group=None,
                 provider_location=None, volume_id=GROUP_REPL_VOLUME_ID):
        super(FakeVolume, self).__init__()
        self.id = volume_id
        self.group_id = group_id
        self._group = group
        self.metadata = metadata
        if provider_location is not None:
            self['provider_location'] = provider_location

    @property
    def group(self):
        if isinstance(self._group, Exception):
            raise self._group
        return self._group


@ddt.ddt
class HBSDGroupReplicationHelperTest(test.TestCase):
    """Unit tests for the module level helpers of hbsd_replication."""

    @ddt.data(
        # (secondary_backend_id, expected backend, expected mode)
        (None, None, None),
        ('', '', None),
        ('remote-backend', 'remote-backend', None),
        ('remote-backend:graceful', 'remote-backend', 'graceful'),
        ('remote-backend:emergency', 'remote-backend', 'emergency'),
        ('remote-backend:GRACEFUL', 'remote-backend', 'graceful'),
        ('remote-backend:Emergency', 'remote-backend', 'emergency'),
        ('remote-backend:bogus', 'remote-backend:bogus', None),
        ('host:5000', 'host:5000', None),
    )
    @ddt.unpack
    def test_parse_failover_target(self, given, backend_id, mode):
        self.assertEqual(
            (backend_id, mode),
            hbsd_replication._parse_failover_target(given))

    def test_parse_failover_target_mode_only(self):
        self.assertEqual(
            (None, 'graceful'),
            hbsd_replication._parse_failover_target(':graceful'))

    def test_parse_failover_target_failback_sentinel(self):
        self.assertEqual(
            (hbsd_replication._REP_FAILBACK, None),
            hbsd_replication._parse_failover_target(
                hbsd_replication._REP_FAILBACK))

    def test_parse_failover_target_failback_with_mode(self):
        sentinel = hbsd_replication._REP_FAILBACK
        self.assertEqual(
            (sentinel, 'graceful'),
            hbsd_replication._parse_failover_target(sentinel + ':graceful'))

    def test_failover_mode_request_wins(self):
        group = FakeGroup(group_type_id='type-id')
        with mock.patch.object(hbsd_replication, 'group_types') as types:
            types.get_group_type_specs.return_value = 'graceful'
            self.assertEqual(
                'emergency',
                hbsd_replication._failover_mode(group, 'emergency'))
        types.get_group_type_specs.assert_not_called()

    def test_failover_mode_defaults_to_emergency(self):
        self.assertEqual(
            hbsd_replication._MODE_EMERGENCY,
            hbsd_replication._failover_mode(None, None))
        self.assertEqual(
            hbsd_replication._MODE_EMERGENCY,
            hbsd_replication._failover_mode(FakeGroup(), None))

    @ddt.data('graceful', '  GRACEFUL  ', 'Graceful')
    def test_failover_mode_from_group_type(self, spec):
        group = FakeGroup(group_type_id='type-id')
        with mock.patch.object(hbsd_replication, 'group_types') as types:
            types.get_group_type_specs.return_value = spec
            self.assertEqual(
                'graceful', hbsd_replication._failover_mode(group, None))
        types.get_group_type_specs.assert_called_once_with(
            'type-id', key=hbsd_replication._GROUP_REPL_MODE_SPEC)

    @ddt.data('emergency', 'bogus', '', None)
    def test_failover_mode_group_type_not_graceful(self, spec):
        group = FakeGroup(group_type_id='type-id')
        with mock.patch.object(hbsd_replication, 'group_types') as types:
            types.get_group_type_specs.return_value = spec
            self.assertEqual(
                hbsd_replication._MODE_EMERGENCY,
                hbsd_replication._failover_mode(group, None))

    def test_failover_mode_group_type_missing(self):
        group = FakeGroup(group_type_id='gone')
        with mock.patch.object(hbsd_replication, 'group_types') as types:
            types.get_group_type_specs.side_effect = (
                exception.GroupTypeNotFound(group_type_id='gone'))
            self.assertEqual(
                hbsd_replication._MODE_EMERGENCY,
                hbsd_replication._failover_mode(group, None))

    def test_has_group_repl_spec_removed(self):
        self.assertFalse(hasattr(hbsd_replication, '_has_group_repl_spec'))
        self.assertFalse(hasattr(hbsd_replication, '_GROUP_REPL_SPECS'))
        self.assertFalse(hasattr(hbsd_replication, '_is_group_replication'))
        self.assertFalse(
            hasattr(hbsd_replication, '_is_group_snapshot_replication'))

    def test_volume_in_group_replication_no_group(self):
        self.assertFalse(
            hbsd_replication._volume_in_group_replication(FakeVolume()))

    @ddt.data('group_replication_enabled',
              'consistent_group_replication_enabled')
    def test_volume_in_group_replication(self, key):
        group = fake_group.fake_group_obj(CTXT, group_type_id='type-id')
        volume = FakeVolume(group_id=GROUP_REPL_GROUP_ID, group=group)
        wanted = key

        def _specs(group_type_id, key=None):
            return '<is> True' if key == wanted else '<is> False'

        with mock.patch.object(group_types, 'get_group_type_specs') as specs:
            specs.side_effect = _specs
            self.assertTrue(
                hbsd_replication._volume_in_group_replication(volume))

    def test_volume_in_group_replication_not_replicated(self):
        group = fake_group.fake_group_obj(CTXT, group_type_id='type-id')
        volume = FakeVolume(group_id=GROUP_REPL_GROUP_ID, group=group)
        with mock.patch.object(
                group_types, 'get_group_type_specs', return_value=False):
            self.assertFalse(
                hbsd_replication._volume_in_group_replication(volume))

    def test_volume_in_group_replication_type_missing(self):
        # Deliberately not swallowed, matching Group.is_replicated.
        group = fake_group.fake_group_obj(CTXT, group_type_id='gone')
        volume = FakeVolume(group_id=GROUP_REPL_GROUP_ID, group=group)
        with mock.patch.object(
                group_types, 'get_group_type_specs',
                side_effect=exception.GroupTypeNotFound(
                    group_type_id='gone')):
            self.assertRaises(
                exception.GroupTypeNotFound,
                hbsd_replication._volume_in_group_replication, volume)

    def test_volume_in_group_replication_group_gone(self):
        volume = FakeVolume(
            group_id=GROUP_REPL_GROUP_ID,
            group=exception.GroupNotFound(group_id=GROUP_REPL_GROUP_ID))
        self.assertFalse(
            hbsd_replication._volume_in_group_replication(volume))

    def test_group_snapshot_is_replicated_none(self):
        self.assertFalse(
            hbsd_replication._group_snapshot_is_replicated(None))

    def test_group_snapshot_is_replicated_type_id_none(self):
        group_snapshot = FakeGroup(group_type_id=None)
        self.assertFalse(
            hbsd_replication._group_snapshot_is_replicated(group_snapshot))

    @ddt.data('group_replication_enabled',
              'consistent_group_replication_enabled')
    def test_group_snapshot_is_replicated_true(self, key):
        group_snapshot = FakeGroup(group_type_id='type-id')
        wanted = key

        def _specs(group_type_id, key=None):
            return '<is> True' if key == wanted else '<is> False'

        with mock.patch.object(group_types, 'get_group_type_specs') as specs:
            specs.side_effect = _specs
            self.assertTrue(
                hbsd_replication._group_snapshot_is_replicated(
                    group_snapshot))

    @ddt.data('<is> False', 'True', '', None, '<is>True')
    def test_group_snapshot_is_replicated_false(self, spec):
        group_snapshot = FakeGroup(group_type_id='type-id')
        with mock.patch.object(
                group_types, 'get_group_type_specs', return_value=spec):
            self.assertFalse(
                hbsd_replication._group_snapshot_is_replicated(
                    group_snapshot))

    def test_typed_for_group_replication_reads_the_unscoped_key(self):
        self.assertEqual('group_replication_enabled',
                         hbsd_replication._GROUP_REPL_VOLUME_SPEC)
        self.assertTrue(
            hbsd_replication._typed_for_group_replication(
                {'group_replication_enabled': '<is> True'}))

    def test_typed_for_group_replication_ignores_the_old_scoped_key(self):
        old_key = 'hbsd:' + hbsd_replication._GROUP_REPL_VOLUME_SPEC
        self.assertFalse(
            hbsd_replication._typed_for_group_replication(
                {old_key: '<is> True'}))

    @ddt.data('<is> True', '  <is> True  ')
    def test_typed_for_group_replication_true(self, spec):
        self.assertTrue(
            hbsd_replication._typed_for_group_replication(
                {hbsd_replication._GROUP_REPL_VOLUME_SPEC: spec}))

    @ddt.data('True', 'true', '<is> true')
    def test_typed_for_group_replication_false_for_near_miss_values(
            self, spec):
        self.assertFalse(
            hbsd_replication._typed_for_group_replication(
                {hbsd_replication._GROUP_REPL_VOLUME_SPEC: spec}))

    @ddt.data({hbsd_replication._GROUP_REPL_VOLUME_SPEC: '<is> False'},
              {hbsd_replication._GROUP_REPL_VOLUME_SPEC: ''},
              {'other': 'x'}, {}, None)
    def test_typed_for_group_replication_false(self, extra_specs):
        self.assertFalse(
            hbsd_replication._typed_for_group_replication(extra_specs))

    def test_group_snapshot_is_replicated_type_missing(self):
        group_snapshot = FakeGroup(group_type_id='gone')
        with mock.patch.object(
                group_types, 'get_group_type_specs',
                side_effect=exception.GroupTypeNotFound(
                    group_type_id='gone')):
            self.assertRaises(
                exception.GroupTypeNotFound,
                hbsd_replication._group_snapshot_is_replicated,
                group_snapshot)

    @ddt.data(None, {}, {'other': 'x'}, {hbsd_replication._MD_COPY_GROUP: ''})
    def test_volume_copy_group_binding_absent(self, metadata):
        self.assertIsNone(
            hbsd_replication._volume_copy_group_binding(
                FakeVolume(metadata=metadata)))

    def test_volume_copy_group_binding(self):
        volume = FakeVolume(
            metadata={hbsd_replication._MD_COPY_GROUP: GROUP_REPL_COPY_GROUP})
        self.assertEqual(
            GROUP_REPL_COPY_GROUP,
            hbsd_replication._volume_copy_group_binding(volume))

    def test_volume_copy_group_binding_unreadable(self):
        volume = mock.Mock()
        type(volume).metadata = mock.PropertyMock(side_effect=Exception('x'))
        self.assertIsNone(
            hbsd_replication._volume_copy_group_binding(volume))

    def test_volume_in_group_replication_or_bound_by_binding(self):
        volume = FakeVolume(
            metadata={hbsd_replication._MD_COPY_GROUP: GROUP_REPL_COPY_GROUP})
        self.assertTrue(
            hbsd_replication._volume_in_group_replication_or_bound(volume))

    def test_volume_in_group_replication_or_bound_neither(self):
        self.assertFalse(
            hbsd_replication._volume_in_group_replication_or_bound(
                FakeVolume(metadata={})))

    def test_metadata_model_update_merges(self):
        volume = FakeVolume(metadata={'keep': 'me'})
        self.assertEqual(
            {'metadata': {'keep': 'me', 'added': '5'}},
            hbsd_replication._metadata_model_update(volume, added=5))

    def test_metadata_model_update_stringifies(self):
        volume = FakeVolume(metadata={})
        update = hbsd_replication._metadata_model_update(volume, ldev=11)
        self.assertEqual({'ldev': '11'}, update['metadata'])

    def test_metadata_model_update_removes_on_none(self):
        volume = FakeVolume(metadata={'drop': '1', 'keep': '2'})
        self.assertEqual(
            {'metadata': {'keep': '2'}},
            hbsd_replication._metadata_model_update(volume, drop=None))

    def test_metadata_model_update_removes_missing_key(self):
        volume = FakeVolume(metadata={'keep': '2'})
        self.assertEqual(
            {'metadata': {'keep': '2'}},
            hbsd_replication._metadata_model_update(volume, absent=None))

    def test_metadata_model_update_does_not_mutate_volume(self):
        original = {'keep': 'me'}
        volume = FakeVolume(metadata=original)
        hbsd_replication._metadata_model_update(volume, added=1)
        self.assertEqual({'keep': 'me'}, original)

    def test_metadata_model_update_unreadable(self):
        volume = mock.Mock()
        type(volume).metadata = mock.PropertyMock(side_effect=Exception('x'))
        self.assertEqual(
            {}, hbsd_replication._metadata_model_update(volume, added=1))

    @ddt.data(
        None,
        {},
        {'provider_location': None},
        {'provider_location': ''},
        {'provider_location': '1'},
        {'provider_location': json.dumps({'pldev': 1})},
    )
    def test_svol_of_absent(self, obj):
        self.assertIsNone(hbsd_replication._svol_of(obj))

    def test_svol_of(self):
        obj = {'provider_location': json.dumps({'pldev': 1, 'sldev': 2})}
        self.assertEqual(2, hbsd_replication._svol_of(obj))

    def test_svol_of_is_an_int(self):
        obj = {'provider_location': json.dumps({'sldev': '7'})}
        self.assertEqual(7, hbsd_replication._svol_of(obj))

    def test_svol_of_ignores_instance_role(self):
        obj = {'provider_location': json.dumps({'pldev': 9, 'sldev': 4})}
        self.assertEqual(4, hbsd_replication._svol_of(obj))

    def test_log_step_success(self):
        with mock.patch.object(hbsd_replication, 'LOG') as log:
            with hbsd_replication._log_step(
                    'doing it', copy_group=GROUP_REPL_COPY_GROUP):
                pass
        self.assertEqual(2, log.info.call_count)
        self.assertEqual(0, log.warning.call_count)

    def test_log_step_reraises_and_warns(self):
        with mock.patch.object(hbsd_replication, 'LOG') as log:
            self.assertRaises(
                ValueError, self._raise_in_log_step)
        self.assertEqual(1, log.info.call_count)
        self.assertEqual(1, log.warning.call_count)

    def _raise_in_log_step(self):
        with hbsd_replication._log_step(
                'doing it', copy_group=GROUP_REPL_COPY_GROUP):
            raise ValueError('boom')


class HBSDGroupReplicationRestApiTest(test.TestCase):
    """Unit tests for the REST calls added for group replication."""

    def setUp(self):
        super(HBSDGroupReplicationRestApiTest, self).setUp()
        self.client = mock.Mock()
        self.client.object_url = 'https://storage/v1/objects/storages/123456'
        self.client.storage_id = '123456'
        self.client._remote_copygroup_id = pytypes.MethodType(
            hbsd_rest_api.RestApiClient._remote_copygroup_id, self.client)

    def _call(self, func, *args, **kwargs):
        return func(self.client, *args, **kwargs)

    def _secondary_id(self, side='S'):
        return 'NotSpecified,%s,%s%s,NotSpecified' % (
            GROUP_REPL_COPY_GROUP, GROUP_REPL_COPY_GROUP, side)

    def test_takeover_remote_copy_grp(self):
        self._call(
            hbsd_rest_api.RestApiClient.takeover_remote_copy_grp,
            None, GROUP_REPL_COPY_GROUP)
        self.client._invoke.assert_called_once()
        args, kwargs = self.client._invoke.call_args
        self.assertEqual(
            {'parameters': {'mode': 'forceSplit'}}, kwargs['body'])
        self.assertTrue(kwargs['job_nowait'])
        url = args[0]
        self.assertTrue(url.endswith('/actions/takeover/invoke'))
        self.assertIn('/remote-mirror-copygroups/', url)
        self.assertIn(self._secondary_id(), url)

    def test_get_remote_copy_grp_without_remote_client(self):
        self.client._get_object.return_value = {
            'copyGroupName': GROUP_REPL_COPY_GROUP}
        ret = self._call(
            hbsd_rest_api.RestApiClient.get_remote_copy_grp,
            None, GROUP_REPL_COPY_GROUP, is_secondary=True)
        self.assertEqual({'copyGroupName': GROUP_REPL_COPY_GROUP}, ret)
        self.client._get_object.assert_called_once()
        args, kwargs = self.client._get_object.call_args
        self.assertNotIn('remote_auth', kwargs)
        self.assertIn(self._secondary_id(), args[0])

    def test_get_remote_copy_grp_secondary_flag_flips_device_groups(self):
        self.client._get_object.return_value = {}
        self._call(
            hbsd_rest_api.RestApiClient.get_remote_copy_grp,
            None, GROUP_REPL_COPY_GROUP, is_secondary=False)
        args, _ = self.client._get_object.call_args
        self.assertIn(self._secondary_id(side='P'), args[0])

    def test_remote_storage_not_registered_is_not_retried(self):
        self.assertIn(
            hbsd_rest_api.MSGID_REMOTE_STORAGE_NOT_REGISTERED,
            hbsd_rest_api._REST_NO_RETRY_MESSAGEIDS)

    def test_response_data_reports_detail_code(self):
        data = hbsd_rest_api.ResponseData(FakeResponse(500, {
            'errorSource': 'src',
            'messageId': 'KART40152-E',
            'message': 'msg',
            'cause': 'cause',
            'solution': 'solution',
            'errorCode': {},
            'detailCode': '2E23-5000',
        }))
        self.assertEqual('2E23-5000', data.get_errobj()['detailCode'])

    def test_response_data_detail_code_defaults_to_empty(self):
        data = hbsd_rest_api.ResponseData(FakeResponse(500, {
            'errorSource': 'src',
            'messageId': 'KART30013-E',
        }))
        self.assertEqual('', data.get_errobj()['detailCode'])
        hbsd_utils.HBSDMsg.REST_API_FAILED.value['msg'] % dict(
            data.get_errobj(), method='GET', url='u', params=None, body=None)
