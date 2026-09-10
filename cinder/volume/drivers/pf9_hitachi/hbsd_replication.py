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
"""replication module for Hitachi HBSD Driver."""

from collections import defaultdict
import contextlib
import json
import time

from eventlet import greenthread
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import timeutils

from cinder import exception
from cinder.objects import fields
from cinder.objects import volume as cinder_volume
# PF9 Start
from cinder.volume.drivers.pf9_hitachi import hbsd_common as common
from cinder.volume.drivers.pf9_hitachi import hbsd_rest as rest
from cinder.volume.drivers.pf9_hitachi import hbsd_utils as utils
# PF9 End
from cinder.volume import group_types
from cinder.volume import manager
from cinder.zonemanager import utils as fczm_utils

_ASYNC_STRING = 'async'

# Volume metadata keys the driver stamps for a DR orchestrator.
#
# Cinder's volume API returns user metadata (GET /v3/volumes/detail) but
# never provider_location, and this driver sets no provider_id, so metadata
# is the only channel that carries the array-side identifiers out to a
# client. Without it an orchestrator has to open its own Configuration
# Manager session purely to learn which LDEVs back a volume.
_MD_PVOL = 'hbsd_pvol_id'
_MD_SVOL = 'hbsd_svol_id'
_MD_COPY_GROUP = 'hbsd_copy_group'

# Optional operator override for the same binding, on the Cinder group's
# name. Weaker than the metadata above -- a group can be renamed through
# PUT /groups/{id} with no validation and the Group object has no metadata
# field to record the resolved binding against -- so it is the fallback,
# never the primary channel.
_GROUP_NAME_BINDING_PREFIX = 'hbsd-cg:'

# Which of the backend's two storage systems holds the replication S-VOLs.
# 'source' is the site that created the copy group: its S-VOLs live on the
# replication_device. 'target' is a recovery site adopting promoted S-VOLs,
# where they are this backend's own LDEVs and the peer may be gone.
_ROLE_SOURCE = 'source'
_ROLE_TARGET = 'target'

# Pool capability keys carrying remote replication pair state. Reported as
# scalars -- the pair map is a JSON string -- because the scheduler holds
# capabilities as a read-only Mapping and a scalar is the only shape
# guaranteed to survive that and the API hop unaltered.
_PAIR_STATUS_KEY = 'group_replication_pairs'
_PAIR_STATUS_UPDATED_KEY = 'group_replication_pairs_updated_at'
_PAIR_STATUS_PEER_KEY = 'group_replication_peer_initialized'
# False when the copy groups could not be listed from the storage system
# and the report was built from names this process had already seen. It
# tells a client whether an empty map means "nothing is replicated" or
# "we could not ask".
_PAIR_STATUS_ENUMERATED_KEY = 'group_replication_pairs_enumerated'

# Upper bound on copy groups inspected per statistics cycle. Each one costs
# a Configuration Manager request, and the cycle repeats every
# backend_stats_polling_interval seconds (60 by default).
_PAIR_STATUS_MAX_COPY_GROUPS = 64

# Group type extra specs that opt a group in to Cinder group replication.
#
# Cinder's Group.is_replicated accepts EITHER key, and the API gate for the
# replication group actions is that property. Keying on only one of them let
# a group typed with the other pass the API check and reach
# enable_replication -- which has no _is_group_replication guard, so the copy
# group really was created -- while manage_existing, unmanage, update_group
# and create_group_snapshot all evaluated their guard as False and fell into
# the upstream blocking path. The group was then replicated but unmanageable.
_GROUP_REPL_SPECS = ('consistent_group_replication_enabled',
                     'group_replication_enabled')

# How failover_replication splits the copy group.
#
# Cinder hands the action two parameters and consumes one of them itself:
# allow_attached_volume never reaches a driver (the volume manager uses it
# as a precondition and drops it), and the request schema rejects anything
# else. secondary_backend_id is therefore the only per-request channel a
# client has, so it carries an optional '<backend_id>:<mode>' suffix --
# group/api.py passes the value through unvalidated, and the manager only
# ever compares it to the failback sentinel. A group type can set a default
# for every request on that group instead.
_GROUP_REPL_MODE_SPEC = 'hbsd:group_replication_failover_mode'
_MODE_GRACEFUL = 'graceful'
_MODE_EMERGENCY = 'emergency'
_MODE_SUFFIX_SEP = ':'

_REP_STATUS_CHECK_SHORT_INTERVAL = 5
_REP_STATUS_CHECK_LONG_INTERVAL = 10 * 60
_REP_STATUS_CHECK_TIMEOUT = 24 * 60 * 60

_PRIMARY = 1
_SECONDARY = 2
_PRIMARY_SECONDARY = 3

_WAIT_PAIR = 1
_WAIT_PSUS = 2
_WAIT_SSWS = 3
_WAIT_SPLIT = 4

_REP_FAILBACK = manager.VolumeManager.FAILBACK_SENTINEL

_JOURNAL_VOLUME_LABEL = '%s-JNL'

# The longest copy group name whose journal label still fits MAX_LDEV_LABEL.
# create_journals() labels every journal LDEV '<copy group name>-JNL', so the
# name is bounded by the LABEL limit, not just _MAX_COPY_GROUP_NAME.
_MAX_GROUP_COPY_GROUP_NAME = min(
    rest._MAX_COPY_GROUP_NAME,
    rest.MAX_LDEV_LABEL - len(_JOURNAL_VOLUME_LABEL % ''))

_MIRROR_IDENTIFIER = 'G'
_ASYNC_IDENTIFIER = 'U'

_INHERITED_REP_VOLUME_OPTS = [
    'replication_device',
]

_REP_OPTS = [
    cfg.IntOpt(
        'hitachi_replication_status_check_short_interval',
        default=_REP_STATUS_CHECK_SHORT_INTERVAL,
        help='Initial interval at which remote replication pair status is '
        'checked'),
    cfg.IntOpt(
        'hitachi_replication_status_check_long_interval',
        default=_REP_STATUS_CHECK_LONG_INTERVAL,
        help='Interval at which remote replication pair status is checked. '
        'This parameter is applied if the status has not changed to the '
        'expected status after the time indicated by this parameter has '
        'elapsed.'),
    cfg.IntOpt(
        'hitachi_replication_status_check_timeout',
        default=_REP_STATUS_CHECK_TIMEOUT,
        help='Maximum wait time before the remote replication pair status '
        'changes to the expected status'),
    cfg.IntOpt(
        'hitachi_path_group_id',
        default=0, min=0, max=255,
        help='Path group ID assigned to the remote connection for remote '
        'replication'),
    cfg.IntOpt(
        'hitachi_quorum_disk_id',
        min=0, max=31,
        help='ID of the Quorum disk used for global-active device'),
    cfg.IntOpt(
        'hitachi_replication_copy_speed',
        min=1, max=15, default=3,
        help='Remote copy speed of storage system. 1 or 2 indicates '
             'low speed, 3 indicates middle speed, and a value between 4 and '
             '15 indicates high speed.'),
    cfg.BoolOpt(
        'hitachi_set_mirror_reserve_attribute',
        default=True,
        help='Whether or not to set the mirror reserve attribute'),
    cfg.IntOpt(
        'hitachi_replication_number',
        default=0, min=0, max=255,
        help='Instance number for REST API'),
]

COMMON_REPLICATION_OPTS = [
    cfg.StrOpt(
        'hitachi_replication_role',
        default='source',
        choices=['source', 'target'],
        help='This backend\'s role in remote replication. Use "source" (the '
             'default, and the only value that changes nothing) where '
             'volumes are created and the copy group is made: the '
             'replication secondary volumes then live on the storage system '
             'named by replication_device. Use "target" at a disaster '
             'recovery site whose backend adopts promoted secondary '
             'volumes, where those volumes are this backend\'s own and the '
             'peer storage system may be unreachable. The role cannot be '
             'derived at run time, because the group replication actions '
             'reach whichever backend owns the group object and a recovery '
             'site owns nothing until it adopts.'),
    cfg.BoolOpt(
        'hitachi_replication_report_pair_status',
        default=True,
        help='Whether or not to report remote replication pair state and '
             'consistency time for each copy group in the pool capabilities. '
             'Enabling this lets a client read pair state and replication '
             'lag through the Block Storage scheduler-stats API instead of '
             'querying the storage system directly, at the cost of one '
             'Configuration Manager request per copy group on every '
             'statistics cycle.'),
    cfg.IntOpt(
        'hitachi_replication_mun',
        default=1, min=0, max=3,
        help='Mirror unit ID used for asynchronous remote replication'),
    cfg.IntOpt(
        'hitachi_replication_journal_size',
        default=None, min=10, max=1024,
        help='Size in gigabytes of the journal used for asynchronous remote '
        'replication'),
    cfg.IntOpt(
        'hitachi_replication_journal_overflow_tolerance',
        default=60, min=0, max=600,
        help='Delay in seconds before a volume pair is split after the data '
        'in a journal volume reaches its maximum'),
    cfg.BoolOpt(
        'hitachi_replication_journal_use_cache',
        default=True,
        help='Whether or not to cache restore journal data in asynchronous '
        'remote replication'),
    cfg.StrOpt(
        'hitachi_replication_journal_transfer_speed',
        default='256',
        choices=['3', '10', '100', '256'],
        help='Site-to-site journal data transfer speed in megabits per second '
        'in asynchronous remote replication'),
    cfg.StrOpt(
        'hitachi_replication_journal_creation_speed',
        default='L',
        choices=['L', 'M', 'H'],
        help='Journal data creation speed for initial copy in asynchronous '
        'remote replication'),
    cfg.IntOpt(
        'hitachi_replication_journal_path_failure_tolerance',
        default=5, min=0, max=60,
        help='Delay in minutes before a volume pair is split after path '
        'failure occurs'),
]

_REPLICATION_DEVICE_KEY_NAMES = [
    'storage_id',
    'pool',
    'snap_pool',
    'ldev_range',
    'target_ports',
    'compute_target_ports',
    'pair_target_number',
    'rest_pair_target_ports',
]

_REPLICATION_DEVICE_ISCSI_KEY_NAMES = [
    'use_chap_auth',
    'chap_username',
    'chap_password',
]

_REPLICATION_DEVICE_STANDARD_KEY_NAMES = [
    'driver_ssl_cert_verify',
    'driver_ssl_cert_path',
    'san_login',
    'san_password',
    'san_ip',
    'san_api_port',
]

COMMON_MIRROR_OPTS = [
    cfg.StrOpt(
        'hitachi_mirror_storage_id',
        default=None,
        help='ID of secondary storage system'),
    cfg.StrOpt(
        'hitachi_mirror_pool',
        default=None,
        help='Pool of secondary storage system'),
    cfg.StrOpt(
        'hitachi_mirror_snap_pool',
        default=None,
        help='Thin pool of secondary storage system'),
    cfg.StrOpt(
        'hitachi_mirror_ldev_range',
        default=None,
        help='Logical device range of secondary storage system'),
    cfg.ListOpt(
        'hitachi_mirror_target_ports',
        default=[],
        help='Target port names for host group or iSCSI target'),
    cfg.ListOpt(
        'hitachi_mirror_compute_target_ports',
        default=[],
        help=(
            'Target port names of compute node '
            'for host group or iSCSI target')),
    cfg.IntOpt(
        'hitachi_mirror_pair_target_number',
        min=0, max=99, default=0,
        help='Pair target name of the host group or iSCSI target'),
]

ISCSI_MIRROR_OPTS = [
    cfg.BoolOpt(
        'hitachi_mirror_use_chap_auth',
        default=False,
        help='Whether or not to use iSCSI authentication'),
    cfg.StrOpt(
        'hitachi_mirror_auth_user',
        default=None,
        help='iSCSI authentication username'),
    cfg.StrOpt(
        'hitachi_mirror_auth_password',
        default=None,
        secret=True,
        help='iSCSI authentication password'),
]

REST_MIRROR_OPTS = [
    cfg.ListOpt(
        'hitachi_mirror_rest_pair_target_ports',
        default=[],
        help='Target port names for pair of the host group or iSCSI target'),
]

REST_MIRROR_API_OPTS = [
    cfg.StrOpt(
        'hitachi_mirror_rest_user',
        default=None,
        help='Username of secondary storage system for REST API'),
    cfg.StrOpt(
        'hitachi_mirror_rest_password',
        default=None,
        secret=True,
        help='Password of secondary storage system for REST API'),
    cfg.StrOpt(
        'hitachi_mirror_rest_api_ip',
        default=None,
        help='IP address of REST API server'),
    cfg.PortOpt(
        'hitachi_mirror_rest_api_port',
        default=443,
        help='Port number of REST API server'),
]

REST_MIRROR_SSL_OPTS = [
    cfg.BoolOpt('hitachi_mirror_ssl_cert_verify',
                default=False,
                help='If set to True the http client will validate the SSL '
                     'certificate of the backend endpoint.'),
    cfg.StrOpt('hitachi_mirror_ssl_cert_path',
               help='Can be used to specify a non default path to a '
               'CA_BUNDLE file or directory with certificates of '
               'trusted CAs, which will be used to validate the backend'),
]

_MSGID_JOURNAL_ID_ALREADY_USED = 'KART40054-E'
_MSGID_NO_AVAILABLE_JOURNAL_ID = 'KART40046-E'
_MSGID_INSTANCE_CANNOT_OPERATED = 'KART40041-E'

_MAX_JID_COUNT_EXCEEDED = ('2E23', '5000')

CONF = cfg.CONF
CONF.register_opts(_REP_OPTS)
CONF.register_opts(COMMON_REPLICATION_OPTS)
CONF.register_opts(COMMON_MIRROR_OPTS)
CONF.register_opts(ISCSI_MIRROR_OPTS)
CONF.register_opts(REST_MIRROR_OPTS)
CONF.register_opts(REST_MIRROR_API_OPTS)
CONF.register_opts(REST_MIRROR_SSL_OPTS)

LOG = logging.getLogger(__name__)

MSG = utils.HBSDMsg


@contextlib.contextmanager
def _log_step(step, **details):
    """Bracket a blocking group-replication step with start/end INFO lines.

    These paths issue CM jobs whose response timeout is 30 minutes, so a wedged
    array looks exactly like a busy one: no output at all between the request
    and the eventual failure. The elapsed time on the closing line is the part
    that tells them apart.
    """
    detail = ', '.join('%s: %s' % kv for kv in sorted(details.items()))
    LOG.info('Group replication: %(step)s started. (%(detail)s)',
             {'step': step, 'detail': detail})
    watch = timeutils.StopWatch()
    watch.start()
    try:
        yield
    except Exception:
        LOG.info(
            'Group replication: %(step)s failed after %(sec).1fs. '
            '(%(detail)s)',
            {'step': step, 'sec': watch.elapsed(), 'detail': detail})
        raise
    LOG.info(
        'Group replication: %(step)s finished in %(sec).1fs. (%(detail)s)',
        {'step': step, 'sec': watch.elapsed(), 'detail': detail})


def _has_group_repl_spec(group_type_id):
    """True if the group type opts in to Cinder group replication.

    Mirrors Group.is_replicated: either spec key counts, compared the same
    way volume_utils.is_group_a_type() compares it. A group type that can no
    longer be read is treated as not opting in, so the caller falls through
    to the path it used before this feature.
    """
    if group_type_id is None:
        return False
    for key in _GROUP_REPL_SPECS:
        try:
            spec = group_types.get_group_type_specs(group_type_id, key=key)
        except exception.GroupTypeNotFound:
            return False
        if spec == '<is> True':
            return True
    return False


def _volume_copy_group_binding(volume):
    """The array copy group a volume says it belongs to, or None.

    Written by _group_repl_add_volume at the site that creates the pair,
    and supplied on the manage_existing call that adopts a promoted S-VOL
    elsewhere -- the manage flow puts the requested metadata on the volume
    before the driver ever sees it, so the binding arrives with the adopt.
    """
    try:
        metadata = volume.metadata or {}
    except Exception:
        return None
    return metadata.get(_MD_COPY_GROUP) or None


def _volume_is_bound_to_copy_group(volume):
    """True if this volume carries a copy-group binding."""
    return _volume_copy_group_binding(volume) is not None


def _volume_in_group_replication_or_bound(volume):
    """True for a group-replication member or an adopted S-VOL.

    An adopted S-VOL cannot be a group member yet: POST /manageable_volumes
    has no group field, so the volume exists with group_id None until a
    later PUT /groups/{id} adds it. Keying only on membership sent every
    such adopt down the upstream path, which refuses a paired LDEV.
    """
    return (_volume_in_group_replication(volume) or
            _volume_is_bound_to_copy_group(volume))


def _parse_failover_target(secondary_backend_id):
    """Split a failover target into (backend_id, requested mode or None).

    Only a suffix that names a mode is consumed, so a backend_id
    containing a colon for any other reason is returned untouched, and so
    is the failback sentinel.
    """
    if not secondary_backend_id:
        return secondary_backend_id, None
    backend_id, sep, mode = secondary_backend_id.rpartition(_MODE_SUFFIX_SEP)
    if not sep or mode.lower() not in (_MODE_GRACEFUL, _MODE_EMERGENCY):
        return secondary_backend_id, None
    return backend_id or None, mode.lower()


def _failover_mode(group, requested_mode):
    """Resolve how failover_replication should split the copy group.

    The request wins, then the group type, then emergency -- which is both
    what this driver did before the mode was selectable and the only mode
    that can complete once the primary site is gone. Defaulting to
    graceful would quietly change what an existing caller gets, and would
    fail in exactly the situation failover exists for.
    """
    if requested_mode:
        return requested_mode
    if group is not None and group.group_type_id is not None:
        try:
            spec = group_types.get_group_type_specs(
                group.group_type_id, key=_GROUP_REPL_MODE_SPEC)
        except exception.GroupTypeNotFound:
            spec = None
        if spec and spec.strip().lower() == _MODE_GRACEFUL:
            return _MODE_GRACEFUL
    return _MODE_EMERGENCY


def _is_group_replication(group):
    """True if this group's type asks for Cinder group replication."""
    return group is not None and _has_group_repl_spec(group.group_type_id)


def _is_group_snapshot_replication(group_snapshot):
    """True for a group snapshot of a group-replication group.

    Keyed on the group snapshot's own group_type_id so that the group is
    never lazy-loaded from the database.
    """
    return group_snapshot is not None and _has_group_repl_spec(
        group_snapshot.group_type_id)


def _volume_in_group_replication(volume):
    """True if this volume is a member of a group-replication group."""
    if not volume.group_id:
        return False
    try:
        return _is_group_replication(volume.group)
    except exception.GroupNotFound:
        return False


def _check_rep_ldev(self, volume, operation):
    if (('group_id' in volume and volume.group_id) or
            ('consistencygroup_id' in volume and volume.consistencygroup_id)):
        group = (volume.group_id if 'group_id' in volume and
                 volume.group_id else volume.consistencygroup_id)
        msg = utils.output_log(
            MSG.REPLICATION_AND_GROUP_ERROR, operation=operation,
            volume=volume.id, group=group)
        self.raise_error(msg)


def _get_rep_type(self, extra_specs):
    replication_type = extra_specs.get('replication_type')
    if replication_type is not None:
        if len(replication_type.split()) == 2:
            replication_type = replication_type.split()[1]
        if replication_type == _ASYNC_STRING:
            return self.driver_info['rep_type_async']
        msg = utils.output_log(
            MSG.INVALID_EXTRA_SPEC_KEY, key='replication_type',
            value=replication_type)
        self.raise_error(msg)
    return self.driver_info['rep_type_async']


def _metadata_model_update(volume, **kwargs):
    """Return a {'metadata': ...} model update fragment, or {}.

    Both paths that persist a driver metadata update REPLACE user metadata
    rather than merging it -- Volume.save() for the replication group
    actions and manage_existing, db.volumes_update() for update_group -- so
    the whole map has to be sent every time.

    Which is why this emits nothing at all when the volume's current
    metadata cannot be read: sending only the driver's own keys would
    silently drop whatever the volume's owner had set. Reading it can go to
    the database if the attribute was never loaded, and neither that nor
    anything else here is worth failing a replication operation over. A
    None value removes that key.
    """
    try:
        merged = dict(volume.metadata or {})
    except Exception:
        LOG.debug('Not annotating volume %s: its current metadata could '
                  'not be read, and a partial map would discard the '
                  'metadata already on it.', volume.id, exc_info=True)
        return {}
    for key, value in kwargs.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = str(value)
    return {'metadata': merged}


def _pack_rep_provider_location(pldev=None, sldev=None, rep_type=None):
    provider_location = {}
    if pldev is not None:
        provider_location['pldev'] = pldev
    if sldev is not None:
        provider_location['sldev'] = sldev
    if rep_type is not None:
        provider_location['remote-copy'] = rep_type
    return json.dumps(provider_location)


def _get_unused_minimum_value(value_list):
    value = 0
    while True:
        if value not in value_list:
            return value
        value += 1


def _delays(short_interval, long_interval, timeout):
    start_time = timeutils.utcnow()
    watch = timeutils.StopWatch()
    i = 0
    while True:
        watch.restart()
        yield i
        if utils.timed_out(start_time, timeout):
            raise StopIteration()
        watch.stop()
        interval = long_interval if utils.timed_out(
            start_time, long_interval) else short_interval
        idle = max(interval - watch.elapsed(), 0)
        time.sleep(idle)
        i += 1


def _get_ldev_site(obj):
    if not obj:
        return None
    provider_location = obj.get('provider_location')
    if not provider_location:
        return None
    if provider_location.isdigit():
        return _PRIMARY
    if provider_location.startswith('{'):
        loc = json.loads(provider_location)
        if isinstance(loc, dict):
            if 'sldev' in loc and 'pldev' not in loc:
                return _SECONDARY
            if 'pldev' in loc and 'sldev' not in loc:
                return _PRIMARY
            if 'pldev' in loc and 'sldev' in loc:
                return _PRIMARY_SECONDARY
    return None


def _svol_of(obj):
    """The S-VOL id straight out of a packed provider_location, or None.

    Deliberately not instance.get_ldev(): that picks pldev or sldev from
    the instance's own is_primary/is_secondary flag, so an instance chosen
    for its client -- which is what the adopt and takeover paths do --
    would silently read the wrong key and return None.
    """
    if not obj:
        return None
    provider_location = obj.get('provider_location')
    if not provider_location or not provider_location.startswith('{'):
        return None
    loc = json.loads(provider_location)
    if not isinstance(loc, dict) or 'sldev' not in loc:
        return None
    return int(loc['sldev'])


def _get_failover_volume_update(volumes, failover_success_volumes):
    volume_updates = []
    for volume in volumes:
        volume_update = {'volume_id': volume.id}
        if volume in failover_success_volumes:
            volume_update['updates'] = {
                'replication_status': fields.ReplicationStatus.FAILED_OVER}
            for snapshot in volume.snapshots:
                if _get_ldev_site(snapshot) == _PRIMARY:
                    snapshot.status = fields.SnapshotStatus.ERROR
                    snapshot.save()
        else:
            volume_update['updates'] = {'status': 'error'}
            if volume.replication_status in (
                    fields.ReplicationStatus.ENABLED,
                    fields.ReplicationStatus.FAILOVER_ERROR):
                volume_update['updates']['replication_status'] = (
                    fields.ReplicationStatus.FAILOVER_ERROR)
        volume_updates.append(volume_update)
    return volume_updates


class HBSDREPLICATION(rest.HBSDREST):

    def __init__(self, conf, driverinfo, db, active_backend_id=None):
        super(HBSDREPLICATION, self).__init__(conf, driverinfo, db)
        conf.append_config_values(_REP_OPTS)
        conf.append_config_values(COMMON_REPLICATION_OPTS)
        if driverinfo['proto'] == 'iSCSI':
            conf.append_config_values(ISCSI_MIRROR_OPTS)
        conf.append_config_values(REST_MIRROR_OPTS)
        conf.append_config_values(REST_MIRROR_API_OPTS)
        conf.append_config_values(REST_MIRROR_SSL_OPTS)
        driver_impl_class = self.driver_info['driver_impl_class']
        self.primary = driver_impl_class(conf, driverinfo, db)
        self.rep_primary = self.primary
        self.rep_primary.is_primary = True
        self.rep_primary.storage_id = conf.safe_get(
            self.driver_info['param_prefix'] + '_storage_id') or ''
        self.primary_storage_id = self.rep_primary.storage_id
        self.secondary = driver_impl_class(conf, driverinfo, db)
        self.rep_secondary = self.secondary
        self.rep_secondary.is_secondary = True
        self.rep_secondary.storage_id = (
            conf.safe_get(
                self.driver_info['param_prefix'] + '_mirror_storage_id') or
            conf.safe_get('replication_device')[0].get('storage_id') or '')
        self.secondary_storage_id = self.rep_secondary.storage_id
        # The peer's replication_device label, held on self rather than on
        # rep_secondary: it comes from config, needs no session, and must
        # stay readable when do_setup() clears rep_secondary because the
        # peer was unreachable.
        self.rep_secondary_backend_id = None
        # Copy groups this process has seen. Once failed over the copy
        # groups cannot be listed -- that needs a session on the primary --
        # but each remembered name can still be read from the secondary.
        self._known_copy_groups = set()
        self._active_backend_id = active_backend_id
        self._LDEV_NAME = self.driver_info['driver_prefix'] + '-LDEV-%d-%d'

    @property
    def instances(self):
        """The two sites, read at call time rather than at __init__.

        do_setup() clears rep_secondary when the peer never answered, so a
        tuple captured in __init__ would keep handing out an instance with
        no session on it.
        """
        return self.rep_primary, self.rep_secondary

    def update_mirror_conf(self, conf, opts):
        for opt in opts:
            name = opt.name.replace('hitachi_mirror_', 'hitachi_')
            try:
                if opt.name == 'hitachi_mirror_pool':
                    if conf.safe_get('hitachi_mirror_pool'):
                        name = 'hitachi_pools'
                        value = [getattr(conf, opt.name)]
                    else:
                        raise ValueError()
                else:
                    value = getattr(conf, opt.name)
                setattr(conf, name, value)
            except Exception:
                with excutils.save_and_reraise_exception():
                    self.rep_secondary.output_log(
                        MSG.INVALID_PARAMETER, param=opt.name)

    def _replace_with_mirror_conf(self):
        conf = self.conf
        new_conf = utils.Config(conf, {})
        self.rep_secondary.conf = new_conf
        self.update_mirror_conf(new_conf, COMMON_MIRROR_OPTS)
        self.update_mirror_conf(new_conf, REST_MIRROR_OPTS)
        if self.rep_secondary.driver_info['volume_type'] == 'iscsi':
            self.update_mirror_conf(new_conf, ISCSI_MIRROR_OPTS)
        new_conf.san_login = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_rest_user'))
        new_conf.san_password = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_rest_password'))
        new_conf.san_ip = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_rest_api_ip'))
        new_conf.san_api_port = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_rest_api_port'))
        new_conf.driver_ssl_cert_verify = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_ssl_cert_verify'))
        new_conf.driver_ssl_cert_path = (
            conf.safe_get(self.driver_info['param_prefix'] +
                          '_mirror_ssl_cert_path'))

    def do_setup(self, context):
        """Prepare for the startup of the driver."""
        if self.conf.hitachi_mirror_storage_id:
            self.rep_primary = self.primary
            self.rep_secondary = self.secondary
            self.ctxt = context
            try:
                self.rep_primary.check_opts(
                    self.rep_primary.conf, _REP_OPTS)
                self.rep_primary.do_setup(context)
                self.client = self.rep_primary.client
            except Exception:
                self.rep_primary.output_log(
                    MSG.SITE_INITIALIZATION_FAILED, site='primary')
                self.rep_primary = None
            try:
                self.rep_secondary.check_opts(
                    self.rep_secondary.conf, _REP_OPTS)
                self._replace_with_mirror_conf()
                self.rep_secondary.do_setup(context)
            except Exception:
                self.rep_secondary.output_log(
                    MSG.SITE_INITIALIZATION_FAILED, site='secondary')
                if not self.rep_primary:
                    raise
                self.rep_secondary = None
        else:
            self.ctxt = context
            self._check_param()
            self._setup_replication()
            if self._active_backend_id:
                # Failed over: the secondary IS the active side, so it has
                # to initialize -- there is nothing else to serve from.
                self.rep_secondary.do_setup(context)
                return
            self.rep_primary.do_setup(context)
            # The peer is best effort, the same way the mirror path above
            # treats its two sites. A site whose partner has gone dark is
            # precisely the disaster case, and it must still bring its own
            # backend up: an exception here leaves driver.initialized False,
            # which stops the volume service heart-beating
            # (VolumeManager.is_working), drops the backend out of the
            # scheduler, and makes require_driver_initialized() refuse even
            # the local-only work recovery depends on -- manage_existing
            # above all. Degraded beats absent.
            try:
                self.rep_secondary.do_setup(context)
            except Exception:
                self.rep_secondary.output_log(
                    MSG.SITE_INITIALIZATION_FAILED, site='secondary')
                self.rep_secondary = None

    def _check_param(self):
        """Check parameter values and consistency among them."""
        self.rep_primary.check_opt_value(
            self.rep_primary.conf, _INHERITED_REP_VOLUME_OPTS)
        self.rep_primary.check_opts(
            self.rep_primary.conf, _REP_OPTS)
        self.rep_primary.check_opts(
            self.rep_primary.conf, COMMON_REPLICATION_OPTS)
        if not self.conf.safe_get(
                self.driver_info['param_prefix'] +
                '_replication_journal_size'):
            msg = utils.output_log(
                MSG.INVALID_PARAMETER,
                param='hitachi_replication_journal_size')
            self.raise_error(msg)

    def _create_rep_conf(self, conf):
        opts = {}
        opt_list = self.driver_info['driver_class'].get_driver_options()
        for opt in opt_list:
            opts[opt.name] = opt
        return utils.Config(conf, opts)

    def _setup_replication(self):
        """Set up the replication device."""
        self.rep_secondary.conf = self._create_rep_conf(self.conf)

        rep_devs = self.conf.safe_get('replication_device')
        if len(rep_devs) > 1:
            msg = utils.output_log(
                MSG.DRIVER_INITIALIZE_FAILED,
                config_group=self.conf.config_group,
                param='replication_device')
            self.raise_error(msg)

        rep_dev = dict(rep_devs[0])
        if not rep_dev.get('backend_id'):
            msg = utils.output_log(
                MSG.INVALID_PARAMETER,
                param=('replication_device[backend_id]'))
            self.raise_error(msg)
        self.rep_secondary.backend_id = rep_dev.pop('backend_id')
        self.rep_secondary_backend_id = self.rep_secondary.backend_id

        names = (_REPLICATION_DEVICE_KEY_NAMES +
                 _REPLICATION_DEVICE_STANDARD_KEY_NAMES)
        if self.driver_info['volume_type'] == 'iscsi':
            names += _REPLICATION_DEVICE_ISCSI_KEY_NAMES
        for name in names:
            opt_prefix = ('hitachi_' if name not in
                          (_REPLICATION_DEVICE_STANDARD_KEY_NAMES +
                           _REPLICATION_DEVICE_ISCSI_KEY_NAMES) else '')
            opt_name = opt_prefix + name
            if opt_name == 'hitachi_pool':
                opt_name = 'hitachi_pools'
            opt_val = rep_dev.pop(name, None)
            try:
                self.rep_secondary.conf.update(opt_name, opt_val)
            except Exception:
                msg = utils.output_log(
                    MSG.INVALID_PARAMETER,
                    param=('replication_device[%s]' % name))
                self.raise_error(msg)

        if rep_dev:
            names = ', '.join(rep_dev.keys())
            msg = utils.output_log(
                MSG.INVALID_PARAMETER,
                param=('replication_device[%s]' % names))
            self.raise_error(msg)

        if len(getattr(self.rep_secondary.conf, 'hitachi_pools', [])) != 1:
            msg = utils.output_log(
                MSG.INVALID_PARAMETER,
                param=('replication_device[pool]'))
            self.raise_error(msg)

    def update_volume_stats(self):
        """Update properties, capabilities and current states of the driver."""
        if self.conf.hitachi_mirror_storage_id:
            if self.rep_primary:
                data = self.rep_primary.update_volume_stats()
            else:
                data = self.rep_secondary.update_volume_stats()
        else:
            data = self._get_active_backend().update_volume_stats()
            data['replication_enabled'] = True
            data['replication_targets'] = [self.rep_secondary_backend_id]
            data['replication_type'] = [_ASYNC_STRING]
            data['consistent_group_replication_enabled'] = True
            data['group_replication_enabled'] = True
            if 'pools' in data:
                # Pool level only: the capabilities API filters its
                # response down to a fixed field list, so these would be
                # dropped there, while scheduler-stats returns the pool
                # dict verbatim.
                pair_status = (
                    self._pair_status_capabilities()
                    if self.conf.hitachi_replication_report_pair_status
                    else {})
                for pool in data['pools']:
                    pool.update(pair_status)
                    pool['replication_enabled'] = True
                    pool['replication_targets'] = [
                        self.rep_secondary_backend_id]
                    pool['replication_type'] = [_ASYNC_STRING]
                    # Group.is_replicated accepts either key, so a group
                    # type keyed on the other one must still schedule here.
                    pool['consistent_group_replication_enabled'] = True
                    pool['group_replication_enabled'] = True
                    pool['location_info']['execution_site'] = (
                        utils.SECONDARY_STR if self._active_backend_id else
                        utils.PRIMARY_STR)
        return data

    def _journals_by_id(self, instance):
        """Every journal on one site, keyed by journal id.

        Read once per statistics cycle and shared by every copy group: the
        copy groups in a backend usually share a journal, and one listing
        costs the same as one journal.
        """
        try:
            journals = instance.client.get_journals() or []
        except Exception:
            LOG.debug('Could not list journals for the pool capabilities.',
                      exc_info=True)
            return {}
        return {journal['journalId']: journal for journal in journals
                if journal.get('journalId') is not None}

    def _journal_state(self, copy_pairs, journals, is_secondary):
        """The journal metrics for one copy group, as the array reports them.

        A remote-mirror copy group does not carry consistencyTime or
        journalUsageRate on every microcode -- on VSP One B26 / VSP 5000 it
        carries neither -- so the only place the inputs to an RPO check
        exist is the journal itself. qCount is the number of Q-markers
        still held by the master journal, i.e. the write backlog that has
        not reached the other site; PJNN/SJNN with qCount 0 means the two
        sides are current.

        Reported from whichever side was queried: the master journal at the
        primary, the restore journal at a recovery site. Nothing is
        derived -- a lag in seconds would have to be invented, and would be
        indistinguishable from a real one.
        """
        jkey = 'svolJournalId' if is_secondary else 'pvolJournalId'
        ids = {pair[jkey] for pair in copy_pairs
               if pair.get(jkey) is not None}
        if len(ids) != 1:
            # No journal, or a copy group spanning several: an aggregate
            # over journals is not something the storage system defines.
            return {}
        journal = journals.get(ids.pop())
        if not journal:
            return {}
        state = {
            'journal_id': journal.get('journalId'),
            'journal_status': journal.get('journalStatus'),
            'journal_usage_rate': journal.get('usageRate'),
            'journal_q_count': journal.get('qCount'),
            'journal_q_marker': journal.get('qMarker'),
            'journal_active_paths': journal.get('numOfActivePaths'),
            'journal_side': utils.SECONDARY_STR if is_secondary
            else utils.PRIMARY_STR,
        }
        return {key: value for key, value in state.items()
                if value is not None}

    def _copy_grp_pair_state(self, copy_group_name, journals=None):
        """Read one copy group's state as the storage system reports it.

        Issued from whichever side is actually up: from the primary with a
        session on the peer in normal operation, and from the secondary
        with no session at all once failed over, where the primary's client
        was never initialized.
        """
        is_secondary = bool(self._active_backend_id or self._is_target_role())
        if is_secondary:
            grp = self._svol_instance().client.get_remote_copy_grp(
                None, copy_group_name, is_secondary=True)
        else:
            grp = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client, copy_group_name)
        copy_pairs = grp.get('copyPairs') or []
        # Only what the storage system actually returned: the fields on a
        # remote-mirror copy group vary by microcode, and a value invented
        # here would be indistinguishable from a real one.
        state = {
            'pair_count': len(copy_pairs),
            'pair_status': grp.get('pairStatus'),
            'consistency_time': grp.get('consistencyTime'),
            'journal_usage_rate': grp.get('journalUsageRate'),
        }
        if state['pair_status'] is None and copy_pairs:
            # No group-level status on this microcode. Report the member
            # states verbatim rather than inventing an aggregate, which
            # would need an ordering over PAIR/COPY/PSUS/PSUE/SSWS that
            # the storage system does not define.
            state['pvol_statuses'] = sorted(
                {pair['pvolStatus'] for pair in copy_pairs
                 if pair.get('pvolStatus')})
            state['svol_statuses'] = sorted(
                {pair['svolStatus'] for pair in copy_pairs
                 if pair.get('svolStatus')})
        if state.get('journal_usage_rate') is None:
            # Same reason the member states are reported above: the copy
            # group carried no journal metrics, so read them where they
            # actually live.
            state.update(self._journal_state(
                copy_pairs, journals or {}, is_secondary))
        return {key: value for key, value in state.items()
                if value is not None}

    def _pair_status_capabilities(self):
        """Per-copy-group pair state and the inputs an RPO check needs.

        Block Storage exposes neither replication lag nor a vendor pair
        state: volume.replication_status is a coarse enum set from
        configuration, not from the storage system, and the driver method
        that could report lag is never called. A client that needs either
        one therefore has to hold storage credentials of its own purely to
        read them. Pool capabilities are returned verbatim by
        GET /v3/scheduler-stats/get_pools?detail=True, which makes this the
        one channel that carries the data out without a Block Storage
        change.

        Best effort by contract. This runs on every statistics cycle and
        must never take that cycle down with it, so each failure degrades
        to fewer keys instead of an exception.
        """
        capabilities = {
            _PAIR_STATUS_PEER_KEY: self.rep_secondary is not None}
        if self.rep_secondary is None:
            # Every copy-group read goes through the secondary instance,
            # for its client when failed over and as the remote end when
            # not, and it never initialized.
            return capabilities
        enumerated = True
        if self._active_backend_id or self._is_target_role():
            # Listing copy groups needs a session on the primary, whose
            # client was never set up on the failed-over path. Fall back to
            # the names this process has already seen: each one can still
            # be read from the secondary without a session. A process that
            # starts up already failed over has seen none, and reports
            # none until a group operation names one -- hence the flag.
            enumerated = False
            copy_group_names = sorted(self._known_copy_groups)
        else:
            try:
                copy_grps = self.rep_primary.client.get_remote_copy_grps(
                    self.rep_secondary.client) or []
            except Exception:
                LOG.debug(
                    'Could not enumerate copy groups for the pool '
                    'capabilities.', exc_info=True)
                capabilities[_PAIR_STATUS_ENUMERATED_KEY] = False
                return capabilities
            copy_group_names = [grp['copyGroupName'] for grp in copy_grps
                                if grp.get('copyGroupName')]
            self._known_copy_groups.update(copy_group_names)
        if len(copy_group_names) > _PAIR_STATUS_MAX_COPY_GROUPS:
            LOG.debug(
                'Reporting pair state for %(max)d of %(found)d copy '
                'groups; raise _PAIR_STATUS_MAX_COPY_GROUPS to report '
                'more.',
                {'max': _PAIR_STATUS_MAX_COPY_GROUPS,
                 'found': len(copy_group_names)})
            copy_group_names = copy_group_names[
                :_PAIR_STATUS_MAX_COPY_GROUPS]
        pairs = {}
        journals = self._journals_by_id(
            self._svol_instance() if
            (self._active_backend_id or self._is_target_role())
            else self.rep_primary)
        for copy_group_name in copy_group_names:
            try:
                pairs[copy_group_name] = self._copy_grp_pair_state(
                    copy_group_name, journals)
            except Exception:
                LOG.debug(
                    'Could not read copy group %s for the pool '
                    'capabilities.', copy_group_name, exc_info=True)
        capabilities[_PAIR_STATUS_ENUMERATED_KEY] = enumerated
        capabilities[_PAIR_STATUS_KEY] = json.dumps(pairs, sort_keys=True)
        capabilities[_PAIR_STATUS_UPDATED_KEY] = (
            timeutils.utcnow().isoformat())
        return capabilities

    def _get_active_backend(self):
        """Get the active backend."""
        return (self.rep_secondary if self._active_backend_id else
                self.rep_primary)

    def _convert_model_update(self, model_update, is_snapshot=False):
        """Convert a model update information."""
        if self.conf.hitachi_mirror_storage_id:
            return model_update
        if not is_snapshot:
            model_update['replication_status'] = (
                fields.ReplicationStatus.DISABLED)
        if not self._active_backend_id:
            model_update['provider_location'] = (
                _pack_rep_provider_location(
                    pldev=int(model_update['provider_location'])))
        else:
            model_update['provider_location'] = (
                _pack_rep_provider_location(
                    sldev=int(model_update['provider_location'])))
        return model_update

    def _require_rep_primary(self):
        if not self.rep_primary:
            msg = utils.output_log(
                MSG.SITE_NOT_INITIALIZED, storage_id=self.primary_storage_id,
                site='primary')
            self.raise_error(msg)

    def _require_rep_secondary(self):
        if not self.rep_secondary:
            msg = utils.output_log(
                MSG.SITE_NOT_INITIALIZED, storage_id=self.secondary_storage_id,
                site='secondary')
            self.raise_error(msg)

    def _is_target_role(self):
        """True when this backend adopts S-VOLs rather than creating them."""
        return self.conf.hitachi_replication_role == _ROLE_TARGET

    def _svol_instance(self):
        """The instance whose storage system holds the S-VOLs.

        Site-relative, and everything on the adopt and takeover paths has
        to agree on it. At a source-role backend the S-VOLs are on the
        replication_device, so it is rep_secondary. At a target-role
        backend -- a recovery site that adopted promoted S-VOLs -- they are
        this backend's own LDEVs, so it is rep_primary, and rep_secondary
        may be None because the peer never answered.
        """
        if self._is_target_role():
            return self.rep_primary
        return self.rep_secondary

    def _require_svol_instance(self):
        """Fail cleanly when the S-VOL side never initialized."""
        if self._is_target_role():
            self._require_rep_primary()
        else:
            self._require_rep_secondary()

    def _resolve_copy_group_name(self, group, volumes=None):
        """Which array copy group this Cinder group is bound to.

        Deriving the name from the Cinder group id is right only at the
        site that created the group. A group created anywhere else has a
        different UUID -- and the derivation keeps just 25 of its 32 hex
        characters, so it cannot be reversed either -- and would name a
        copy group the storage system has never heard of. Hence an
        explicit binding, in precedence order: the members' own metadata,
        then a marker on the group's name, then the derivation.
        """
        bound = {name for name in
                 (_volume_copy_group_binding(volume)
                  for volume in volumes or ())
                 if name}
        if len(bound) > 1:
            # A half-bound group would operate on one copy group while
            # reporting on another. Refuse instead of picking.
            msg = utils.output_log(
                MSG.GROUP_REPLICATION_BINDING_CONFLICT,
                group=group.id, copy_groups=', '.join(sorted(bound)))
            self.raise_error(msg)
        if bound:
            return bound.pop()
        name = getattr(group, 'name', None) or ''
        if name.startswith(_GROUP_NAME_BINDING_PREFIX):
            explicit = name[len(_GROUP_NAME_BINDING_PREFIX):].strip()
            if explicit:
                return explicit
        return self._create_group_copy_group_name(group.id)

    def _is_mirror_spec(self, extra_specs):
        topology = None
        if not extra_specs:
            return False
        if self.driver_info.get('driver_dir_name'):
            topology = extra_specs.get(
                self.driver_info['driver_dir_name'] + ':topology')
        if topology is None:
            return False
        elif topology == 'active_active_mirror_volume':
            return True
        else:
            msg = self.rep_primary.output_log(
                MSG.INVALID_EXTRA_SPEC_KEY,
                key=self.driver_info['driver_dir_name'] + ':topology',
                value=topology)
            self.raise_error(msg)

    def _create_rep_ldev(self, volume, extra_specs, rep_type, pvol=None):
        """Create a primary volume and  a secondary volume."""
        pool_id = self.rep_secondary.storage_info['pool_id'][0]
        ldev_range = self.rep_secondary.storage_info['ldev_range']
        qos_specs = utils.get_qos_specs_from_volume(volume)
        thread = greenthread.spawn(
            self.rep_secondary.create_ldev, volume.size, extra_specs,
            pool_id, ldev_range, qos_specs=qos_specs)
        if pvol is None:
            try:
                pool_id = self.rep_primary.get_pool_id_of_volume(volume)
                ldev_range = self.rep_primary.storage_info['ldev_range']
                pvol = self.rep_primary.create_ldev(volume.size,
                                                    extra_specs,
                                                    pool_id, ldev_range,
                                                    qos_specs=qos_specs)
            except exception.VolumeDriverException:
                self.rep_primary.output_log(MSG.CREATE_LDEV_FAILED)
        try:
            svol = thread.wait()
        except Exception:
            self.rep_secondary.output_log(MSG.CREATE_LDEV_FAILED)
            svol = None
        if pvol is None or svol is None:
            for vol, type_, instance in zip((pvol, svol), ('P-VOL', 'S-VOL'),
                                            self.instances):
                if vol is None:
                    msg = instance.output_log(
                        MSG.CREATE_REPLICATION_VOLUME_FAILED,
                        type=type_, rep_type=rep_type,
                        volume_id=volume.id,
                        volume_type=volume.volume_type.name, size=volume.size)
                else:
                    instance.delete_ldev(vol)
            self.raise_error(msg)
        thread = greenthread.spawn(
            self.rep_secondary.modify_ldev_name,
            svol, volume['id'].replace("-", ""))
        try:
            self.rep_primary.modify_ldev_name(
                pvol, volume['id'].replace("-", ""))
        finally:
            thread.wait()
        return pvol, svol

    def _create_rep_copy_group_name(self, ldev):
        return self.driver_info['target_prefix'] + '%s%02X%s%02d' % (
            CONF.my_ip, self.conf.hitachi_replication_number,
            _MIRROR_IDENTIFIER if self.conf.hitachi_mirror_storage_id else
            _ASYNC_IDENTIFIER, ldev >> 10)

    def _create_group_copy_group_name(self, group_id):
        # One copy group per Cinder group, namespaced by the driver
        # prefix so it never collides with the per-LDEV names built by
        # _create_rep_copy_group_name above. Cut to
        # _MAX_GROUP_COPY_GROUP_NAME rather than _MAX_COPY_GROUP_NAME:
        # create_journals() suffixes this name with '-JNL' to label the
        # journal LDEV, and a 29-character name overruns that field by one.
        prefix = self.driver_info['target_prefix']
        name = prefix + group_id.replace(
            '-', '')[:_MAX_GROUP_COPY_GROUP_NAME - len(prefix)]
        if len(name) > _MAX_GROUP_COPY_GROUP_NAME:
            # A longer prefix would put the name over the limit silently,
            # and every enable_replication would then fail at the array.
            msg = utils.output_log(
                MSG.INVALID_PARAMETER, param='copy group name: %s' % name)
            self.raise_error(msg)
        return name

    def _create_group_snapshot_group_name(self, group_snapshot_id):
        # One Thin Image group per Cinder group snapshot. The 'HBSD-'
        # target prefix keeps these clear of upstream's 'HBSD'-prefixed
        # per-LDEV CTG names built by _create_ctg_snapshot_group_name.
        return (self.driver_info['target_prefix'] + 'C' +
                group_snapshot_id.replace('-', ''))[:rest._MAX_COPY_GROUP_NAME]

    def _modify_journal(self, instance, journal_id):
        """Modify the journal information."""
        tolerance = (
            self.conf.hitachi_replication_journal_path_failure_tolerance)
        body = {
            'dataOverflowWatchInSeconds':
                self.conf.hitachi_replication_journal_overflow_tolerance,
            'isCacheModeEnabled':
                self.conf.hitachi_replication_journal_use_cache,
            'copySpeed':
                int(self.conf.hitachi_replication_journal_transfer_speed),
            'mirrorUnit': {
                'muNumber': self.conf.hitachi_replication_mun,
                'copyPace':
                    self.conf.hitachi_replication_journal_creation_speed,
                'pathBlockadeWatchInMinutes': tolerance},
        }
        instance.client.modify_journal(journal_id, body)

    def _journal_instances(self):
        """The sites a journal can actually be created on or removed from.

        A site with no session is not one of them: iterating it would only
        raise on the first client call.
        """
        return [instance for instance in self.instances if instance]

    def _delete_journals(self, journal_ids):
        """Delete journal volumes."""
        for instance, journal_id in zip(self._journal_instances(),
                                        journal_ids):
            try:
                ldev = instance.client.get_journal(journal_id, no_log=True)[
                    'firstLdevId']
                instance.client.delete_journal(journal_id, no_log=True)
                instance.delete_ldev(ldev)
                LOG.debug(
                    'A journal and its LDEV were deleted. (storage: '
                    '%(storage)s, journal: %(journal)s, LDEV: %(ldev)s)',
                    {'storage': instance.storage_id[-6:],
                     'journal': journal_id, 'ldev': ldev})
            except exception.VolumeDriverException:
                # Leaks a journal and a journal LDEV on the array, so this
                # has to be visible without debug logging enabled.
                LOG.warning(
                    'A journal and/or its LDEV were not deleted. '
                    '(storage: %(storage)s, journal: %(journal)s)',
                    {'storage': instance.storage_id[-6:],
                     'journal': journal_id})

    def create_journals(self, volume, copy_group_name):
        """Create a journal volume."""
        journal_ids = []
        journal_ldevs = []
        instances = self._journal_instances()
        try:
            for instance in instances:
                pool_id = (self.rep_primary.get_pool_id_of_volume(volume)
                           if instance == self.rep_primary
                           else self.rep_secondary.storage_info['pool_id'][0])
                ldev_range = instance.storage_info['ldev_range']
                ldev = instance.create_ldev(
                    self.conf.hitachi_replication_journal_size, {},
                    pool_id, ldev_range)
                # Track before labelling, not after: the rollback below frees
                # only what is in journal_ldevs, so a modify_ldev failure used
                # to leak the LDEV it had just created.
                journal_ldevs.append(ldev)
                instance.client.modify_ldev(
                    ldev, {'label': _JOURNAL_VOLUME_LABEL % copy_group_name})
                while True:
                    journal_list = instance.client.get_journals()
                    journal_id = _get_unused_minimum_value(
                        [journal['journalId'] for journal in journal_list])
                    errobj = instance.client.add_journal(
                        journal_id, ldev, ignore_message_id=[
                            _MSGID_JOURNAL_ID_ALREADY_USED,
                            _MSGID_NO_AVAILABLE_JOURNAL_ID],
                        ignore_error=[_MAX_JID_COUNT_EXCEEDED])[1]
                    if (errobj.get('messageId') ==
                            _MSGID_JOURNAL_ID_ALREADY_USED):
                        continue
                    if (errobj.get('messageId') ==
                            _MSGID_NO_AVAILABLE_JOURNAL_ID or
                            utils.safe_get_err_code(errobj) ==
                            _MAX_JID_COUNT_EXCEEDED):
                        msg = instance.output_log(
                            MSG.CREATE_JOURNAL_FAILED, volume=volume.id)
                        self.raise_error(msg)
                    LOG.debug(
                        'A journal and its LDEV were created. (storage: '
                        '%(storage)s, journal: %(journal)s, LDEV: %(ldev)s)',
                        {'storage': instance.storage_id[-6:],
                         'journal': journal_id, 'ldev': ldev})
                    journal_ids.append(journal_id)
                    self._modify_journal(instance, journal_id)
                    break
        except exception.VolumeDriverException:
            with excutils.save_and_reraise_exception():
                self._delete_journals(journal_ids)
                if len(journal_ldevs) > len(journal_ids):
                    instances[len(journal_ldevs) - 1].delete_ldev(
                        journal_ldevs[-1])
        return journal_ids

    def _get_rep_copy_speed(self):
        rep_copy_speed = self.rep_primary.conf.safe_get(
            self.driver_info['param_prefix'] + '_replication_copy_speed')
        if rep_copy_speed:
            return rep_copy_speed
        else:
            return self.rep_primary.conf.hitachi_copy_speed

    def _get_wait_pair_status_change_params(self, wait_type):
        """Get a replication pair status information."""
        _wait_pair_status_change_params = {
            _WAIT_PAIR: {
                'instance': self.rep_primary,
                'remote_client': self.rep_secondary.client,
                'is_secondary': False,
                'transitional_status': ['COPY'],
                'expected_status': ['PAIR', 'PFUL'],
                'msgid': MSG.CREATE_REPLICATION_PAIR_FAILED,
                'status_keys': ['pvolStatus', 'svolStatus'],
            },
            _WAIT_PSUS: {
                'instance': self.rep_primary,
                'remote_client': self.rep_secondary.client,
                'is_secondary': False,
                'transitional_status': ['PAIR', 'PFUL'],
                'expected_status': ['PSUS', 'SSUS'],
                'msgid': MSG.SPLIT_REPLICATION_PAIR_FAILED,
                'status_keys': ['pvolStatus', 'svolStatus'],
            },
            _WAIT_SSWS: {
                'instance': self.rep_secondary,
                'remote_client': None,
                'is_secondary': True,
                'transitional_status': ['PAIR', 'PFUL', 'PFUS', 'PSUE',
                                        'SSUS'],
                'expected_status': ['SSWS'],
                'msgid': MSG.SPLIT_REPLICATION_PAIR_FAILED,
                'status_keys': ['svolStatus'],
            },
            _WAIT_SPLIT: {
                'instance': self.rep_primary,
                'remote_client': self.rep_secondary.client,
                'is_secondary': False,
                'transitional_status': ['PAIR', 'PFUL'],
                'expected_status': ['PSUS', 'SSUS', 'PSUE', 'PFUS', 'SSWS'],
                'msgid': MSG.SPLIT_REPLICATION_PAIR_FAILED,
                'status_keys': ['pvolStatus', 'svolStatus'],
            }
        }
        return _wait_pair_status_change_params[wait_type]

    def _wait_pair_status_change(self, copy_group_name, pvol, svol,
                                 rep_type, wait_type, instance=None):
        """Wait until the replication pair status changes to the specified

        status.

        :param instance: overrides which instance is polled. The SSWS
            parameters name rep_secondary, which is the S-VOL side only at
            a source-role backend; a takeover issued from a recovery site
            has to be confirmed on the storage system it was issued to,
            not on the one that is gone.
        """
        for _ in _delays(
                self.conf.hitachi_replication_status_check_short_interval,
                self.conf.hitachi_replication_status_check_long_interval,
                self.conf.hitachi_replication_status_check_timeout):
            params = self._get_wait_pair_status_change_params(wait_type)
            if instance is not None:
                params = dict(params, instance=instance)
            status = params['instance'].client.get_remote_copypair(
                params['remote_client'], copy_group_name, pvol, svol,
                is_secondary=params['is_secondary'])
            statuses = [status.get(status_key) for status_key in
                        params['status_keys']]
            unexpected_status_set = (set(statuses) -
                                     set(params['expected_status']))
            if not unexpected_status_set:
                break
            if unexpected_status_set.issubset(
                    set(params['transitional_status'])):
                continue
            msg = params['instance'].output_log(
                params['msgid'], rep_type=rep_type, pvol=pvol, svol=svol,
                copy_group=copy_group_name, status='/'.join(statuses))
            self.raise_error(msg)
        else:
            status = params['instance'].client.get_remote_copypair(
                params['remote_client'], copy_group_name, pvol, svol,
                is_secondary=params['is_secondary'])
            msg = params['instance'].output_log(
                MSG.PAIR_CHANGE_TIMEOUT,
                rep_type=rep_type, pvol=pvol, svol=svol,
                copy_group=copy_group_name, current_status='/'.join(statuses),
                expected_status=str(params['expected_status']),
                timeout=self.conf.hitachi_replication_status_check_timeout)
            self.raise_error(msg)

    def _create_rep_pair(self, volume, pvol, svol, rep_type,
                         is_data_reduction_force_copy,
                         do_initialcopy=True, journal_ids=None):
        """Create a replication pair."""
        created_journal_ids = []
        copy_group_name = self._create_rep_copy_group_name(pvol)

        @utils.synchronized_on_copy_group()
        def inner(self, remote_client, copy_group_name, secondary_storage_id,
                  conf, copyPace, journal_ids, parent):
            is_new_copy_grp = True
            result = self.get_remote_copy_grps(remote_client)
            if result:
                for data in result:
                    if copy_group_name == data['copyGroupName']:
                        is_new_copy_grp = False
                        break
            body = {
                'copyGroupName': copy_group_name,
                'copyPairName': parent._LDEV_NAME % (pvol, svol),
                'replicationType': rep_type,
                'remoteStorageDeviceId': secondary_storage_id,
                'pvolLdevId': pvol,
                'svolLdevId': svol,
                'pathGroupId': conf.hitachi_path_group_id,
                'localDeviceGroupName': copy_group_name + 'P',
                'remoteDeviceGroupName': copy_group_name + 'S',
                'isNewGroupCreation': is_new_copy_grp,
                'doInitialCopy': do_initialcopy,
                'isDataReductionForceCopy': is_data_reduction_force_copy
            }
            if rep_type == parent.driver_info['mirror_attr']:
                body['quorumDiskId'] = conf.hitachi_quorum_disk_id
                body['copyPace'] = copyPace
                if is_new_copy_grp:
                    body['muNumber'] = 0
            if (rep_type == parent.driver_info.get('rep_type_async') and
                    is_new_copy_grp):
                body['muNumber'] = conf.hitachi_replication_mun
                if not journal_ids:
                    journal_ids = parent.create_journals(volume,
                                                         copy_group_name)
                    created_journal_ids.extend(journal_ids)
                body['pvolJournalId'], body['svolJournalId'] = journal_ids
            self.add_remote_copypair(remote_client, body)

        try:
            inner(
                self.rep_primary.client, self.rep_secondary.client,
                copy_group_name, self.rep_secondary.storage_id,
                self.rep_secondary.conf, self._get_rep_copy_speed(),
                journal_ids, self)
            self._wait_pair_status_change(
                copy_group_name, pvol, svol, rep_type, _WAIT_PAIR)
        except exception.VolumeDriverException:
            with excutils.save_and_reraise_exception():
                if created_journal_ids:
                    self._delete_journals(created_journal_ids)

    def _create_rep_ldev_and_pair(
            self, volume, extra_specs, rep_type, pvol=None):
        """Create volume and Replication pair."""
        capacity_saving = None
        if self.driver_info.get('driver_dir_name'):
            capacity_saving = extra_specs.get(
                self.driver_info['driver_dir_name'] + ':capacity_saving')
        is_data_reduction_force_copy = (
            capacity_saving == 'deduplication_compression')
        svol = None
        pvol, svol = self._create_rep_ldev(volume, extra_specs, rep_type, pvol)
        try:
            thread = greenthread.spawn(
                self.rep_secondary.initialize_pair_connection, svol)
            try:
                self.rep_primary.initialize_pair_connection(pvol)
            finally:
                thread.wait()
            if rep_type == self.driver_info['mirror_attr'] and self.\
                    rep_primary.conf.\
                    hitachi_set_mirror_reserve_attribute:
                self.rep_secondary.client.assign_virtual_ldevid(svol)
            self._create_rep_pair(volume, pvol, svol, rep_type,
                                  is_data_reduction_force_copy)
        except Exception:
            with excutils.save_and_reraise_exception():
                if svol is not None:
                    self.rep_secondary.terminate_pair_connection(svol)
                    if rep_type == self.driver_info['mirror_attr'] and self.\
                            rep_primary.conf.\
                            hitachi_set_mirror_reserve_attribute:
                        self.rep_secondary.client.unassign_virtual_ldevid(
                            svol)
                    self.rep_secondary.delete_ldev(svol)
                if pvol is not None:
                    self.rep_primary.terminate_pair_connection(pvol)
                    self.rep_primary.delete_ldev(pvol)
        return pvol, svol

    def create_volume(self, volume):
        """Create a volume from a volume or snapshot and return its properties.

        """
        if self._active_backend_id:
            return self._convert_model_update(
                self.rep_secondary.create_volume(volume))
        self._require_rep_primary()
        extra_specs = self.rep_primary.get_volume_extra_specs(volume)
        if self._is_mirror_spec(extra_specs):
            self._require_rep_secondary()
            rep_type = self.driver_info['mirror_attr']
            pldev, sldev = self._create_rep_ldev_and_pair(
                volume, extra_specs, rep_type)
            provider_location = _pack_rep_provider_location(
                pldev, sldev, rep_type)
            return {
                'provider_location': provider_location
            }
        if (volume.is_replicated() and
                not _volume_in_group_replication(volume)):
            # A member of a group-replication group is created unpaired:
            # _group_repl_add_volume takes the plain LDEV as the P-VOL and
            # builds the pair itself. _check_rep_ldev still rejects a
            # replicated volume in any other kind of group.
            _check_rep_ldev(self, volume, 'create a volume')
            rep_type = _get_rep_type(self, extra_specs)
            pldev, sldev = self._create_rep_ldev_and_pair(
                volume, extra_specs, rep_type)
            provider_location = _pack_rep_provider_location(
                pldev, sldev, rep_type)
            return {
                'provider_location': provider_location,
                'replication_status': fields.ReplicationStatus.ENABLED
            }
        return self._convert_model_update(
            self.rep_primary.create_volume(volume))

    def _verify_ldev(self, obj, operation, group_info=''):
        """Check ldev site for operation."""
        if (self._active_backend_id and _get_ldev_site(obj) == _PRIMARY or
                not self._active_backend_id and _get_ldev_site(obj) ==
                _SECONDARY):
            provider_location = obj.get('provider_location')
            obj_name = 'volume' if isinstance(
                obj, cinder_volume.Volume) else 'snapshot'
            if not self._active_backend_id:
                execution_site = utils.PRIMARY_STR
                ldev_site = utils.SECONDARY_STR
                ldev = json.loads(provider_location)['sldev']
            else:
                execution_site = utils.SECONDARY_STR
                ldev_site = utils.PRIMARY_STR
                ldev = (provider_location if provider_location.isdigit() else
                        json.loads(provider_location)['pldev'])
            msg = utils.output_log(
                MSG.OTHER_SITE_ERROR,
                operation=operation, execution_site=execution_site,
                ldev_site=ldev_site, group_info=group_info, obj=obj_name,
                obj_id=obj.id, ldev=ldev)
            self.raise_error(msg)

    def _has_rep_pair(self, ldev, instance=None, ldev_info=None):
        """Return if the specified LDEV has a replication pair.

        :param int ldev: The LDEV ID
        :param dict ldev_info: LDEV info
        :return: True if the LDEV status is normal and the LDEV has a
        replication pair, False otherwise
        :rtype: bool
        """
        instance = instance or self._get_active_backend()
        if ldev_info is None:
            ldev_info = instance.get_ldev_info(['status', 'attributes'], ldev)
        return (ldev_info['status'] == rest.NORMAL_STS and
                (rest.REP_ATTR in ldev_info['attributes'] or
                self.driver_info['mirror_attr'] in ldev_info['attributes']))

    def _get_rep_pair_info(self, pldev, sldev=None, ldev_info=None):
        """Return replication pair info.

        :param int pldev: The ID of the LDEV(P-VOL in case of a pair)
        :param dict ldev_info: LDEV info
        :return: replication pair info. An empty dict if the LDEV does not
        have a pair.
        :rtype: dict
        """
        pair_info = {}
        if sldev is not None:
            ldev = sldev
            instance = self.rep_secondary
        else:
            ldev = pldev
            instance = self._get_active_backend()
        if not self._has_rep_pair(ldev, instance, ldev_info):
            return pair_info
        self._require_rep_secondary()
        copy_group_name = self._create_rep_copy_group_name(pldev)
        if sldev is not None:
            pair = self.rep_secondary.client.get_remote_copypair(
                None, copy_group_name, pldev, sldev, is_secondary=True)
            if not pair:
                return pair_info
        else:
            pairs = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client,
                copy_group_name).get('copyPairs', [])
            for pair in pairs:
                if (pair.get('replicationType') in
                        [self.driver_info.get('rep_type_async'),
                         self.driver_info['mirror_attr']] and
                        pair['pvolLdevId'] == pldev):
                    break
            else:
                return pair_info
        pair_info['pvol'] = pldev
        pair_info['svol_info'] = [{
            'ldev': pair.get('svolLdevId'),
            'rep_type': pair.get('replicationType'),
            'is_psus': pair.get('svolStatus') in ['SSUS', 'PFUS'],
            'pvol_status': pair.get('pvolStatus'),
            'svol_status': pair.get('svolStatus')}]
        return pair_info

    def _get_journal_ids(self, copy_group_name):
        pairs = self.rep_primary.client.get_remote_copy_grp(
            self.rep_secondary.client, copy_group_name).get('copyPairs', [])
        return pairs[0].get('pvolJournalId'), pairs[0].get('svolJournalId')

    def _split_rep_pair(self, pvol, svol):
        copy_group_name = self._create_rep_copy_group_name(pvol)
        rep_type = self.driver_info['mirror_attr']
        self.rep_primary.client.split_remote_copypair(
            self.rep_secondary.client, copy_group_name, pvol, svol, rep_type)
        self._wait_pair_status_change(
            copy_group_name, pvol, svol, rep_type, _WAIT_PSUS)

    def _delete_rep_pair(
            self, pvol, svol, do_split=False, delete_journal=False):
        """Delete a replication pair."""
        copy_group_name = self._create_rep_copy_group_name(pvol)
        if delete_journal:
            journal_ids = self._get_journal_ids(copy_group_name)
        if do_split:
            self._split_rep_pair(pvol, svol)
        self.rep_primary.client.delete_remote_copypair(
            self.rep_secondary.client, copy_group_name, pvol, svol)
        if delete_journal:
            rtn = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client, copy_group_name,
                ignore_message_id=[_MSGID_INSTANCE_CANNOT_OPERATED])
            if rtn.get('messageId') == _MSGID_INSTANCE_CANNOT_OPERATED:
                self._delete_journals(journal_ids)

    def _delete_volume_pre_check(self, volume):
        """Pre-check for delete_volume().

        :param Volume volume: The volume to be checked
        :return: svol: The ID of the S-VOL
        :rtype: int
        :return: pvol_is_invalid: True if P-VOL is invalid, False otherwise
        :rtype: bool
        :return: svol_is_invalid: True if S-VOL is invalid, False otherwise
        :rtype: bool
        :return: pair_exists: True if the pair exists, False otherwise
        :rtype: bool
        """
        # Check if the LDEV in the primary storage corresponds to the volume
        pvol_is_invalid = True
        # To avoid KeyError when accessing a missing attribute, set the default
        # value to None.
        pvol_info = defaultdict(lambda: None)
        pvol = self.rep_primary.get_ldev(volume)
        if pvol is not None:
            if self.rep_primary.is_invalid_ldev(pvol, volume, pvol_info):
                # If the LDEV is assigned to another object, skip deleting it.
                self.rep_primary.output_log(
                    MSG.SKIP_DELETING_LDEV, obj='volume', obj_id=volume.id,
                    ldev=pvol, ldev_label=pvol_info['label'])
            else:
                pvol_is_invalid = False
        # Check if the pair exists on the storage.
        pair_exists = False
        svol_is_invalid = True
        svol = None
        rep_type = None
        if not pvol_is_invalid:
            pair_info = self._get_rep_pair_info(pvol, ldev_info=pvol_info)
            if pair_info:
                pair_exists = True
                # Because this pair is a valid P-VOL's pair, we need to delete
                # it and its LDEVs. The LDEV ID of the S-VOL to be deleted is
                # uniquely determined from the pair info. Therefore, there is
                # no need to get it from provider_location or to validate the
                # S-VOL by comparing the volume ID with the S-VOL's label.
                svol = pair_info['svol_info'][0]['ldev']
                svol_is_invalid = False
                rep_type = pair_info['svol_info'][0]['rep_type']
        # Check if the LDEV in the secondary storage corresponds to the volume
        if svol_is_invalid:
            svol = self.rep_secondary.get_ldev(volume)
            if svol is not None:
                # To avoid KeyError when accessing a missing attribute, set the
                # default value to None.
                svol_info = defaultdict(lambda: None)
                if self.rep_secondary.is_invalid_ldev(svol, volume, svol_info):
                    # If the LDEV is assigned to another object, skip deleting
                    # it.
                    self.rep_secondary.output_log(
                        MSG.SKIP_DELETING_LDEV, obj='volume', obj_id=volume.id,
                        ldev=svol, ldev_label=svol_info['label'])
                else:
                    svol_is_invalid = False
        return svol, pvol_is_invalid, svol_is_invalid, pair_exists, rep_type

    def delete_volume(self, volume):
        """Delete the specified volume."""
        self._require_rep_primary()
        # A volume that lives only on the secondary while the driver runs
        # from the primary is a group-replication object: a test-recovery
        # clone from create_group_from_src, or an S-VOL taken in by
        # manage_existing. It is secondary-resident by design, so
        # _verify_ldev's site check -- there to stop work on the site the
        # driver is not running from -- does not apply, and there is no
        # P-VOL of ours to unpair. Without this the clones could be
        # created but never removed.
        if (not self._active_backend_id and
                _get_ldev_site(volume) == _SECONDARY):
            self._require_rep_secondary()
            self.rep_secondary.delete_volume(volume)
            return
        self._verify_ldev(volume, 'delete a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if ldev is None:
            self._get_active_backend().output_log(
                MSG.INVALID_LDEV_FOR_DELETION, method='delete_volume',
                id=volume.id)
            return
        if self._active_backend_id:
            if self._has_rep_pair(ldev):
                msg = self.rep_secondary.output_log(
                    MSG.REPLICATION_PAIR_ERROR,
                    operation='delete a volume', volume=volume.id,
                    snapshot_info='', ldev=ldev)
                self.raise_error(msg)
            self.rep_secondary.delete_volume(volume)
            return
        # Run pre-check.
        svol, pvol_is_invalid, svol_is_invalid, pair_exists, rep_type = (
            self._delete_volume_pre_check(volume))
        # Delete the pair if it exists.
        if pair_exists:
            self._delete_rep_pair(
                ldev, svol,
                rep_type == self.driver_info['mirror_attr'],
                rep_type != self.driver_info['mirror_attr'])
        # Delete LDEVs if they are valid.
        thread = None
        if not svol_is_invalid:
            thread = greenthread.spawn(
                self.rep_secondary.delete_volume, volume)
        try:
            if not pvol_is_invalid:
                self.rep_primary.delete_volume(volume)
        finally:
            if thread is not None:
                thread.wait()

    def delete_ldev(self, ldev, ldev_info=None):
        """Delete the specified LDEV[s].

        :param int ldev: The ID of the LDEV(P-VOL in case of a pair) to be
        deleted
        :param dict ldev_info: LDEV(P-VOL in case of a pair) info
        :return: None
        """
        self._require_rep_primary()
        pair_info = self._get_rep_pair_info(ldev, ldev_info=ldev_info)
        if pair_info:
            self._delete_rep_pair(
                ldev, pair_info['svol_info'][0]['ldev'],
                pair_info['svol_info'][0]['rep_type'] ==
                self.driver_info['mirror_attr'],
                pair_info['svol_info'][0]['rep_type'] !=
                self.driver_info['mirror_attr'])
            th = greenthread.spawn(self.rep_secondary.delete_ldev,
                                   pair_info['svol_info'][0]['ldev'])
            try:
                self.rep_primary.delete_ldev(ldev)
            finally:
                th.wait()
        else:
            self.rep_primary.delete_ldev(ldev)

    def _create_rep_volume_from_src(
            self, volume, extra_specs, src, src_type, operation, rep_type):
        """Create a replication volume from a volume or snapshot and return

        its properties.
        """
        if rep_type != self.driver_info['mirror_attr']:
            _check_rep_ldev(self, volume, operation)
        data = self.rep_primary.create_volume_from_src(
            volume, src, src_type, is_rep=True)
        new_ldev = self.rep_primary.get_ldev(data)
        sldev = self._create_rep_ldev_and_pair(
            volume, extra_specs, rep_type, new_ldev)[1]
        provider_location = _pack_rep_provider_location(
            new_ldev, sldev, rep_type)
        if rep_type != self.driver_info['mirror_attr']:
            return {
                'provider_location': provider_location,
                'replication_status': fields.ReplicationStatus.ENABLED
            }
        else:
            return {
                'provider_location': provider_location,
            }

    def _create_volume_from_src(self, volume, src, src_type):
        """Create a volume from a volume or snapshot and return its properties.

        """
        self._require_rep_primary()
        operation = ('create a volume from a %s' % src_type)
        self._verify_ldev(src, operation)
        if self._active_backend_id:
            return self._convert_model_update(
                self.rep_secondary.create_volume_from_src(
                    volume, src, src_type))
        extra_specs = self.rep_primary.get_volume_extra_specs(volume)
        if self._is_mirror_spec(extra_specs):
            self._require_rep_secondary()
            return self._create_rep_volume_from_src(
                volume, extra_specs, src, src_type, operation,
                self.driver_info['mirror_attr'])
        if (volume.is_replicated() and
                not _volume_in_group_replication(volume)):
            return self._create_rep_volume_from_src(
                volume, extra_specs, src, src_type, operation,
                _get_rep_type(self, extra_specs))
        return self._convert_model_update(
            self.rep_primary.create_volume_from_src(volume, src, src_type))

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume and return its properties."""
        return self._create_volume_from_src(
            volume, src_vref, common.STR_VOLUME)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot and return its properties."""
        return self._create_volume_from_src(
            volume, snapshot, common.STR_SNAPSHOT)

    def create_snapshot(self, snapshot):
        """Create a snapshot from a volume and return its properties."""
        self._require_rep_primary()
        self._verify_ldev(snapshot.volume, 'create a snapshot')
        model_update = self._convert_model_update(
            self._get_active_backend().create_snapshot(snapshot),
            is_snapshot=True)
        return model_update

    def delete_snapshot(self, snapshot):
        """Delete the specified snapshot."""
        self._require_rep_primary()
        self._verify_ldev(snapshot, 'delete a snapshot')
        self._get_active_backend().delete_snapshot(snapshot)

    def _get_remote_copy_mode(self, vol):
        provider_location = vol.get('provider_location')
        if not provider_location:
            return None
        if provider_location.startswith('{'):
            loc = json.loads(provider_location)
            if isinstance(loc, dict):
                return loc.get('remote-copy')
        return None

    def _merge_properties(self, prop1, prop2):
        if prop1 is None:
            if prop2 is None:
                return []
            return prop2
        elif prop2 is None:
            return prop1
        d = dict(prop1)
        for key in ('target_luns', 'target_wwn', 'target_portals',
                    'target_iqns'):
            if key in d:
                d[key] = d[key] + prop2[key]
        if 'initiator_target_map' in d:
            for key2 in d['initiator_target_map']:
                d['initiator_target_map'][key2] = (
                    d['initiator_target_map'][key2]
                    + prop2['initiator_target_map'][key2])
        return d

    def initialize_connection_mirror(self, volume, connector):
        lun = None
        prop1 = None
        prop2 = None
        if self.rep_primary:
            try:
                conn_info1 = (
                    self.rep_primary.initialize_connection(
                        volume, connector, is_mirror=True))
            except Exception as ex:
                self.rep_primary.output_log(
                    MSG.REPLICATION_VOLUME_OPERATION_FAILED,
                    operation='attach', type='P-VOL',
                    volume_id=volume.id, reason=str(ex))
            else:
                prop1 = conn_info1['data']
                if self.driver_info['volume_type'] == 'fibre_channel':
                    if 'target_lun' in prop1:
                        lun = prop1['target_lun']
                    else:
                        lun = prop1['target_luns'][0]
        if self.rep_secondary:
            try:
                conn_info2 = (
                    self.rep_secondary.initialize_connection(
                        volume, connector, lun=lun, is_mirror=True))
            except Exception as ex:
                self.rep_secondary.output_log(
                    MSG.REPLICATION_VOLUME_OPERATION_FAILED,
                    operation='attach', type='S-VOL',
                    volume_id=volume.id, reason=str(ex))
                if prop1 is None:
                    raise ex
            else:
                prop2 = conn_info2['data']
        conn_info = {
            'driver_volume_type': self.driver_info['volume_type'],
            'data': self._merge_properties(prop1, prop2),
        }
        return conn_info

    def initialize_connection(self, volume, connector, is_snapshot=False):
        """Initialize connection between the server and the volume."""
        if (self._get_remote_copy_mode(volume) ==
                self.driver_info['mirror_attr']):
            conn_info = self.initialize_connection_mirror(volume, connector)
            if self.driver_info['volume_type'] == 'fibre_channel':
                fczm_utils.add_fc_zone(conn_info)
            return conn_info
        else:
            self._require_rep_primary()
            self._verify_ldev(volume, 'initialize volume connection')
            return self._get_active_backend().initialize_connection(
                volume, connector, is_snapshot=is_snapshot)

    def terminate_connection_mirror(self, volume, connector):
        prop1 = None
        prop2 = None
        if self.rep_primary:
            try:
                conn_info1 = self.rep_primary.terminate_connection(
                    volume, connector, is_mirror=True)
            except Exception as ex:
                self.rep_primary.output_log(
                    MSG.REPLICATION_VOLUME_OPERATION_FAILED,
                    operation='detach', type='P-VOL',
                    volume_id=volume.id, reason=str(ex))
                raise ex
            else:
                if conn_info1:
                    prop1 = conn_info1['data']
        if self.rep_secondary:
            try:
                conn_info2 = self.rep_secondary.terminate_connection(
                    volume, connector, is_mirror=True)
            except Exception as ex:
                self.rep_secondary.output_log(
                    MSG.REPLICATION_VOLUME_OPERATION_FAILED,
                    operation='detach', type='S-VOL',
                    volume_id=volume.id, reason=str(ex))
                raise ex
            else:
                if conn_info2:
                    prop2 = conn_info2['data']
        conn_info = {
            'driver_volume_type': self.driver_info['volume_type'],
            'data': self._merge_properties(prop1, prop2),
        }
        return conn_info

    def terminate_connection(self, volume, connector):
        """Terminate connection between the server and the volume."""
        if (self._get_remote_copy_mode(volume) ==
                self.driver_info['mirror_attr']):
            conn_info = self.terminate_connection_mirror(volume, connector)
            if self.driver_info['volume_type'] == 'fibre_channel':
                fczm_utils.remove_fc_zone(conn_info)
            return conn_info
        else:
            self._require_rep_primary()
            self._verify_ldev(volume, 'terminate volume connection')
            return self._get_active_backend().terminate_connection(
                volume, connector)

    def _extend_pair_volume(self, volume, new_size, ldev, pair_info):
        """Extend the specified  replication volume to the specified size."""
        extra_specs = self.rep_primary.get_volume_extra_specs(volume)
        capacity_saving = extra_specs.get(
            self.driver_info['driver_dir_name'] + ':capacity_saving')
        is_data_reduction_force_copy = (
            capacity_saving == 'deduplication_compression')
        if self.conf.hitachi_mirror_storage_id:
            rep_type = self.driver_info['mirror_attr']
        else:
            rep_type = _get_rep_type(self, extra_specs)
        pvol_info = self.rep_primary.get_ldev_info(
            ['numOfPorts'], pair_info['pvol'])
        if pvol_info['numOfPorts'] > 1:
            msg = self.rep_primary.output_log(
                MSG.EXTEND_REPLICATION_VOLUME_ERROR,
                rep_type=rep_type, volume_id=volume.id, ldev=ldev,
                source_size=volume.size, destination_size=new_size,
                pvol=pair_info['pvol'], svol='',
                pvol_num_of_ports=pvol_info['numOfPorts'],
                svol_num_of_ports='')
            self.raise_error(msg)
        if self.conf.hitachi_mirror_storage_id:
            journal_ids = None
        else:
            copy_group_name = self._create_rep_copy_group_name(ldev)
            journal_ids = self._get_journal_ids(copy_group_name)
        if not self.conf.safe_get(self.driver_info['param_prefix'] +
                                  '_extend_snapshot_volumes'):
            # If the volume has a snapshot, P-VOL is not expandable because it
            # is a P-VOL of a TI pair, while S-VOL is expandable because it is
            # not in a TI pair because a snapshot is not created in the
            # secondary storage. Expanding only the S-VOL makes it difficult to
            # restore the pair after an error occurs. To avoid this situation,
            # we check if P-VOL is expandable before expanding both LDEVs. The
            # following method raises an exception if P-VOL is in a TI pair,
            # and thus we can prevent expanding only S-VOL. Contrary to its
            # name, this method does not actually delete a TI pair in this
            # context because the P-VOL of a GAD/UR pair cannot be the S-VOL of
            # a TI pair.
            self.rep_primary.delete_pair(ldev)
        self._delete_rep_pair(
            ldev, pair_info['svol_info'][0]['ldev'],
            rep_type == self.driver_info['mirror_attr'], delete_journal=False)
        thread = greenthread.spawn(
            self.rep_secondary.extend_volume, volume, new_size)
        try:
            self.rep_primary.extend_volume(volume, new_size)
        finally:
            thread.wait()
        self._create_rep_pair(
            volume, pair_info['pvol'], pair_info['svol_info'][0]['ldev'],
            rep_type, is_data_reduction_force_copy, do_initialcopy=False,
            journal_ids=journal_ids)

    def extend_volume(self, volume, new_size):
        """Extend the specified volume to the specified size."""
        self._require_rep_primary()
        self._verify_ldev(volume, 'extend a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if ldev is None:
            msg = self._get_active_backend().output_log(
                MSG.INVALID_LDEV_FOR_EXTENSION, volume_id=volume.id)
            self.raise_error(msg)
        if self._active_backend_id:
            if self._has_rep_pair(ldev):
                msg = self.rep_secondary.output_log(
                    MSG.REPLICATION_PAIR_ERROR,
                    operation='extend a volume', volume=volume.id,
                    snapshot_info='', ldev=ldev)
                self.raise_error(msg)
            self.rep_secondary.extend_volume(volume, new_size)
            return
        pair_info = self._get_rep_pair_info(ldev)
        if pair_info:
            self._extend_pair_volume(volume, new_size, ldev, pair_info)
        else:
            self.rep_primary.extend_volume(volume, new_size)

    def manage_existing(self, volume, existing_ref):
        """Return volume properties which Cinder needs to manage the volume."""
        # A group-replication member, or a volume being adopted into one.
        if _volume_in_group_replication_or_bound(volume):
            return self._group_repl_manage_existing(volume, existing_ref)
        self._require_rep_primary()
        return self._convert_model_update(
            self._get_active_backend().manage_existing(volume, existing_ref))

    def manage_existing_get_size(self, volume, existing_ref):
        """Return the size[GB] of the specified volume."""
        if _volume_in_group_replication_or_bound(volume):
            return self._group_repl_manage_existing_get_size(
                volume, existing_ref)
        self._require_rep_primary()
        if not self.conf.hitachi_mirror_storage_id:
            if volume.is_replicated():
                msg = utils.output_log(
                    MSG.MANAGE_REPLICATION_VOLUME_ERROR,
                    replication_enabled=volume.volume_type.
                    extra_specs['replication_enabled'],
                    source_id=existing_ref.get('source-id'), volume=volume.id,
                    volume_type=volume.volume_type.name)
                self.raise_error(msg)
            ldev = common.str2int(existing_ref.get('source-id'))
            if ldev is None:
                msg = utils.output_log(MSG.INVALID_LDEV_FOR_MANAGE)
                raise exception.ManageExistingInvalidReference(
                    existing_ref=existing_ref, reason=msg)
            if self._has_rep_pair(ldev):
                msg = self._get_active_backend().output_log(
                    MSG.REPLICATION_PAIR_ERROR,
                    operation='manage a volume', volume=volume.id,
                    snapshot_info='', ldev=ldev)
                self.raise_error(msg)
        return self._get_active_backend().manage_existing_get_size(
            volume, existing_ref)

    def unmanage(self, volume):
        """Prepare the volume for removing it from Cinder management."""
        # Releasing a group-replication member leaves its pair intact.
        if _volume_in_group_replication_or_bound(volume):
            return self._group_repl_unmanage(volume)
        self._require_rep_primary()
        self._verify_ldev(volume, 'unmanage a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if ldev is None:
            self._get_active_backend().output_log(
                MSG.INVALID_LDEV_FOR_DELETION,
                method='unmanage', id=volume.id)
            return
        if self._has_rep_pair(ldev):
            msg = self._get_active_backend().output_log(
                MSG.REPLICATION_PAIR_ERROR,
                operation='unmanage a volume', volume=volume.id,
                snapshot_info='', ldev=ldev)
            self.raise_error(msg)
        self._get_active_backend().unmanage(volume)

    def discard_zero_page(self, volume):
        self._require_rep_primary()
        self._verify_ldev(volume, 'discard zero-data pages of a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if self._has_rep_pair(ldev):
            if self.conf.hitachi_mirror_storage_id:
                self._require_rep_secondary()
                th = greenthread.spawn(
                    self.rep_secondary.discard_zero_page, volume)
                try:
                    self.rep_primary.discard_zero_page(volume)
                finally:
                    th.wait()
        else:
            self._get_active_backend().discard_zero_page(volume)

    def unmanage_snapshot(self, snapshot):
        if not self.rep_primary:
            return self.rep_secondary.unmanage_snapshot(snapshot)
        else:
            return self._get_active_backend().unmanage_snapshot(snapshot)

    def retype(self, ctxt, volume, new_type, diff, host):
        self._require_rep_primary()
        self._verify_ldev(volume, 'retype a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if ldev is None:
            msg = self._get_active_backend().output_log(
                MSG.INVALID_LDEV_FOR_VOLUME_COPY,
                type='volume', id=volume.id)
            self.raise_error(msg)
        if (self._has_rep_pair(ldev) or new_type.is_replicated() or
                self._is_mirror_spec(new_type['extra_specs'])):
            return False
        return self._get_active_backend().retype(
            ctxt, volume, new_type, diff, host)

    def migrate_volume(self, volume, host):
        self._require_rep_primary()
        self._verify_ldev(volume, 'migrate a volume')
        ldev = self._get_active_backend().get_ldev(volume)
        if ldev is None:
            msg = self._get_active_backend().output_log(
                MSG.INVALID_LDEV_FOR_VOLUME_COPY,
                type='volume', id=volume.id)
            self.raise_error(msg)
        if self._get_rep_pair_info(ldev):
            return False, None
        else:
            return self._get_active_backend().migrate_volume(volume, host)

    def _resync_rep_pair(self, pvol, svol):
        copy_group_name = self._create_rep_copy_group_name(pvol)
        rep_type = self.driver_info['mirror_attr']
        self.rep_primary.client.resync_remote_copypair(
            self.rep_secondary.client, copy_group_name, pvol, svol,
            rep_type, copy_speed=self._get_rep_copy_speed())
        self._wait_pair_status_change(
            copy_group_name, pvol, svol, rep_type, _WAIT_PAIR)

    def revert_to_snapshot(self, volume, snapshot):
        """Rollback the specified snapshot."""
        self._require_rep_primary()
        self._verify_ldev(volume, 'revert a volume to a snapshot')
        self._verify_ldev(snapshot, 'revert a volume to a snapshot')
        ldev = self._get_active_backend().get_ldev(volume)
        if self.conf.hitachi_mirror_storage_id:
            svol = self.rep_primary.get_ldev(snapshot)
            if None in (ldev, svol):
                raise NotImplementedError()
            pair_info = self._get_rep_pair_info(ldev)
            is_snap = self.rep_primary.has_snap_pair(ldev, svol)
            if pair_info and is_snap:
                self._split_rep_pair(pair_info['pvol'],
                                     pair_info['svol_info'][0]['ldev'])
            try:
                self.rep_primary.revert_to_snapshot(volume, snapshot)
            finally:
                if pair_info and is_snap:
                    self._resync_rep_pair(pair_info['pvol'],
                                          pair_info['svol_info'][0]['ldev'])
        else:
            if ldev is None:
                msg = self._get_active_backend().output_log(
                    MSG.LDEV_NUMBER_NOT_FOUND,
                    operation='revert a volume to a snapshot',
                    obj='volume', obj_id=volume.id)
                self.raise_error(msg)
            if self._has_rep_pair(ldev):
                msg = self._get_active_backend().output_log(
                    MSG.REPLICATION_PAIR_ERROR,
                    operation='revert a volume to a snapshot',
                    volume=volume.id,
                    snapshot_info='snapshot: %s, ' % snapshot.id,
                    ldev=ldev)
                self.raise_error(msg)
            self._get_active_backend().revert_to_snapshot(volume, snapshot)

    def create_group(self):
        self._require_rep_primary()
        return self._get_active_backend().create_group()

    def delete_group(self, group, volumes):
        # Group-replication members carry a pair in the group's copy group,
        # and it has to be torn down before their LDEVs can go.
        if _is_group_replication(group):
            return self._group_repl_delete_group(group, volumes)
        if self.conf.hitachi_mirror_storage_id:
            self._require_rep_primary()
            return super(HBSDREPLICATION, self).delete_group(group, volumes)
        else:
            for volume in volumes:
                self._verify_ldev(volume, 'delete a volume in a group',
                                  'group: %s, ' % group.id)
            return self._get_active_backend().delete_group(group, volumes)

    def create_group_from_src(
            self, context, group, volumes, snapshots=None, source_vols=None):
        if self.conf.hitachi_mirror_storage_id:
            self._require_rep_primary()
            return super(HBSDREPLICATION, self).create_group_from_src(
                context, group, volumes, snapshots, source_vols)
        else:
            sources = snapshots or source_vols or []
            # Group-replication snapshots are Thin Image pairs the
            # secondary array holds beside the replication S-VOLs, so
            # their provider_location carries an sldev and no pldev.
            # Cloning them has to run on the secondary: the path below
            # calls _verify_ldev, which rejects a secondary-side LDEV
            # whenever the driver is not failed over, and that left the
            # snapshots create_group_snapshot makes creatable and
            # deletable but never usable -- no test recovery.
            if (not self._active_backend_id and sources and
                    all(_get_ldev_site(src) == _SECONDARY
                        for src in sources)):
                return self._group_repl_create_group_from_src(
                    context, group, volumes, snapshots, source_vols)
            operation = ('create a volume from a %s' %
                         ('volume in a group' if snapshots is None else
                          'snapshot in a group snapshot'))
            for obj in snapshots or source_vols:
                self._verify_ldev(obj, operation)
            model_update, volumes_model_update = (
                self._get_active_backend().create_group_from_src(
                    context, group, volumes, snapshots, source_vols))
            for volume_model_update in volumes_model_update:
                self._convert_model_update(volume_model_update)
            return model_update, volumes_model_update

    def update_group(self, group, add_volumes=None, remove_volumes=None):
        # Group-replication members are added to / removed from the
        # group's copy group; every other group keeps the upstream path.
        if _is_group_replication(group):
            return self._group_repl_update_group(
                group, add_volumes, remove_volumes)
        if self.conf.hitachi_mirror_storage_id:
            self._require_rep_primary()
            return self.rep_primary.update_group(group, add_volumes)
        else:
            for volume in add_volumes:
                self._verify_ldev(volume, 'add a volume to a group',
                                  'group: %s, ' % group.id)
                ldev = self._get_active_backend().get_ldev(volume)
                if ldev is None:
                    msg = self._get_active_backend().output_log(
                        MSG.LDEV_NOT_EXIST_FOR_ADD_GROUP,
                        volume_id=volume.id, group='group', group_id=group.id)
                    self.raise_error(msg)
                if self._has_rep_pair(ldev):
                    extra_specs = (self._get_active_backend().
                                   get_volume_extra_specs(volume))
                    rep_type = _get_rep_type(self, extra_specs)
                    msg = self._get_active_backend().output_log(
                        MSG.REPLICATION_VOLUME_ADD_GROUP_ERROR,
                        rep_type=rep_type, volume_id=volume.id, ldev=ldev,
                        group_id=group.id)
                    self.raise_error(msg)
            return self._get_active_backend().update_group(group, add_volumes)

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        if _is_group_snapshot_replication(group_snapshot):
            return self._group_repl_create_group_snapshot(
                context, group_snapshot, snapshots)
        if self.conf.hitachi_mirror_storage_id:
            self._require_rep_primary()
            return self.rep_primary.create_group_snapshot(
                context, group_snapshot, snapshots)
        else:
            for snapshot in snapshots:
                self._verify_ldev(snapshot.volume, 'create a group snapshot')
            rtn = self._get_active_backend().create_group_snapshot(
                context, group_snapshot, snapshots)
            for snapshot_model_update in rtn[-1]:
                if 'provider_location' in snapshot_model_update:
                    self._convert_model_update(
                        snapshot_model_update, is_snapshot=True)
            return rtn

    def delete_group_snapshot(self, group_snapshot, snapshots):
        if _is_group_snapshot_replication(group_snapshot):
            return self._group_repl_delete_group_snapshot(
                group_snapshot, snapshots)
        if self.conf.hitachi_mirror_storage_id:
            self._require_rep_primary()
            return self.rep_primary.delete_group_snapshot(
                group_snapshot, snapshots)
        else:
            for snapshot in snapshots:
                self._verify_ldev(snapshot,
                                  'delete a snapshot in a group snapshot',
                                  'group snapshot: %s, ' % group_snapshot.id)
            return self._get_active_backend().delete_group_snapshot(
                group_snapshot, snapshots)

    def _group_repl_aggregate_status(self, volumes_model_update,
                                     success_status):
        if any(update.get('replication_status') ==
               fields.ReplicationStatus.ERROR
               for update in volumes_model_update):
            return fields.ReplicationStatus.ERROR
        return success_status

    def _group_repl_create_pair(self, volume, copy_group_name, pvol, svol,
                                is_data_reduction_force_copy,
                                is_new_copy_grp):
        parent = self
        created_journal_ids = []

        @utils.synchronized_on_copy_group()
        def inner(self, remote_client, copy_group_name):
            body = {
                'copyGroupName': copy_group_name,
                'copyPairName': parent._LDEV_NAME % (pvol, svol),
                'replicationType': parent.driver_info['rep_type_async'],
                'fenceLevel': 'ASYNC',
                'remoteStorageDeviceId': parent.rep_secondary.storage_id,
                'pvolLdevId': pvol,
                'svolLdevId': svol,
                'pathGroupId':
                    parent.rep_secondary.conf.hitachi_path_group_id,
                'localDeviceGroupName': copy_group_name + 'P',
                'remoteDeviceGroupName': copy_group_name + 'S',
                'isNewGroupCreation': is_new_copy_grp,
                'doInitialCopy': True,
                'isDataReductionForceCopy': is_data_reduction_force_copy,
                'muNumber':
                    parent.rep_secondary.conf.hitachi_replication_mun,
            }
            if is_new_copy_grp:
                # An asynchronous UR pair has nowhere to stage writes
                # without them, so the pair that creates the copy group
                # creates its journals too, exactly as _create_rep_pair
                # does for the per-LDEV copy groups.
                with _log_step('create journals',
                               copy_group=copy_group_name):
                    journal_ids = parent.create_journals(
                        volume, copy_group_name)
                created_journal_ids.extend(journal_ids)
                body['pvolJournalId'], body['svolJournalId'] = journal_ids
            with _log_step('create replication pair',
                           copy_group=copy_group_name, pvol=pvol, svol=svol):
                self.add_remote_copypair(remote_client, body)

        try:
            inner(self.rep_primary.client, self.rep_secondary.client,
                  copy_group_name)
        except exception.VolumeDriverException:
            with excutils.save_and_reraise_exception():
                if created_journal_ids:
                    self._delete_journals(created_journal_ids)

    def _group_repl_journal_ids(self, copy_group_name):
        """The copy group's journal ids, read while its pairs still exist.

        Once the last pair goes the array removes the copy group, and with
        it the only record of which journals the group was using.
        """
        try:
            grp = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client, copy_group_name)
        except exception.VolumeDriverException:
            return None
        pairs = grp.get('copyPairs') or []
        if not pairs:
            return None
        journal_ids = (pairs[0].get('pvolJournalId'),
                       pairs[0].get('svolJournalId'))
        if any(journal_id is None for journal_id in journal_ids):
            return None
        return journal_ids

    def _group_repl_delete_journals(self, copy_group_name, journal_ids):
        """Drop the group's journals once its last pair has gone.

        The group path creates the copy group, so it owns the journals for
        their whole life -- unlike _create_rep_pair, whose per-LDEV copy
        groups _delete_rep_pair tears down. A copy group that still answers
        still has pairs in it, so its journals stay.
        """
        if not journal_ids:
            return
        try:
            rtn = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client, copy_group_name,
                ignore_message_id=[_MSGID_INSTANCE_CANNOT_OPERATED])
        except exception.VolumeDriverException:
            return
        if rtn.get('messageId') == _MSGID_INSTANCE_CANNOT_OPERATED:
            with _log_step('delete journals', copy_group=copy_group_name,
                           journals=journal_ids):
                self._delete_journals(journal_ids)

    def _group_repl_copy_grp_exists(self, copy_group_name):
        """Check the copy group with the list call, not the get (B3)."""
        remote_copy_grps = self.rep_primary.client.get_remote_copy_grps(
            self.rep_secondary.client) or []
        return any(grp['copyGroupName'] == copy_group_name
                   for grp in remote_copy_grps)

    def _group_repl_add_volume(self, volume, copy_group_name,
                               is_new_copy_grp, operation):
        """Create the S-VOL and its pair for one group-replication member.

        Shared by enable_replication and update_group; returns the per-volume
        model update, marking the volume ERROR rather than aborting the rest.
        """
        try:
            pvol = self.rep_primary.get_ldev(volume)
            if pvol is None:
                msg = self.rep_primary.output_log(
                    MSG.LDEV_NUMBER_NOT_FOUND, operation=operation,
                    obj='volume', obj_id=volume.id)
                self.raise_error(msg)
            extra_specs = self.rep_primary.get_volume_extra_specs(volume)
            capacity_saving = None
            if self.driver_info.get('driver_dir_name'):
                capacity_saving = extra_specs.get(
                    self.driver_info['driver_dir_name'] + ':capacity_saving')
            with _log_step('create secondary volume', volume=volume.id):
                svol = self.rep_secondary.create_ldev(
                    volume.size, extra_specs,
                    self.rep_secondary.storage_info['pool_id'][0],
                    self.rep_secondary.storage_info['ldev_range'],
                    qos_specs=utils.get_qos_specs_from_volume(volume))
            try:
                self._group_repl_create_pair(
                    volume, copy_group_name, pvol, svol,
                    capacity_saving == 'deduplication_compression',
                    is_new_copy_grp)
            except exception.VolumeDriverException:
                with excutils.save_and_reraise_exception():
                    self.rep_secondary.delete_ldev(svol)
            utils.output_log(
                MSG.GROUP_REPLICATION_PAIR_CREATED,
                copy_group=copy_group_name, pvol=pvol, svol=svol)
            volume_update = {
                'id': volume.id,
                'replication_status': fields.ReplicationStatus.ENABLED,
                'provider_location': _pack_rep_provider_location(
                    pldev=pvol, sldev=svol)}
            volume_update.update(_metadata_model_update(
                volume, **{_MD_PVOL: pvol,
                           _MD_SVOL: svol,
                           _MD_COPY_GROUP: copy_group_name}))
            return volume_update
        except exception.VolumeDriverException:
            self.rep_primary.output_log(
                MSG.GROUP_REPLICATION_PAIR_CREATE_FAILED,
                volume=volume.id, copy_group=copy_group_name)
            return {
                'id': volume.id,
                'replication_status': fields.ReplicationStatus.ERROR}

    def _group_repl_delete_volume(self, volume, copy_group_name, operation):
        """Delete one member's copy pair.

        Shared by disable_replication and update_group. The array removes the
        copy group itself once the last pair goes (B1), so it is never deleted
        explicitly here.
        """
        try:
            pvol = self.rep_primary.get_ldev(volume)
            svol = self.rep_secondary.get_ldev(volume)
            if pvol is None or svol is None:
                msg = self.rep_primary.output_log(
                    MSG.LDEV_NUMBER_NOT_FOUND, operation=operation,
                    obj='volume', obj_id=volume.id)
                self.raise_error(msg)
            with _log_step('delete replication pair',
                           copy_group=copy_group_name, pvol=pvol, svol=svol):
                self.rep_primary.client.delete_remote_copypair(
                    self.rep_secondary.client, copy_group_name, pvol, svol)
            utils.output_log(
                MSG.GROUP_REPLICATION_PAIR_DELETED,
                copy_group=copy_group_name, pvol=pvol, svol=svol)
            volume_update = {
                'id': volume.id,
                'replication_status': fields.ReplicationStatus.DISABLED}
            volume_update.update(_metadata_model_update(
                volume, **{_MD_PVOL: None,
                           _MD_SVOL: None,
                           _MD_COPY_GROUP: None}))
            return volume_update
        except exception.VolumeDriverException:
            self.rep_primary.output_log(
                MSG.GROUP_REPLICATION_PAIR_DELETE_FAILED,
                volume=volume.id, copy_group=copy_group_name)
            return {
                'id': volume.id,
                'replication_status': fields.ReplicationStatus.ERROR}

    def _group_repl_create_group_from_src(self, context, group, volumes,
                                          snapshots, source_vols):
        """Clone secondary-resident sources into volumes on the secondary.

        This is the test-recovery path: the sources are the Thin Image
        snapshots _group_repl_create_group_snapshot left on the secondary,
        and the clones have to live beside them. provider_location is
        packed sldev-only so every later operation on a clone -- delete
        included -- resolves to the secondary, and replication_status is
        DISABLED because a clone is not a member of the copy group.

        The copy loop is written out rather than delegated to the upstream
        one because that rollback resolves created LDEVs with a primary
        instance and finds none on this path, so a partial failure would
        leak every LDEV it had already made.
        """
        self._require_rep_secondary()
        secondary = self.rep_secondary
        from_snapshot = bool(snapshots)
        sources = snapshots if from_snapshot else source_vols
        volumes_model_update = []
        new_ldevs = []
        try:
            for volume, src in zip(volumes, sources):
                if secondary.get_ldev(src) is None:
                    msg = secondary.output_log(
                        MSG.INVALID_LDEV_FOR_VOLUME_COPY,
                        type='snapshot' if from_snapshot else 'volume',
                        id=src.id)
                    self.raise_error(msg)
                model_update = (
                    secondary.create_volume_from_snapshot(volume, src)
                    if from_snapshot else
                    secondary.create_cloned_volume(volume, src))
                new_ldev = int(model_update['provider_location'])
                new_ldevs.append(new_ldev)
                volumes_model_update.append({
                    'id': volume.id,
                    'provider_location': _pack_rep_provider_location(
                        sldev=new_ldev),
                    'replication_status':
                        fields.ReplicationStatus.DISABLED})
        except Exception:
            with excutils.save_and_reraise_exception():
                for new_ldev in new_ldevs:
                    try:
                        secondary.delete_ldev(new_ldev)
                    except exception.VolumeDriverException:
                        secondary.output_log(
                            MSG.DELETE_LDEV_FAILED, ldev=new_ldev)
        return None, volumes_model_update

    def _group_repl_suspended_members(self, copy_group_name, volumes):
        """Members that already have a pair in this copy group.

        The storage system is only asked when at least one member's
        provider_location claims both LDEVs: a member carrying only a
        P-VOL has never been paired, and the query costs a remote-mirror
        session on every enable_replication.
        """
        candidates = [volume for volume in volumes
                      if _get_ldev_site(volume) == _PRIMARY_SECONDARY]
        if not candidates:
            return []
        try:
            grp = self.rep_primary.client.get_remote_copy_grp(
                self.rep_secondary.client, copy_group_name)
        except exception.VolumeDriverException:
            # Without the pair list, treat every member as unpaired: an
            # add against an existing pair fails loudly, whereas a resync
            # of a pair that is not there would fail silently.
            LOG.warning('Could not read copy group %s; enabling replication '
                        'will add pairs rather than restart them.',
                        copy_group_name)
            return []
        paired = {pair.get('pvolLdevId')
                  for pair in grp.get('copyPairs') or []}
        return [volume for volume in candidates
                if self.rep_primary.get_ldev(volume) in paired]

    def _group_repl_resync_members(self, copy_group_name, volumes):
        """Restart replication for members whose pairs are only suspended.

        The counterpart of the graceful split in failover_replication: swap
        is deliberately off, so this restores the original direction rather
        than reversing it. Reversing it is failback, and that goes through
        failover_replication with the failback sentinel.

        Reachable through enable_replication because re-enabling
        replication on a copy group that still exists is exactly what this
        is, and Cinder has no other verb for it. It also replaces a path
        that was simply wrong: adding a member whose pair already existed
        allocated a second S-VOL and then failed to create the pair.
        """
        rep_type = self.driver_info['rep_type_async']
        try:
            # resync is a copy-group operation, so it is issued once and
            # confirmed per pair below.
            with _log_step('resync copy group', copy_group=copy_group_name,
                           volumes=len(volumes)):
                self.rep_primary.client.resync_remote_copy_grp(
                    self.rep_secondary.client, copy_group_name, rep_type)
        except exception.VolumeDriverException:
            for volume in volumes:
                self.rep_primary.output_log(
                    MSG.GROUP_REPLICATION_RESYNC_FAILED,
                    volume=volume.id, copy_group=copy_group_name)
            return [{'id': volume.id,
                     'replication_status': fields.ReplicationStatus.ERROR}
                    for volume in volumes]
        volumes_model_update = []
        for volume in volumes:
            pvol = self.rep_primary.get_ldev(volume)
            svol = self.rep_secondary.get_ldev(volume)
            volume_status = fields.ReplicationStatus.ERROR
            try:
                self._wait_pair_status_change(
                    copy_group_name, pvol, svol, rep_type, _WAIT_PAIR)
                volume_status = fields.ReplicationStatus.ENABLED
            except exception.VolumeDriverException:
                self.rep_primary.output_log(
                    MSG.GROUP_REPLICATION_RESYNC_FAILED,
                    volume=volume.id, copy_group=copy_group_name)
            volumes_model_update.append(
                {'id': volume.id, 'replication_status': volume_status})
        return volumes_model_update

    def _group_repl_adopted_members(self, volumes):
        """True when every member is an adopted S-VOL: sldev, no pldev.

        The same discriminator create_group_from_src and delete_volume
        already use. A member created here carries both LDEVs; one adopted
        from a promoted S-VOL carries only its own.
        """
        return bool(volumes) and all(
            _get_ldev_site(volume) == _SECONDARY for volume in volumes)

    def _group_repl_adopt_members(self, copy_group_name, volumes):
        """Record replication for members the array has already paired.

        The recovery-site case. These volumes were adopted from promoted
        S-VOLs, so the copy group and its pairs exist and there is nothing
        to create -- but Cinder will not accept failover_replication until
        the group reports ENABLED, and enable_replication is the only
        transition it offers. So this branch verifies and records, and
        touches the storage system not at all.

        The copy group is read from the S-VOL side with no session on the
        peer, which is the only form that still answers once the other
        site is gone.
        """
        instance = self._svol_instance()
        try:
            grp = instance.client.get_remote_copy_grp(
                None, copy_group_name, is_secondary=True)
        except exception.VolumeDriverException:
            for volume in volumes:
                instance.output_log(
                    MSG.GROUP_REPLICATION_ADOPT_FAILED,
                    volume=volume.id, copy_group=copy_group_name)
            return [{'id': volume.id,
                     'replication_status': fields.ReplicationStatus.ERROR}
                    for volume in volumes]
        paired = {pair.get('svolLdevId')
                  for pair in grp.get('copyPairs') or []}
        volumes_model_update = []
        for volume in volumes:
            svol = _svol_of(volume)
            if svol is not None and svol in paired:
                status = fields.ReplicationStatus.ENABLED
            else:
                # Do not guess: a member the storage system does not list
                # as an S-VOL of this copy group is not replicated, and
                # reporting it ENABLED would make the group look
                # recoverable when it is not.
                status = fields.ReplicationStatus.ERROR
                instance.output_log(
                    MSG.GROUP_REPLICATION_ADOPT_FAILED,
                    volume=volume.id, copy_group=copy_group_name)
            volumes_model_update.append(
                {'id': volume.id, 'replication_status': status})
        return volumes_model_update

    def _group_repl_delete_group(self, group, volumes):
        """Delete a group-replication group and every member's copy pair.

        This path used to fall through to
        self._get_active_backend().delete_group(), which binds
        HBSDREST._delete_group's self.delete_volume() to rep_primary -- a
        plain LDEV deletion with no pair teardown. That destroyed the
        P-VOLs and left the UR pair and every S-VOL orphaned on the
        secondary array. Deleting the pair first is not optional: the array
        refuses to delete a paired LDEV, and the pair lives in the group's
        copy group, not in the per-LDEV copy group _delete_rep_pair()
        derives from a P-VOL id.

        The secondary is required even though nothing is created here: with
        the peer unreachable the pair cannot be torn down, and deleting the
        P-VOL alone would orphan exactly what this method exists to clean
        up.
        """
        self._require_rep_primary()
        self._require_rep_secondary()
        copy_group_name = self._resolve_copy_group_name(
            group, volumes)
        journal_ids = self._group_repl_journal_ids(copy_group_name)
        LOG.info('Group replication: deleting group %(group)s. (copy group: '
                 '%(cg)s, volumes: %(n)d, journals: %(j)s)',
                 {'group': group.id, 'cg': copy_group_name,
                  'n': len(volumes), 'j': journal_ids})
        model_update = {'status': group.status}
        volumes_model_update = []
        for volume in volumes:
            volume_update = self._group_repl_delete_group_volume(
                group, volume, copy_group_name)
            if volume_update['status'] != 'deleted':
                model_update['status'] = 'error'
            volumes_model_update.append(volume_update)
        self._group_repl_delete_journals(copy_group_name, journal_ids)
        return model_update, volumes_model_update

    def _group_repl_delete_group_volume(self, group, volume,
                                        copy_group_name):
        """Delete one member's pair, then its LDEVs on both arrays.

        Errors are reported per volume rather than raised, so one member
        that will not go does not strand the rest of the group.
        """
        pvol = self.rep_primary.get_ldev(volume)
        svol = self.rep_secondary.get_ldev(volume)
        try:
            if pvol is not None and svol is not None:
                with _log_step('delete replication pair',
                               copy_group=copy_group_name,
                               pvol=pvol, svol=svol):
                    self.rep_primary.client.delete_remote_copypair(
                        self.rep_secondary.client, copy_group_name,
                        pvol, svol)
                utils.output_log(
                    MSG.GROUP_REPLICATION_PAIR_DELETED,
                    copy_group=copy_group_name, pvol=pvol, svol=svol)
            # Both LDEVs are unpaired now, so either side can go first.
            thread = None
            if svol is not None:
                thread = greenthread.spawn(
                    self.rep_secondary.delete_volume, volume)
            try:
                if pvol is not None:
                    self.rep_primary.delete_volume(volume)
            finally:
                if thread is not None:
                    thread.wait()
            return {'id': volume.id, 'status': 'deleted'}
        except (exception.VolumeDriverException, exception.VolumeIsBusy,
                exception.SnapshotIsBusy) as exc:
            self.rep_primary.output_log(
                MSG.GROUP_OBJECT_DELETE_FAILED, obj='volume', group='group',
                group_id=group.id, obj_id=volume.id,
                ldev=self.get_ldev(volume, both=True), reason=exc.msg)
            return {
                'id': volume.id,
                'status': 'available' if isinstance(
                    exc, (exception.VolumeIsBusy,
                          exception.SnapshotIsBusy)) else 'error'}

    def _group_repl_create_group_snapshot(
            self, context, group_snapshot, snapshots):
        """Create one crash-consistent Thin Image group on the secondary.

        The members are snapshots of the replication S-VOLs, so the whole
        group is taken, split and later deleted on the secondary array.
        """
        self._require_rep_secondary()
        secondary = self.rep_secondary
        snapshot_group_name = self._create_group_snapshot_group_name(
            group_snapshot.id)
        pairs = []
        try:
            for snapshot in snapshots:
                # The P-VOL of each Thin Image pair is the replication S-VOL.
                pvol = secondary.get_ldev(snapshot.volume)
                if pvol is None:
                    msg = secondary.output_log(
                        MSG.INVALID_LDEV_FOR_VOLUME_COPY,
                        type='volume', id=snapshot.volume_id)
                    self.raise_error(msg)
                extra_specs = secondary.get_volume_extra_specs(snapshot.volume)
                svol = secondary.create_ldev(
                    snapshot.volume_size, extra_specs,
                    secondary.storage_info['pool_id'][0],
                    secondary.storage_info['ldev_range'],
                    qos_specs=utils.get_qos_specs_from_volume(snapshot))
                secondary.modify_ldev_name(svol, snapshot.id.replace('-', ''))
                pairs.append(
                    {'snapshot': snapshot, 'pvol': pvol, 'svol': svol})
            # Upstream already builds the CTG bodies with autoSplit off and
            # issues exactly one split_snapshotgroup for the whole group;
            # it only needs the group's name from us.
            with _log_step('create group snapshot',
                           snapshot_group=snapshot_group_name,
                           snapshots=len(pairs)):
                secondary._create_ctg_snap_pair(pairs, snapshot_group_name)
        except Exception:
            utils.output_log(
                MSG.GROUP_REPLICATION_SNAPSHOT_FAILED,
                group_snapshot=group_snapshot.id)
            for pair in pairs:
                if pair.get('svol') is not None:
                    try:
                        secondary.delete_ldev(pair['svol'])
                    except exception.VolumeDriverException:
                        secondary.output_log(
                            MSG.DELETE_LDEV_FAILED, ldev=pair['svol'])
            return ({'status': fields.GroupSnapshotStatus.ERROR},
                    [{'id': snapshot.id,
                      'status': fields.SnapshotStatus.ERROR}
                     for snapshot in snapshots])
        # sldev provider_location is what lets the delete path find these
        # Thin Image pairs on the secondary.
        return None, [
            {'id': pair['snapshot'].id,
             'status': fields.SnapshotStatus.AVAILABLE,
             'provider_location': _pack_rep_provider_location(
                 sldev=pair['svol'])}
            for pair in pairs]

    def _group_repl_delete_group_snapshot(self, group_snapshot, snapshots):
        """Delete the Thin Image pairs and S-VOLs left on the secondary."""
        self._require_rep_secondary()
        try:
            with _log_step('delete group snapshot',
                           group_snapshot=group_snapshot.id,
                           snapshots=len(snapshots)):
                return self.rep_secondary._delete_group(
                    group_snapshot, snapshots, True)
        except Exception:
            with excutils.save_and_reraise_exception():
                utils.output_log(
                    MSG.GROUP_REPLICATION_SNAPSHOT_DELETE_FAILED,
                    group_snapshot=group_snapshot.id)

    def _check_adopted_svol_manageability(self, ldev, existing_ref):
        """Manageability check for an S-VOL that is still in a copy pair.

        The general check refuses any LDEV whose attributes are not a
        subset of the plain volume ones, which excludes the
        remote-replication attribute. That is right everywhere else and
        wrong here: adopting a promoted S-VOL is the entire point of this
        path, and the S-VOL carries that attribute for as long as the pair
        exists -- so H1 could never adopt the volumes a recovery actually
        has to adopt.

        Every other guard is kept, in particular that the LDEV is still
        unmapped: Cinder owns the export from here on, so the LUN paths
        have to be created after the adopt, not before.
        """
        instance = self._svol_instance()
        ldev_info = instance.get_ldev_info(
            ['emulationType', 'numOfPorts', 'attributes', 'status'], ldev)
        allowed = set([
            'CVS', utils.DRS_VOL_ATTR, utils.VC_VOL_ATTR, rest.REP_ATTR,
            self.driver_info['hdp_vol_attr'],
            self.driver_info['hdt_vol_attr']])
        attributes = set(ldev_info['attributes'])
        if (ldev_info['status'] != rest.NORMAL_STS or
                not ldev_info['emulationType'].startswith('OPEN-V') or
                len(attributes) < 2 or
                not attributes.issubset(allowed)):
            msg = instance.output_log(
                MSG.INVALID_LDEV_ATTR_FOR_MANAGE, ldev=ldev,
                ldevtype=self.driver_info['nvol_ldev_type'])
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)
        if ldev_info['numOfPorts']:
            msg = instance.output_log(
                MSG.INVALID_LDEV_PORT_FOR_MANAGE, ldev=ldev)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

    def _group_repl_resolve_ref_ldev(self, volume, existing_ref):
        """Resolve existing_ref to an LDEV on the S-VOL side."""
        ldev = None
        if 'source-name' in existing_ref:
            ldev = self._svol_instance().get_ldev_by_name(
                existing_ref.get('source-name').replace('-', ''))
        elif 'source-id' in existing_ref:
            ldev = common.str2int(existing_ref.get('source-id'))
        # The LDEV must resolve before any of its properties are read.
        if ldev is None:
            utils.output_log(
                MSG.GROUP_REPLICATION_MANAGE_FAILED, volume=volume.id,
                reason='the reference does not name an LDEV on the '
                       'secondary storage')
            msg = utils.output_log(MSG.INVALID_LDEV_FOR_MANAGE)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)
        return ldev

    def _group_repl_manage_existing(self, volume, existing_ref):
        """Adopt a promoted S-VOL on the S-VOL side."""
        self._require_svol_instance()
        instance = self._svol_instance()
        ldev = self._group_repl_resolve_ref_ldev(volume, existing_ref)
        self._check_adopted_svol_manageability(ldev, existing_ref)
        instance.modify_ldev_name(ldev, volume['id'].replace('-', ''))
        new_qos_specs = utils.get_qos_specs_from_volume(volume)
        old_qos_specs = instance.get_qos_specs_from_ldev(ldev)
        if old_qos_specs != new_qos_specs:
            instance.change_qos_specs(ldev, old_qos_specs, new_qos_specs)
        # No LUN is mapped here: export belongs to initialize_connection.
        model_update = {
            'provider_location': _pack_rep_provider_location(sldev=ldev)}
        model_update.update(
            _metadata_model_update(volume, **{_MD_SVOL: ldev}))
        return model_update

    def _group_repl_manage_existing_get_size(self, volume, existing_ref):
        """Return the size[GB] of a promoted S-VOL on the S-VOL side."""
        self._require_svol_instance()
        ldev = self._group_repl_resolve_ref_ldev(volume, existing_ref)
        return self._svol_instance().get_ldev_size_in_gigabyte(
            ldev, existing_ref)

    def _group_repl_unmanage(self, volume):
        """Release Cinder's claim on a member without touching its pair."""
        self._require_svol_instance()
        instance = self._svol_instance()
        ldev = _svol_of(volume)
        if ldev is None:
            instance.output_log(
                MSG.INVALID_LDEV_FOR_DELETION, method='unmanage',
                id=volume['id'])
            return
        # Clear the nickname manage_existing set, or it leaks on the array.
        try:
            instance.modify_ldev_name(ldev, '')
        except exception.VolumeDriverException:
            utils.output_log(
                MSG.GROUP_REPLICATION_NICKNAME_CLEANUP_FAILED,
                volume=volume['id'], ldev=ldev)
        utils.output_log(
            MSG.GROUP_REPLICATION_VOLUME_UNMANAGED,
            volume=volume['id'], ldev=ldev)

    def _group_repl_update_group(self, group, add_volumes, remove_volumes):
        """Add pairs to / remove pairs from an existing group copy group."""
        self._require_rep_primary()
        self._require_rep_secondary()
        copy_group_name = self._resolve_copy_group_name(
            group, (add_volumes or []) + (remove_volumes or []))
        copy_grp_exists = self._group_repl_copy_grp_exists(copy_group_name)
        LOG.info('Group replication: updating group %(group)s. (copy group: '
                 '%(cg)s, add: %(add)d, remove: %(rm)d, copy group exists: '
                 '%(exists)s)',
                 {'group': group.id, 'cg': copy_group_name,
                  'add': len(add_volumes or []),
                  'rm': len(remove_volumes or []),
                  'exists': copy_grp_exists})
        # A member whose pair is merely suspended is restarted, not added:
        # adding it would allocate a second S-VOL and then fail to pair it.
        suspended = (
            self._group_repl_suspended_members(
                copy_group_name, add_volumes or [])
            if copy_grp_exists else [])
        suspended_ids = {volume.id for volume in suspended}
        add_volumes_update = []
        if suspended:
            add_volumes_update.extend(
                self._group_repl_resync_members(copy_group_name, suspended))
        is_new_copy_grp = not copy_grp_exists
        for volume in add_volumes or []:
            if volume.id in suspended_ids:
                continue
            volume_model_update = self._group_repl_add_volume(
                volume, copy_group_name, is_new_copy_grp,
                'add a volume to a group replication group')
            if (volume_model_update['replication_status'] !=
                    fields.ReplicationStatus.ERROR):
                is_new_copy_grp = False
            add_volumes_update.append(volume_model_update)
        remove_volumes_update = [
            self._group_repl_delete_volume(
                volume, copy_group_name,
                'remove a volume from a group replication group')
            for volume in remove_volumes or []]
        model_update = {
            'status': (
                fields.GroupStatus.ERROR
                if self._group_repl_aggregate_status(
                    add_volumes_update + remove_volumes_update,
                    fields.ReplicationStatus.ENABLED) ==
                fields.ReplicationStatus.ERROR
                else fields.GroupStatus.AVAILABLE)}
        return model_update, add_volumes_update, remove_volumes_update

    def enable_replication(self, context, group, volumes):
        copy_group_name = self._resolve_copy_group_name(
            group, volumes)
        if self._group_repl_adopted_members(volumes):
            # Adopted S-VOLs: the pairs are already there, so record them
            # rather than build anything. Only the S-VOL side has to be
            # reachable, which at a recovery site is all there is.
            self._require_svol_instance()
            volumes_model_update = self._group_repl_adopt_members(
                copy_group_name, volumes)
            return ({'replication_status': self._group_repl_aggregate_status(
                volumes_model_update, fields.ReplicationStatus.ENABLED)},
                volumes_model_update)
        self._require_rep_primary()
        self._require_rep_secondary()
        copy_grp_exists = self._group_repl_copy_grp_exists(copy_group_name)
        LOG.info('Group replication: enabling on group %(group)s. (copy '
                 'group: %(cg)s, volumes: %(n)d, copy group exists: %(e)s)',
                 {'group': group.id, 'cg': copy_group_name,
                  'n': len(volumes), 'e': copy_grp_exists})
        suspended = (
            self._group_repl_suspended_members(copy_group_name, volumes)
            if copy_grp_exists else [])
        suspended_ids = {volume.id for volume in suspended}
        volumes_model_update = []
        if suspended:
            volumes_model_update.extend(
                self._group_repl_resync_members(copy_group_name, suspended))
        is_new_copy_grp = not copy_grp_exists
        for volume in volumes:
            if volume.id in suspended_ids:
                continue
            volume_model_update = self._group_repl_add_volume(
                volume, copy_group_name, is_new_copy_grp,
                'enable group replication')
            if (volume_model_update['replication_status'] !=
                    fields.ReplicationStatus.ERROR):
                is_new_copy_grp = False
            volumes_model_update.append(volume_model_update)
        model_update = {
            'replication_status': self._group_repl_aggregate_status(
                volumes_model_update, fields.ReplicationStatus.ENABLED)}
        return model_update, volumes_model_update

    def disable_replication(self, context, group, volumes):
        self._require_rep_primary()
        self._require_rep_secondary()
        copy_group_name = self._resolve_copy_group_name(
            group, volumes)
        journal_ids = self._group_repl_journal_ids(copy_group_name)
        LOG.info('Group replication: disabling on group %(group)s. (copy '
                 'group: %(cg)s, volumes: %(n)d, journals: %(j)s)',
                 {'group': group.id, 'cg': copy_group_name,
                  'n': len(volumes), 'j': journal_ids})
        volumes_model_update = [
            self._group_repl_delete_volume(
                volume, copy_group_name, 'disable group replication')
            for volume in volumes]
        self._group_repl_delete_journals(copy_group_name, journal_ids)
        model_update = {
            'replication_status': self._group_repl_aggregate_status(
                volumes_model_update, fields.ReplicationStatus.DISABLED)}
        return model_update, volumes_model_update

    def failover_replication(self, context, group, volumes,
                             secondary_backend_id=None):
        self._require_rep_primary()
        # Not rep_secondary: at a target-role backend the S-VOLs are local
        # and the peer is the array the takeover exists to work without.
        self._require_svol_instance()
        copy_group_name = self._resolve_copy_group_name(
            group, volumes)
        secondary_backend_id, requested_mode = _parse_failover_target(
            secondary_backend_id)
        is_failback = secondary_backend_id == _REP_FAILBACK
        LOG.info('Group replication: %(dir)s on group %(group)s. (copy '
                 'group: %(cg)s, volumes: %(n)d, target: %(t)s, mode: %(m)s)',
                 {'dir': 'failback' if is_failback else 'failover',
                  'group': group.id, 'cg': copy_group_name,
                  'n': len(volumes), 't': secondary_backend_id,
                  'm': requested_mode or '-'})
        if is_failback and requested_mode:
            # A split mode means nothing on failback, and accepting it
            # would be worse than useless: the volume manager compares the
            # value it was given -- suffix and all -- against its own
            # failback sentinel, so it would record this group as failed
            # over while the driver resynced it.
            msg = utils.output_log(
                MSG.INVALID_DESTINATION,
                direction='back', execution_site=utils.SECONDARY_STR,
                specified_backend_id=_REP_FAILBACK + _MODE_SUFFIX_SEP +
                requested_mode,
                defined_backend_id=_REP_FAILBACK)
            raise exception.InvalidReplicationTarget(reason=msg)
        rep_type = self.driver_info['rep_type_async']
        mode = _failover_mode(group, requested_mode)
        is_graceful = not is_failback and mode == _MODE_GRACEFUL
        # Remember the name while the primary is still reachable: after
        # this call the copy groups can no longer be listed, and the pool
        # capabilities fall back to the names seen so far.
        self._known_copy_groups.add(copy_group_name)
        try:
            if is_failback:
                self.rep_secondary.client.resync_remote_copy_grp(
                    self.rep_primary.client, copy_group_name,
                    rep_type, swap=True, is_secondary=True)
            elif is_graceful:
                # A plain copy-group pairsplit, issued from the primary
                # with a session on the secondary. The storage system
                # drains the journal to a consistency point before it
                # suspends the pairs, which is what makes this a planned
                # failover: the S-VOLs come up with every acknowledged
                # write, whereas a takeover in forceSplit mode promises
                # only crash consistency. It needs both sites reachable,
                # so it is opt-in and never the default.
                self.rep_primary.client.split_remote_copy_grp(
                    self.rep_secondary.client, copy_group_name, rep_type)
            else:
                # Issued to the storage system holding the S-VOLs. At a
                # source-role backend that is the peer; at a recovery site
                # it is the local one, and the peer is the dead array.
                self._require_svol_instance()
                self._svol_instance().client.takeover_remote_copy_grp(
                    None, copy_group_name)
        except exception.VolumeDriverException:
            msgid = (MSG.GROUP_REPLICATION_FAILBACK_FAILED if is_failback
                     else MSG.GROUP_REPLICATION_FAILOVER_FAILED)
            msg = self._svol_instance().output_log(
                msgid, group=group.id, copy_group=copy_group_name)
            raise exception.UnableToFailOver(reason=msg)
        utils.output_log(
            MSG.GROUP_REPLICATION_TAKEOVER_STARTED,
            copy_group=copy_group_name)
        # The group call above is issued with job_nowait, so confirm each
        # member actually reached its expected state before reporting the
        # new status. Waiting per pair does not defeat the CTG aspect (B2):
        # only the takeover itself has to be group-wide, which is the same
        # split that _failback_copy_group makes.
        # A pairsplit suspends in place -- P-VOL PSUS, S-VOL SSUS -- and
        # only a takeover leaves the S-VOL in SSWS. Waiting for SSWS after
        # a graceful split would time out on pairs that are already where
        # they were asked to go.
        if is_failback:
            wait_type = _WAIT_PAIR
            wait_instance = None
        elif is_graceful:
            wait_type = _WAIT_PSUS
            wait_instance = None
        else:
            wait_type = _WAIT_SSWS
            # The SSWS parameters name rep_secondary, which is the S-VOL
            # side only at a source-role backend. Without this the
            # takeover fires at the right storage system and is then
            # confirmed against the wrong -- possibly dead -- one.
            wait_instance = self._svol_instance()
        status = (fields.ReplicationStatus.ENABLED if is_failback else
                  fields.ReplicationStatus.FAILED_OVER)
        volumes_model_update = []
        for volume in volumes:
            pvol, svol = self._get_ldevs(volume, is_failback=is_failback)
            volume_status = fields.ReplicationStatus.ERROR
            if pvol is not None and svol is not None:
                try:
                    self._wait_pair_status_change(
                        copy_group_name, pvol, svol, rep_type, wait_type,
                        instance=wait_instance)
                    volume_status = status
                except exception.VolumeDriverException:
                    utils.output_log(
                        MSG.FAILOVER_FAILBACK_WARNING,
                        direction='back' if is_failback else 'over',
                        obj='volume',
                        operation='failback' if is_failback else 'failover',
                        obj_id=volume.id)
            volumes_model_update.append(
                {'id': volume.id, 'replication_status': volume_status})
        model_update = {
            'replication_status': self._group_repl_aggregate_status(
                volumes_model_update, status)}
        return model_update, volumes_model_update

    def list_replication_targets(self, context, group):
        self._require_rep_primary()
        copy_group_name = self._resolve_copy_group_name(
            group, None)
        # Listing copy groups needs a session on the peer, which a failed
        # over or target-role backend cannot open -- and this is exactly
        # where a client asks what it can fail over to. Read the one copy
        # group from the side holding the S-VOLs instead, the same way
        # _copy_grp_pair_state does.
        if self._active_backend_id or self._is_target_role():
            self._require_svol_instance()
            try:
                self._svol_instance().client.get_remote_copy_grp(
                    None, copy_group_name, is_secondary=True)
            except exception.VolumeDriverException:
                exists = False
            else:
                exists = True
            return {'replication_targets': (
                [{'backend_id': self.rep_secondary_backend_id}] if exists
                else [])}
        self._require_rep_secondary()
        try:
            remote_copy_grps = self.rep_primary.client.get_remote_copy_grps(
                self.rep_secondary.client) or []
        except exception.VolumeDriverException:
            msg = self.rep_primary.output_log(
                MSG.GROUP_REPLICATION_TARGETS_QUERY_FAILED, group=group.id)
            self.raise_error(msg)
        exists = any(
            grp['copyGroupName'] == copy_group_name
            for grp in remote_copy_grps)
        targets = (
            [{'backend_id': self.rep_secondary_backend_id}] if exists
            else [])
        return {'replication_targets': targets}

    def _get_ldevs(self, volume, is_failback=False):
        pldev = self.rep_primary.get_ldev(volume)
        # Read straight out of the provider_location rather than through
        # rep_secondary.get_ldev: the id is the same at either site, and
        # rep_secondary is None once the peer has gone.
        sldev = (_svol_of(volume) if
                 _get_ldev_site(volume) in (_SECONDARY, _PRIMARY_SECONDARY)
                 else None)
        if pldev is None or sldev is None:
            instance = (self.rep_primary if pldev is None else
                        self._svol_instance())
            instance.output_log(
                MSG.NOT_LDEV_NUMBER_WARNING,
                operation='fail back a volume' if is_failback else
                'fail over a volume', obj='volume', obj_id=volume.id)
        return pldev, sldev

    def _get_rep_pairs(self, volumes):
        rep_pairs = []
        for volume in volumes:
            if _volume_in_group_replication(volume):
                utils.output_log(
                    MSG.GROUP_REPLICATION_UNSUPPORTED_OPERATION,
                    operation='Host failback',
                    details='volume: %(volume)s, group: %(group)s; use '
                            'group failback for group replication volumes' %
                            {'volume': volume.id, 'group': volume.group_id})
                continue
            if volume.replication_status in (
                    fields.ReplicationStatus.FAILED_OVER,
                    fields.ReplicationStatus.FAILOVER_ERROR):
                pldev, sldev = self._get_ldevs(volume, is_failback=True)
                if pldev is None or sldev is None:
                    continue
                rep_pairs.append((pldev, sldev))
        return rep_pairs

    def _extract_my_copy_grps(self, remote_copy_groups):
        my_groups = []
        copy_group_name = self._create_rep_copy_group_name(0)
        my_prefix = copy_group_name[:len(copy_group_name) - 2]
        for remote_copy_group in remote_copy_groups:
            if remote_copy_group['copyGroupName'].startswith(my_prefix):
                my_groups.append(remote_copy_group)
        return my_groups

    def _get_failback_target_pairs(self, copy_group_name, rep_pairs):
        try:
            pairs = (
                self.rep_primary.client.get_remote_copy_grp(
                    self.rep_secondary.client, copy_group_name).get(
                        'copyPairs', []))
        except exception.VolumeDriverException:
            self.rep_primary.output_log(
                MSG.COPY_PAIR_CANNOT_RETRIEVED, copy_grp=copy_group_name)
            return None
        failback_target_pairs = []
        for pair in pairs:
            if (pair['pvolLdevId'], pair['svolLdevId']) not in rep_pairs:
                utils.output_log(
                    MSG.UNMANAGE_LDEV_EXIST_WARNING,
                    copy_grp=copy_group_name, pvol=pair['pvolLdevId'],
                    svol=pair['svolLdevId'],
                    config_group=self.conf.config_group)
                return None
            if pair.get('svolStatus') != 'SSWS':
                utils.output_log(
                    MSG.INVALID_COPY_GROUP_STATUS,
                    copy_grp=copy_group_name, pvol=pair['pvolLdevId'],
                    pvol_status=pair.get('pvolStatus'),
                    svol=pair['svolLdevId'],
                    svol_status=pair.get('svolStatus'))
                return None
            failback_target_pairs.append(
                (pair['pvolLdevId'], pair['svolLdevId'],
                 pair['replicationType'])
            )
        return failback_target_pairs

    def _failback_copy_group(self, copy_group_name, failback_target_pairs):
        rep_type = failback_target_pairs[0][2]
        try:
            self.rep_primary.client.split_remote_copy_grp(
                self.rep_secondary.client, copy_group_name, rep_type)
            for pvol, svol, _ in failback_target_pairs:
                self._wait_pair_status_change(copy_group_name, pvol, svol,
                                              rep_type, _WAIT_SPLIT)
            self.rep_secondary.client.resync_remote_copy_grp(
                self.rep_primary.client, copy_group_name, rep_type, True, True)
            for pvol, svol, _ in failback_target_pairs:
                self._wait_pair_status_change(copy_group_name, pvol, svol,
                                              rep_type, _WAIT_PAIR)
            self.rep_primary.client.split_remote_copy_grp(
                self.rep_secondary.client, copy_group_name, rep_type)
            for pvol, svol, _ in failback_target_pairs:
                self._wait_pair_status_change(copy_group_name, pvol, svol,
                                              rep_type, _WAIT_PSUS)
            self.rep_primary.client.resync_remote_copy_grp(
                self.rep_secondary.client, copy_group_name, rep_type, True)
            for pvol, svol, _ in failback_target_pairs:
                self._wait_pair_status_change(copy_group_name, pvol, svol,
                                              rep_type, _WAIT_PAIR)
        except exception.VolumeDriverException:
            utils.output_log(
                MSG.FAILOVER_FAILBACK_WARNING,
                direction='back', obj='copy group', operation='failback',
                obj_id=copy_group_name)
            return False
        return True

    def _get_failback_volume_update(self, volumes, failback_success_pairs):
        volume_updates = []
        for volume in volumes:
            volume_update = {'volume_id': volume.id}
            pvol = self.rep_primary.get_ldev(volume)
            svol = self.rep_secondary.get_ldev(volume)
            if (pvol, svol) in [(pldev, sldev) for (pldev, sldev, _) in
                                failback_success_pairs]:
                volume_update['updates'] = {
                    'replication_status': fields.ReplicationStatus.ENABLED}
                for snapshot in volume.snapshots:
                    if _get_ldev_site(snapshot) == _SECONDARY:
                        snapshot.status = fields.SnapshotStatus.ERROR
                        snapshot.save()
            else:
                volume_update['updates'] = {'status': 'error'}
                if volume.replication_status in (
                        fields.ReplicationStatus.FAILED_OVER,
                        fields.ReplicationStatus.FAILOVER_ERROR):
                    volume_update['updates']['replication_status'] = (
                        fields.ReplicationStatus.FAILOVER_ERROR)
            volume_updates.append(volume_update)
        return volume_updates

    def _failback_volume(self, volumes):
        failback_success_pairs = []
        rep_pairs = self._get_rep_pairs(volumes)
        if rep_pairs:
            try:
                remote_copy_grps = (
                    self.rep_primary.client.get_remote_copy_grps(
                        self.rep_secondary.client))
            except Exception:
                msg = self.rep_primary.output_log(
                    MSG.COPY_GROUP_CANNOT_RETRIEVED,
                    config_group=self.conf.config_group)
                raise exception.UnableToFailOver(reason=msg)
            remote_copy_grps = self._extract_my_copy_grps(remote_copy_grps)
            for remote_copy_grp in remote_copy_grps:
                copy_group_name = remote_copy_grp['copyGroupName']
                failback_target_pairs = self._get_failback_target_pairs(
                    copy_group_name, rep_pairs)
                if not failback_target_pairs:
                    continue
                if not (self._failback_copy_group(
                        copy_group_name, failback_target_pairs)):
                    continue
                failback_success_pairs.extend(failback_target_pairs)
        return self._get_failback_volume_update(volumes,
                                                failback_success_pairs)

    def _failover_pair_volume(self, volume):
        if _volume_in_group_replication(volume):
            utils.output_log(
                MSG.GROUP_REPLICATION_UNSUPPORTED_OPERATION,
                operation='Host failover',
                details='volume: %(volume)s, group: %(group)s; use '
                        'group failover for group replication volumes' %
                        {'volume': volume.id, 'group': volume.group_id})
            return False
        pldev, sldev = self._get_ldevs(volume)
        if pldev is None or sldev is None:
            return False
        pair_info = self._get_rep_pair_info(pldev, sldev)
        if not pair_info:
            self.rep_secondary.output_log(
                MSG.NOT_REPLICATION_PAIR_WARNING, volume=volume.id, ldev=sldev)
            return False
        if pair_info['svol_info'][0]['svol_status'] != 'PAIR':
            utils.output_log(
                MSG.NOT_SYNCHRONIZED_WARNING,
                volume=volume.id, pvol=pldev, svol=sldev,
                svol_status=pair_info['svol_info'][0]['svol_status'])
        if pair_info['svol_info'][0]['svol_status'] != 'SSWS':
            copy_group_name = self._create_rep_copy_group_name(pldev)
            extra_specs = self.rep_secondary.get_volume_extra_specs(volume)
            rep_type = _get_rep_type(self, extra_specs)
            self.rep_secondary.client.takeover_remote_copypair(
                copy_group_name, pldev, sldev)
            self._wait_pair_status_change(copy_group_name, pldev, sldev,
                                          rep_type, _WAIT_SSWS)
        return True

    def _failover_volume(self, volumes):
        failover_success_volumes = []
        for volume in volumes:
            if volume.replication_status in (
                    fields.ReplicationStatus.ENABLED,
                    fields.ReplicationStatus.FAILOVER_ERROR):
                try:
                    if self._failover_pair_volume(volume):
                        failover_success_volumes.append(volume)
                except exception.VolumeDriverException:
                    utils.output_log(
                        MSG.FAILOVER_FAILBACK_WARNING,
                        direction='over', obj='volume',
                        operation='failover', obj_id=volume.id)
        return _get_failover_volume_update(volumes, failover_success_volumes)

    def failover(self, volumes, secondary_id=None):
        if ((secondary_id not in (None,
                                  _REP_FAILBACK,
                                  self.rep_secondary_backend_id)) or
                (secondary_id ==
                    _REP_FAILBACK and not self._active_backend_id) or
                (secondary_id != _REP_FAILBACK and self._active_backend_id)):
            direction = 'back' if secondary_id == _REP_FAILBACK else 'over'
            execution_site = (utils.SECONDARY_STR if self._active_backend_id
                              else utils.PRIMARY_STR)
            msg = utils.output_log(
                MSG.INVALID_DESTINATION,
                direction=direction, execution_site=execution_site,
                specified_backend_id=secondary_id,
                defined_backend_id=self.rep_secondary_backend_id)
            raise exception.InvalidReplicationTarget(reason=msg)
        if secondary_id == _REP_FAILBACK:
            try:
                self.rep_primary.do_setup(self.rep_primary.ctxt)
            except Exception:
                msg = self.rep_primary.output_log(
                    MSG.FAILED_FAILBACK, site=utils.PRIMARY_STR)
                raise exception.UnableToFailOver(reason=msg)
            return secondary_id, self._failback_volume(volumes), []
        return (self.rep_secondary_backend_id,
                self._failover_volume(volumes), [])

    def failover_completed(self, secondary_id=None):
        self._active_backend_id = ('' if secondary_id == _REP_FAILBACK else
                                   self.rep_secondary_backend_id)

    def failover_host(self, volumes, secondary_id=None):
        backend_id, volumes_update, groups_update = self.failover(
            volumes, secondary_id)
        self.failover_completed(secondary_id)
        return backend_id, volumes_update, groups_update
