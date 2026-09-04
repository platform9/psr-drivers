# Copyright (C) 2026, Platform9 Systems
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
"""Import smoke tests for every class an operator can name in cinder.conf.

test_pf9_group_replication.py imports hbsd_fc, hbsd_common and
hbsd_replication directly, so it exercises the group-replication logic
without ever loading pf9_hitachi_replication.py -- the module whose dotted
path appears in DEPLOYMENT.md and in the operator's volume_driver line. That
module shipped with a doubled "cinder.cinder" package path and a missing
hbsd_iscsi import, and both faults were invisible to the suite because
nothing imported it.

Cinder resolves volume_driver by importlib at manager startup, so an
ImportError here is a backend that never comes up. These tests assert only
that each advertised entry point loads and subclasses the transport driver
it claims.
"""

import importlib

from cinder.tests.unit import test
from cinder.volume.drivers.pf9_hitachi import hbsd_fc
from cinder.volume.drivers.pf9_hitachi import hbsd_iscsi


_DRIVER_MODULE = 'cinder.volume.drivers.pf9_hitachi.pf9_hitachi_replication'

# (class name, expected base) for every class named in the module docstring's
# cinder.conf examples. Adding an entry point without adding it here is the
# regression this file exists to catch.
_ENTRY_POINTS = [
    ('HBSDGroupReplicationFCDriver', hbsd_fc.HBSDFCDriver),
    ('HBSDGroupReplicationISCSIDriver', hbsd_iscsi.HBSDISCSIDriver),
]


class PF9DriverEntryPointTest(test.TestCase):

    def test_driver_module_imports(self):
        """The module an operator names in volume_driver must load."""
        self.assertIsNotNone(importlib.import_module(_DRIVER_MODULE))

    def test_entry_points_resolve_and_subclass_transport(self):
        mod = importlib.import_module(_DRIVER_MODULE)
        for name, expected_base in _ENTRY_POINTS:
            cls = getattr(mod, name, None)
            self.assertIsNotNone(
                cls, 'volume_driver entry point %s.%s does not resolve'
                     % (_DRIVER_MODULE, name))
            self.assertTrue(
                issubclass(cls, expected_base),
                '%s must subclass %s' % (name, expected_base.__name__))

    def test_transport_entry_points_import(self):
        """The stock classes are the supported volume_driver values.

        Group replication now activates inside HBSDREPLICATION on a group-type
        extra spec rather than through a dedicated driver class, so these two
        are what cinder.conf should name.
        """
        for mod_name, cls_name in [
                ('cinder.volume.drivers.pf9_hitachi.hbsd_fc',
                 'HBSDFCDriver'),
                ('cinder.volume.drivers.pf9_hitachi.hbsd_iscsi',
                 'HBSDISCSIDriver')]:
            mod = importlib.import_module(mod_name)
            self.assertIsNotNone(getattr(mod, cls_name, None))
