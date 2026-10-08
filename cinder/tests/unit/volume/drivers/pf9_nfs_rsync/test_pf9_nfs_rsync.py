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

"""Unit tests for the PF9 NFS + rsync replication driver."""

from cinder.tests.unit import test
from cinder.volume import configuration as conf
from cinder.volume.drivers import nfs
from cinder.volume.drivers.pf9_nfs_rsync import pf9_nfs_rsync


class PF9NFSRsyncStatsTestCase(test.TestCase):

    def setUp(self):
        super().setUp()
        self.driver = pf9_nfs_rsync.PF9NFSRsyncDriver(
            configuration=conf.Configuration(None))

    def _update_stats(self):
        def nfs_stats(drv):
            drv._stats = {'volume_backend_name': 'psr-dr',
                          'pools': [{'pool_name': 'a'}, {'pool_name': 'b'}]}
        self.mock_object(nfs.NfsDriver, '_update_volume_stats',
                         nfs_stats)
        self.driver._update_volume_stats()
        return self.driver._stats

    def test_update_volume_stats_reports_cg_replication(self):
        stats = self._update_stats()

        self.assertTrue(stats['consistent_group_replication_enabled'])
        for pool in stats['pools']:
            self.assertTrue(pool['consistent_group_replication_enabled'])

    def test_update_volume_stats_keeps_volume_replication_keys(self):
        stats = self._update_stats()

        for scope in [stats] + stats['pools']:
            self.assertTrue(scope['replication_enabled'])
            self.assertEqual(['async'], scope['replication_type'])
            self.assertEqual(['pf9-nfs-secondary'],
                             scope['replication_targets'])
