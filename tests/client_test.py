# Copyright 2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools
import time
import unittest
import uuid

import oxia
from oxia.internal.compare import compare_with_slash
from oxia.internal.oxia_container import OxiaContainer
from oxia.internal.proto.io.streamnative.oxia import proto as pb


def new_key():
    return f"/test-{uuid.uuid4().hex}"

class OxiaClientTestCase(unittest.TestCase):

    def test_client(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(), namespace="default")
            prefix = new_key()
            ka = prefix + "/a"
            ka, va = client.put(ka, 'v-a', expected_version_id=oxia.EXPECTED_RECORD_DOES_NOT_EXIST)
            self.assertEqual(0, va.modifications_count())

            k1, v1, ve1 = client.get(ka)
            self.assertEqual(va.version_id(), ve1.version_id())
            self.assertEqual(0, ve1.modifications_count())
            self.assertEqual(ka, k1)
            self.assertEqual(b'v-a', v1)

            kc = prefix + "/c"
            kc, vc = client.put(kc, 'v-c', expected_version_id=oxia.EXPECTED_RECORD_DOES_NOT_EXIST)
            self.assertEqual(0, vc.modifications_count())

            kc, vc = client.put(kc, 'v-c', expected_version_id=vc.version_id())
            self.assertEqual(1, vc.modifications_count())

            list_res = client.list(prefix + "/y", prefix + "/z")
            self.assertEqual(0, len(list_res))

            list_res = client.list(prefix + "/a", prefix + "/d")
            self.assertEqual([prefix + '/a', prefix + '/c'], list_res)

            dr = client.delete(prefix + "/a", expected_version_id=va.version_id())
            self.assertEqual(True, dr)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(prefix + "/a")

            client.delete_range(prefix + "/c", prefix + "/d")
            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(prefix + "/c")

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(prefix + "/d")

            client.close()

    def test_notification(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())

            notifications = client.get_notifications()

            prefix = new_key()
            ka = prefix + "/a"
            _, s1 = client.put(ka, '0')

            n = next(notifications)
            self.assertIs(oxia.NotificationType.KEY_CREATED, n.notification_type())
            self.assertEqual(ka, n.key())
            self.assertEqual(s1.version_id(), n.version_id())

            _, s2 = client.put(ka, '1')

            n = next(notifications)
            self.assertIs(oxia.NotificationType.KEY_MODIFIED, n.notification_type())
            self.assertEqual(ka, n.key())
            self.assertEqual(s2.version_id(), n.version_id())

            kb = prefix + "/b"
            _, s3 = client.put(kb, '0')

            client.delete(ka)

            n = next(notifications)
            self.assertIs(oxia.NotificationType.KEY_CREATED, n.notification_type())
            self.assertEqual(kb, n.key())
            self.assertEqual(s3.version_id(), n.version_id())

            n = next(notifications)
            self.assertIs(oxia.NotificationType.KEY_DELETED, n.notification_type())
            self.assertEqual(ka, n.key())
            self.assertEqual(0, n.version_id())

            # Create a 2nd notifications channel
            # This will only receive new updates
            notifications2 = client.get_notifications()

            kx = prefix + "/x"
            _, s4, = client.put(kx, '1')

            n = next(notifications)
            self.assertIs(oxia.NotificationType.KEY_CREATED, n.notification_type())
            self.assertEqual(kx, n.key())
            self.assertEqual(s4.version_id(), n.version_id())

            n = next(notifications2)
            self.assertIs(oxia.NotificationType.KEY_CREATED, n.notification_type())
            self.assertEqual(kx, n.key())
            self.assertEqual(s4.version_id(), n.version_id())

            notifications.close()
            with self.assertRaises(StopIteration):
                next(notifications)

            notifications2.close()
            client.close()

    def test_sessions(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(), namespace="default", session_timeout_ms=5_000)

            prefix = new_key()
            kx = prefix + "/x"
            _, v1 = client.put(kx, 'x', ephemeral=True)
            self.assertTrue(v1.is_ephemeral())
            self.assertEqual(0, v1.modifications_count())
            vx = v1.version_id()

            kr1, va1, vr1 = client.get(kx)
            self.assertEqual(0, vr1.modifications_count())
            self.assertEqual(vx, vr1.version_id())

            client.close()

            client2 = oxia.Client(server.service_url())

            with self.assertRaises(oxia.ex.KeyNotFound):
                client2.get(kx)

            client2.close()

    def test_override_ephemerals(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(), namespace="default", session_timeout_ms=5_000)

            prefix = new_key()
            kx = prefix + "/x"
            _, v1 = client.put(kx, 'x', ephemeral=True)
            self.assertTrue(v1.is_ephemeral())
            self.assertEqual(0, v1.modifications_count())

            # Override with non-ephemeral value
            _, v2 = client.put(kx, 'y', ephemeral=False)
            self.assertFalse(v2.is_ephemeral())
            self.assertEqual(1, v2.modifications_count())
            vx = v2.version_id()

            client.close()

            # Re-open
            client2 = oxia.Client(server.service_url())

            kr2, va2, vr2 = client2.get(kx)
            self.assertEqual(1, vr2.modifications_count())
            self.assertEqual(vx, vr2.version_id())
            self.assertEqual(va2, b'y')
            self.assertFalse(vr2.is_ephemeral())
            self.assertIsNone(vr2.client_identity())

            client2.close()

    def test_client_identity(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(),
                                 client_identifier="client-1")

            k = new_key()
            k1, v1 = client.put(k, 'x', ephemeral=True)
            self.assertTrue(v1.is_ephemeral())
            self.assertEqual('client-1', v1.client_identity())

            client2 = oxia.Client(server.service_url(),
                                  client_identifier="client-2")

            kr2, va2, vr2 = client2.get(k)
            self.assertEqual(0, vr2.modifications_count())
            self.assertEqual(va2, b'x')
            self.assertTrue(vr2.is_ephemeral())
            self.assertEqual('client-1', vr2.client_identity())

            # Override
            k2, v2 = client2.put(k, 'v2', ephemeral=True)
            self.assertTrue(v2.is_ephemeral())
            self.assertEqual('client-2', v2.client_identity())

            client.close()
            client2.close()

    def test_sessions_notifications(self):
        with OxiaContainer() as server:
            client1 = oxia.Client(server.service_url(),
                                  client_identifier="client-1")

            client2 = oxia.Client(server.service_url(),
                                  client_identifier="client-2")

            notifications2 = client2.get_notifications()

            k = new_key()
            k1, v1 = client1.put(k, 'x', ephemeral=True)

            n1 = next(notifications2)
            self.assertIs(oxia.NotificationType.KEY_CREATED, n1.notification_type())
            self.assertEqual(k, n1.key())
            self.assertEqual(v1.version_id(), n1.version_id())

            client1.close()

            n2 = next(notifications2)
            self.assertIs(oxia.NotificationType.KEY_DELETED, n2.notification_type())
            self.assertEqual(k, n2.key())

            client2.close()

    def test_floor_ceiling_get(self):
        with OxiaContainer(shards=10) as server:
            client = oxia.Client(server.service_url())
            k = new_key()

            # The cross-shard floor/ceiling merge in Client.get() is the
            # code path under test. If all five keys happen to hash to the
            # same shard, the test degrades to a single-shard scenario and
            # silently stops covering that merge. Assert the spread up front.
            sd = client._service_discovery
            shards_used = {
                sd.get_shard(k + suffix).shard
                for suffix in ['/a', '/c', '/d', '/e', '/g']
            }
            self.assertGreater(
                len(shards_used), 1,
                f"test keys must span multiple shards to exercise cross-shard logic; "
                f"got {shards_used}")

            client.put(k + "/a", '0')
            # client.put(k + "/b", '1') # Skipped intentionally
            client.put(k + "/c", '2')
            client.put(k + "/d", '3')
            client.put(k + "/e", '4')
            # client.put(k + "/f", '5') # Skipped intentionally
            client.put(k + "/g", '6')

            key, val, _ = client.get(k + '/a')
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.EQUAL)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(k + '/a', comparison_type=oxia.ComparisonType.LOWER)

            key, val, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            # ---------------------------------------------------------------

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(k + '/b')

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(k + '/b', comparison_type=oxia.ComparisonType.EQUAL)

            key, val, _ = client.get(k + '/b', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/b', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/b', comparison_type=oxia.ComparisonType.LOWER)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/b', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            # ---------------------------------------------------------------

            key, val, _ = client.get(k + '/c')
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/c', comparison_type=oxia.ComparisonType.EQUAL)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/c', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/c', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/c', comparison_type=oxia.ComparisonType.LOWER)
            self.assertEqual(k + '/a', key)
            self.assertEqual(b'0', val)

            key, val, _ = client.get(k + '/c', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            # ---------------------------------------------------------------

            key, val, _ = client.get(k + '/d')
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            key, val, _ = client.get(k + '/d', comparison_type=oxia.ComparisonType.EQUAL)
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            key, val, _ = client.get(k + '/d', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            key, val, _ = client.get(k + '/d', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            key, val, _ = client.get(k + '/d', comparison_type=oxia.ComparisonType.LOWER)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            key, val, _ = client.get(k + '/d', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            # ---------------------------------------------------------------

            key, val, _ = client.get(k + '/e')
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/e', comparison_type=oxia.ComparisonType.EQUAL)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/e', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/e', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/e', comparison_type=oxia.ComparisonType.LOWER)
            self.assertEqual(k + '/d', key)
            self.assertEqual(b'3', val)

            key, val, _ = client.get(k + '/e', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/g', key)
            self.assertEqual(b'6', val)

            # ---------------------------------------------------------------

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(k + '/f')

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get(k + '/f', comparison_type=oxia.ComparisonType.EQUAL)

            key, val, _ = client.get(k + '/f', comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/f', comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual(k + '/g', key)
            self.assertEqual(b'6', val)

            key, val, _ = client.get(k + '/f', comparison_type=oxia.ComparisonType.LOWER)
            self.assertEqual(k + '/e', key)
            self.assertEqual(b'4', val)

            key, val, _ = client.get(k + '/f', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/g', key)
            self.assertEqual(b'6', val)


    def test_partition_routing(self):
        with OxiaContainer(shards=10) as server:
            client = oxia.Client(server.service_url())
            client.put('a', '0', partition_key='x')

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('/a')

            key, value, _ = client.get('a', partition_key='x')
            self.assertEqual('a', key)
            self.assertEqual(b'0', value)

            client.put("a", '0', partition_key='x')
            client.put("b", '1', partition_key='x')
            client.put("c", '2', partition_key='x')
            client.put("d", '3', partition_key='x')
            client.put("e", '4', partition_key='x')
            client.put("f", '5', partition_key='x')
            client.put("g", '6', partition_key='x')

            # Listing must yield the same results
            keys = client.list('a',  'd')
            self.assertEqual(['a', 'b', 'c'], keys)

            keys = client.list('a', 'd', partition_key='x')
            self.assertEqual(['a', 'b', 'c'], keys)

            # Searching with wrong partition-key will return empty list
            keys = client.list('a', 'd', partition_key='wrong-partition')
            self.assertEqual([], keys)

            # Delete with wrong partition key would fail
            res = client.delete("g", partition_key='wrong-partition')
            self.assertFalse(res)

            res = client.delete("g", partition_key='x')
            self.assertTrue(res)

            # Get tests
            key, val, _ = client.get("a", comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual('b', key)
            self.assertEqual(b'1', val)

            key, val, _ = client.get("a", comparison_type=oxia.ComparisonType.HIGHER, partition_key='x')
            self.assertEqual('b', key)
            self.assertEqual(b'1', val)

            # Delete with wrong partition key would fail to delete all keys
            client.delete_range("c", "e", partition_key='wrong-partition')

            keys = client.list('c', 'f')
            self.assertEqual(['c', 'd', 'e'], keys)

            client.delete_range("c", "e", partition_key='x')

            keys = client.list('c', 'f')
            self.assertEqual(['e'], keys)

            client.close()

    def test_sequential_keys(self):
        with OxiaContainer(shards=2) as server:
            client = oxia.Client(server.service_url())

            with self.assertRaises(oxia.ex.InvalidOptions):
                client.put('a', '0', sequence_keys_deltas=[1])

            with self.assertRaises(oxia.ex.InvalidOptions):
                client.put('a', '0', sequence_keys_deltas=[1], partition_key='x', expected_version_id=1)

            key, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual(f'a-{1:020}', key)

            key, _ = client.put('a', '1', sequence_keys_deltas=[3], partition_key='x')
            self.assertEqual(f'a-{4:020}', key)

            key, _ = client.put('a', '2', sequence_keys_deltas=[1, 6], partition_key='x')
            self.assertEqual(f'a-{5:020}-{6:020}', key)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('a', partition_key='x')

            _, val, _ = client.get(f'a-{1:020}', partition_key='x')
            self.assertEqual(b'0', val)

            _, val, _ = client.get(f'a-{4:020}', partition_key='x')
            self.assertEqual(b'1', val)

            _, val, _ = client.get(f'a-{5:020}-{6:020}', partition_key='x')
            self.assertEqual(b'2', val)

            client.close()

    def test_range_scan(self):
        with OxiaContainer(shards=2) as server:
            client = oxia.Client(server.service_url())
            client.put('a', '0')
            client.put('b', '1')
            client.put('c', '2')
            client.put('d', '3')
            client.put('e', '4')
            client.put('f', '5')
            client.put('g', '6')

            it = client.range_scan('b', 'f')
            key, val, _ = next(it)
            self.assertEqual('b', key)
            self.assertEqual(b'1', val)

            key, val, _ = next(it)
            self.assertEqual('c', key)
            self.assertEqual(b'2', val)

            key, val, _ = next(it)
            self.assertEqual('d', key)
            self.assertEqual(b'3', val)

            key, val, _ = next(it)
            self.assertEqual('e', key)
            self.assertEqual(b'4', val)

            with self.assertRaises(StopIteration):
                next(it)

            client.close()

    def test_range_scan_on_partition(self):
        with OxiaContainer(shards=3) as server:
            client = oxia.Client(server.service_url())
            client.put('a', '0', partition_key='x')
            client.put('b', '1', partition_key='x')
            client.put('c', '2', partition_key='x')
            client.put('d', '3', partition_key='x')
            client.put('e', '4', partition_key='x')
            client.put('f', '5', partition_key='x')
            client.put('g', '6', partition_key='x')

            it = client.range_scan('b', 'f', partition_key='x')
            key, val, _ = next(it)
            self.assertEqual('b', key)
            self.assertEqual(b'1', val)

            key, val, _ = next(it)
            self.assertEqual('c', key)
            self.assertEqual(b'2', val)

            key, val, _ = next(it)
            self.assertEqual('d', key)
            self.assertEqual(b'3', val)

            key, val, _ = next(it)
            self.assertEqual('e', key)
            self.assertEqual(b'4', val)

            with self.assertRaises(StopIteration):
                next(it)

            client.close()

    def test_sequence_ordering(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())
            for i in range(100):
                key, _ = client.put('a', '0', partition_key='x', sequence_keys_deltas=[1])
                self.assertEqual(f'a-{i+1:020}', key)

    def test_get_without_value(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())

            client.put('a', '0')
            key, val, _ = client.get('a', include_value=False)
            self.assertEqual('a', key)
            self.assertIsNone(val)

            client.close()

    def test_secondary_indexes(self):
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())

            # ////////////////////////////////////////////////////////////////////////

            for i in range(10):
                key = chr(ord('a') + i)
                value = str(i)
                client.put(key, value, secondary_indexes={'value-idx': value})

            # ////////////////////////////////////////////////////////////////////////

            l = client.list("1", "4", use_index="value-idx")
            self.assertEqual(['b', 'c', 'd'], l)

            # ////////////////////////////////////////////////////////////////////////

            it = client.range_scan('1', '4', use_index="value-idx")

            idx = 1
            for k, v, _ in it:
                self.assertEqual(chr(ord('a') + idx), k)
                self.assertEqual(str(idx).encode(), v)
                idx += 1

            self.assertEqual(4, idx)

            client.close()

    def test_secondary_indexes_get(self):
        with OxiaContainer(shards=10) as server:
            client = oxia.Client(server.service_url())

            # ////////////////////////////////////////////////////////////////////////

            for i in range(1, 10):
                key = chr(ord('a') + i)
                value = f'{i:03}'
                client.put(key, value, secondary_indexes={'value-idx': value})

            # ////////////////////////////////////////////////////////////////////////

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('000', use_index="value-idx")

            gk, gv, _ = client.get('001', use_index="value-idx")
            self.assertEqual('b', gk)
            self.assertEqual(b'001', gv)

            gk, gv, _ = client.get('005', use_index="value-idx")
            self.assertEqual('f', gk)
            self.assertEqual(b'005', gv)

            gk, gv, _ = client.get('009', use_index="value-idx")
            self.assertEqual('j', gk)
            self.assertEqual(b'009', gv)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('999', use_index="value-idx")

            # ////////////////////////////////////////////////////////////////////////

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('000', use_index="value-idx", comparison_type=oxia.ComparisonType.FLOOR)

            gk, gv, _ = client.get('001', use_index="value-idx", comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual('b', gk)
            self.assertEqual(b'001', gv)

            gk, gv, _ = client.get('005', use_index="value-idx", comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual('f', gk)
            self.assertEqual(b'005', gv)

            gk, gv, _ = client.get('009', use_index="value-idx", comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual('j', gk)
            self.assertEqual(b'009', gv)

            gk, gv, _ = client.get('999', use_index="value-idx", comparison_type=oxia.ComparisonType.FLOOR)
            self.assertEqual('j', gk)
            self.assertEqual(b'009', gv)

            # ////////////////////////////////////////////////////////////////////////

            gk, gv, _ = client.get('000', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual('b', gk)
            self.assertEqual(b'001', gv)

            gk, gv, _ = client.get('001', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual('c', gk)
            self.assertEqual(b'002', gv)

            gk, gv, _ = client.get('005', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual('g', gk)
            self.assertEqual(b'006', gv)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('009', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('999', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)

            # ////////////////////////////////////////////////////////////////////////

            gk, gv, _ = client.get('000', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual('b', gk)
            self.assertEqual(b'001', gv)

            gk, gv, _ = client.get('001', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual('b', gk)
            self.assertEqual(b'001', gv)

            gk, gv, _ = client.get('005', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual('f', gk)
            self.assertEqual(b'005', gv)

            gk, gv, _ = client.get('009', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)
            self.assertEqual('j', gk)
            self.assertEqual(b'009', gv)

            with self.assertRaises(oxia.ex.KeyNotFound):
                client.get('999', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)


            client.close()

    def test_sequence_updates(self):
        with OxiaContainer(shards=10) as server:
            client = oxia.Client(server.service_url())

            # ////////////////////////////////////////////////////////////////////////
            with self.assertRaises(oxia.ex.InvalidOptions):
                client.get_sequence_updates("a")

            # Subscribe-then-immediately-close, before any data exists.
            gs1 = client.get_sequence_updates("a", partition_key="x")
            gs1.close()

            k1, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual('a-%020d' % 1, k1)
            k2, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual('a-%020d' % 2, k2)

            # A fresh subscription must receive the current highest sequence as
            # the first event (not wait for the next write).
            gs2 = client.get_sequence_updates("a", partition_key="x")
            self.assertEqual(k2, next(gs2))
            gs2.close()

            k3, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual('a-%020d' % 3, k3)

            # After close, the iterator must be exhausted.
            with self.assertRaises(StopIteration):
                next(gs2)

            gs3 = client.get_sequence_updates("a", partition_key="x")
            self.assertEqual(k3, next(gs3))

            k4, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual('a-%020d' % 4, k4)
            self.assertEqual(k4, next(gs3))

            gs3.close()
            client.close()

    def test_session_not_found(self):
        """Exercises the SESSION_DOES_NOT_EXIST server status → SessionNotFound
        client exception path. Reaches into internals to forcibly close a
        session server-side, then does a put that reuses the stale in-memory
        session object — the server should reject it."""
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(),
                                 namespace="default",
                                 session_timeout_ms=5_000)
            try:
                # Pin both puts to the same shard via partition_key, so the
                # second put reuses the first put's session object rather than
                # creating a fresh one on a different shard.
                pk = "t7-partition"
                client.put("/t7-a", 'v1', ephemeral=True, partition_key=pk)

                # Grab the live session and its server-side ID.
                sessions_by_shard = client._session_manager.sessions_by_shard
                self.assertEqual(1, len(sessions_by_shard),
                                 "expected exactly one session to exist")
                shard, session = next(iter(sessions_by_shard.items()))
                stale_session_id = session.session_id()

                # Close server-side via a raw stub. The client's in-memory
                # Session object still thinks it's alive.
                stub = client._service_discovery.get_stub(shard)
                stub.close_session(pb.CloseSessionRequest(
                    shard=shard, session_id=stale_session_id))
                self.assertFalse(session.is_closed(),
                                 "client should still think session is alive")

                # Attempting another ephemeral put on the same shard reuses
                # the stale session and must surface as SessionNotFound.
                with self.assertRaises(oxia.ex.SessionNotFound):
                    client.put("/t7-b", 'v2',
                               ephemeral=True, partition_key=pk)
            finally:
                client.close()

    def test_delete_ephemeral_after_session_closed(self):
        """When the client that created an ephemeral record closes, the server
        should remove the record. A second client observing the key must see
        KeyNotFound on get and False on delete."""
        with OxiaContainer() as server:
            client1 = oxia.Client(server.service_url(),
                                  session_timeout_ms=5_000,
                                  client_identifier="client-1")
            k = new_key()
            _, v = client1.put(k, 'v', ephemeral=True)
            self.assertTrue(v.is_ephemeral())

            # A second client can observe the ephemeral record while client1 is alive.
            client2 = oxia.Client(server.service_url(),
                                  client_identifier="client-2")
            rk, rv, _ = client2.get(k)
            self.assertEqual(k, rk)
            self.assertEqual(b'v', rv)

            # Closing client1 closes its session; the server removes the ephemeral.
            client1.close()

            # Give the server a moment to propagate the session-scoped delete.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                try:
                    client2.get(k)
                except oxia.ex.KeyNotFound:
                    break
                time.sleep(0.05)
            else:
                self.fail(f"ephemeral key {k} was not removed within 5s of client close")

            # delete() on a non-existent key returns False, not True, not raising.
            self.assertFalse(client2.delete(k))
            with self.assertRaises(oxia.ex.KeyNotFound):
                client2.get(k)

            client2.close()

    def test_secondary_index_equal_get_returns_primary_key(self):
        """Secondary-index lookups with the default (EQUAL) comparison must
        return the primary key of the matched record — not the secondary key
        the caller passed in."""
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())
            try:
                client.put('alpha', 'VALUE_007',
                           secondary_indexes={'idx': '007'})

                # EQUAL (default) — the returned key must be the primary key.
                key, val, _ = client.get('007', use_index='idx')
                self.assertEqual('alpha', key,
                                 "returned key must be the primary key 'alpha', "
                                 "not the secondary lookup value '007'")
                self.assertEqual(b'VALUE_007', val)

                # Explicit EQUAL, same expectation.
                key, val, _ = client.get(
                    '007', use_index='idx',
                    comparison_type=oxia.ComparisonType.EQUAL)
                self.assertEqual('alpha', key)
                self.assertEqual(b'VALUE_007', val)
            finally:
                client.close()

    def test_session_survives_beyond_timeout_window(self):
        """A session_timeout_ms of 3000ms (the lowest value where the default
        heartbeat cadence has a comfortable margin) must result in an
        ephemeral record surviving past that window, proving the heartbeat
        thread is actually keeping the session alive.

        Note: 2000ms is the minimum accepted by the default heartbeat path,
        but at exactly 2000ms the default resolves heartbeat_interval_ms to
        1999ms — a 1ms margin that's fragile under any scheduling jitter.
        This test deliberately uses 3000ms to exercise the mechanism without
        depending on sub-millisecond timing."""
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(),
                                 session_timeout_ms=3_000)
            try:
                k = new_key()
                _, v = client.put(k, 'v', ephemeral=True)
                self.assertTrue(v.is_ephemeral())

                # Wait longer than the session timeout. The record must
                # still be there — the heartbeat thread has been keeping
                # the session alive.
                time.sleep(4.5)

                rk, rv, _ = client.get(k)
                self.assertEqual(k, rk)
                self.assertEqual(b'v', rv)
            finally:
                client.close()

    def test_session_timeout_below_floor_rejected(self):
        """A session_timeout_ms below the 2000ms floor must be rejected when
        the first session is actually created (the SessionManager is lazy, so
        the error surfaces on the first ephemeral put rather than at
        Client construction)."""
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url(),
                                 session_timeout_ms=1_999)
            try:
                with self.assertRaises(ValueError):
                    client.put(new_key(), 'v', ephemeral=True)
            finally:
                # close() must work even though we never successfully created a session
                client.close()

    def test_list_empty_range_multi_shard(self):
        """A list() with no matching keys must return [] on a multi-shard setup
        rather than errors — regression guard for the cross-shard merge path."""
        with OxiaContainer(shards=5) as server:
            client = oxia.Client(server.service_url())
            try:
                prefix = new_key()
                client.put(prefix + '/a', '0')
                client.put(prefix + '/b', '1')
                client.put(prefix + '/c', '2')

                # Range that falls entirely after the written keys.
                self.assertEqual([], client.list(prefix + '/y', prefix + '/z'))

                # Range that doesn't intersect anything the test wrote.
                self.assertEqual(
                    [],
                    client.list('ZZZ_nonexistent_a', 'ZZZ_nonexistent_z'))
            finally:
                client.close()

    def test_list_hierarchical_sort_consistency_across_shards(self):
        """The server's list() sort order must match the client's
        compare_with_slash, otherwise the cross-shard merge in Client.list()
        would silently produce wrong orderings.

        Uses keys chosen so that hierarchical and natural (codepoint-wise)
        orderings differ — asserted at the end so this test can't silently
        pass if the two were equivalent.

        Note: range_scan is deliberately NOT tested here. The server uses a
        different ordering for range_scan than for list (segment-count priority
        rather than compare_with_slash), which means the client's cross-shard
        heapq.merge produces incorrect output for range_scan. That is tracked
        as a separate deferred test in tests/deferred_test.py."""
        with OxiaContainer(shards=3) as server:
            client = oxia.Client(server.service_url())
            try:
                prefix = new_key()
                suffixes = [
                    '/a',
                    '/aa',
                    '/a/b',
                    '/a/b/c',
                    '/aa/a',
                    '/b',
                    '/b/a',
                    '/b/ab',
                ]
                keys = [prefix + s for s in suffixes]
                for k in keys:
                    client.put(k, 'v')

                # Lower bound: `prefix + '/'` filters to only this test's keys
                # (hierarchically, any other-prefix key falls outside).
                # Upper bound: 4 '/~' segments — deep enough to hierarchically
                # exceed every 4-segment key we wrote, since '~' is larger
                # than every letter in our suffixes.
                lo = prefix + '/'
                hi = prefix + '/~/~/~/~'

                expected_hier = sorted(
                    keys, key=functools.cmp_to_key(compare_with_slash))

                actual_list = client.list(lo, hi)
                self.assertEqual(expected_hier, actual_list)

                # Sanity check: the chosen key set MUST produce a different
                # order under Python's default sorted(). If a future refactor
                # made hierarchical = natural, this test would otherwise
                # silently pass. Fail loudly if that happens.
                natural = sorted(keys)
                self.assertNotEqual(
                    natural, expected_hier,
                    "this test requires a key set where hierarchical and "
                    "natural orderings differ; pick different suffixes")
            finally:
                client.close()


if __name__ == '__main__':
    unittest.main()
