# Copyright 2025 StreamNative, Inc.
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
import queue
import unittest

import oxia
import uuid

from oxia.internal.oxia_container import OxiaContainer


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

            with self.assertRaises(oxia.KeyNotFound):
                client.get(prefix + "/a")

            client.delete_range(prefix + "/c", prefix + "/d")
            with self.assertRaises(oxia.KeyNotFound):
                client.get(prefix + "/c")

            with self.assertRaises(oxia.KeyNotFound):
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
            with self.assertRaises(queue.ShutDown):
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

            client2 = oxia.Client(self.service_address)

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
                client.get(k + '/a', comparison_type=oxia.ComparisonType.LOWER)

            key, val, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.HIGHER)
            self.assertEqual(k + '/c', key)
            self.assertEqual(b'2', val)

            # ---------------------------------------------------------------

            with self.assertRaises(oxia.KeyNotFound):
                client.get(k + '/b')

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
                client.get(k + '/f')

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.InvalidOptions):
                client.put('a', '0', sequence_keys_deltas=[1])

            with self.assertRaises(oxia.InvalidOptions):
                client.put('a', '0', sequence_keys_deltas=[1], partition_key='x', expected_version_id=1)

            key, _ = client.put('a', '0', sequence_keys_deltas=[1], partition_key='x')
            self.assertEqual(f'a-{1:020}', key)

            key, _ = client.put('a', '1', sequence_keys_deltas=[3], partition_key='x')
            self.assertEqual(f'a-{4:020}', key)

            key, _ = client.put('a', '2', sequence_keys_deltas=[1, 6], partition_key='x')
            self.assertEqual(f'a-{5:020}-{6:020}', key)

            with self.assertRaises(oxia.KeyNotFound):
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
        with OxiaContainer() as server:
            client = oxia.Client(server.service_url())

            # ////////////////////////////////////////////////////////////////////////

            for i in range(1, 10):
                key = chr(ord('a') + i)
                value = f'{i:03}'
                client.put(key, value, secondary_indexes={'value-idx': value})

            # ////////////////////////////////////////////////////////////////////////

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
                client.get('999', use_index="value-idx")

            # ////////////////////////////////////////////////////////////////////////

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
                client.get('009', use_index="value-idx", comparison_type=oxia.ComparisonType.HIGHER)

            with self.assertRaises(oxia.KeyNotFound):
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

            with self.assertRaises(oxia.KeyNotFound):
                client.get('999', use_index="value-idx", comparison_type=oxia.ComparisonType.CEILING)


            client.close()


if __name__ == '__main__':
    unittest.main()
