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


def new_key():
    return f"/test-{uuid.uuid4().hex}"

class OxiaClientTestCase(unittest.TestCase):
    service_address = "localhost:6648"

    def test_client(self):
        client = oxia.Client(self.service_address, namespace="default")
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
        client = oxia.Client(self.service_address, namespace="default")

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
        client = oxia.Client(self.service_address, namespace="default", session_timeout_ms=5_000)

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
        client = oxia.Client(self.service_address, namespace="default", session_timeout_ms=5_000)

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
        client2 = oxia.Client(self.service_address)

        kr2, va2, vr2 = client2.get(kx)
        self.assertEqual(1, vr2.modifications_count())
        self.assertEqual(vx, vr2.version_id())
        self.assertEqual(va2, b'y')
        self.assertFalse(vr2.is_ephemeral())
        self.assertIsNone(vr2.client_identity())

        client2.close()

    def test_client_identity(self):
        client = oxia.Client(self.service_address,
                             client_identifier="client-1")

        k = new_key()
        k1, v1 = client.put(k, 'x', ephemeral=True)
        self.assertTrue(v1.is_ephemeral())
        self.assertEqual('client-1', v1.client_identity())

        client2 = oxia.Client(self.service_address,
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
        client1 = oxia.Client(self.service_address,
                             client_identifier="client-1")

        client2 = oxia.Client(self.service_address,
                              client_identifier="client-2")

        notifications2 = client2.get_notifications()

        k = new_key()
        k1, v1 = client1.put(k, 'x', ephemeral=True)

        client2 = oxia.Client(self.service_address,
                              client_identifier="client-2")

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
        client = oxia.Client(self.service_address)
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

        key, _, _ = client.get(k + '/a', comparison_type=oxia.ComparisonType.LOWER)
        self.assertFalse(key.startswith(k)) # Not found

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
        client = oxia.Client(self.service_address)
        k = new_key()
        client.put(k + "/a", '0', partition_key='x')

        with self.assertRaises(oxia.KeyNotFound):
            client.get(k + '/a')

        key, value, _ = client.get(k + '/a', partition_key='x')
        self.assertEqual(k + '/a', key)
        self.assertEqual(b'0', value)

        client.put(k + "/a", '0', partition_key='x')
        client.put(k + "/b", '1', partition_key='x')
        client.put(k + "/c", '2', partition_key='x')
        client.put(k + "/d", '3', partition_key='x')
        client.put(k + "/e", '4', partition_key='x')
        client.put(k + "/f", '5', partition_key='x')
        client.put(k + "/g", '6', partition_key='x')

        # Listing must yield the same results
        keys = client.list(k + '/a', k + '/d')
        self.assertEqual([k + '/a', k + '/b', k + 'c'], keys)
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, []string{"a", "b", "c"}, keys)
        #
        # 	keys, err = client.List(ctx, "a", "d", PartitionKey("x"))
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, []string{"a", "b", "c"}, keys)
        #
        # 	// Searching with wrong partition-key will return empty list
        # 	keys, err = client.List(ctx, "a", "d", PartitionKey("wrong-partition-key"))
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, []string{}, keys)
        #
        # 	// Delete with wrong partition key would fail
        # 	err = client.Delete(ctx, "g", PartitionKey("wrong-partition-key"))
        # 	assert.ErrorIs(t, err, ErrKeyNotFound)
        #
        # 	err = client.Delete(ctx, "g", PartitionKey("x"))
        # 	assert.NoError(t, err)
        #
        # 	// Get tests
        # 	key, value, _, err = client.Get(ctx, "a", ComparisonHigher())
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, "b", key)
        # 	assert.Equal(t, "1", string(value))
        #
        # 	key, value, _, err = client.Get(ctx, "a", ComparisonHigher(), PartitionKey("x"))
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, "b", key)
        # 	assert.Equal(t, "1", string(value))
        #
        # 	key, value, _, err = client.Get(ctx, "a", ComparisonHigher(), PartitionKey("wrong-partition-key"))
        # 	assert.NoError(t, err)
        # 	assert.NotEqual(t, "b", key)
        # 	assert.NotEqual(t, "1", string(value))
        #
        # 	// Delete with wrong partition key would fail to delete all keys
        # 	err = client.DeleteRange(ctx, "c", "e", PartitionKey("wrong-partition-key"))
        # 	assert.NoError(t, err)
        #
        # 	keys, err = client.List(ctx, "c", "f")
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, []string{"c", "d", "e"}, keys)
        #
        # 	err = client.DeleteRange(ctx, "c", "e", PartitionKey("x"))
        # 	assert.NoError(t, err)
        #
        # 	keys, err = client.List(ctx, "c", "f")
        # 	assert.NoError(t, err)
        # 	assert.Equal(t, []string{"e"}, keys)

if __name__ == '__main__':
    unittest.main()
