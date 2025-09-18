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

        # # Override with non-ephemeral value
        # _, v2 = client.put(kx, 'y', ephemeral=False)
        # self.assertFalse(v2.is_ephemeral())
        # self.assertEqual(1, v2.modifications_count())
        # vx = v2.version_id()

        client.close()

        # # Re-open
        # client2 = oxia.Client(self.service_address)
        #
        # kr2, va2, vr2 = client2.get(kx)
        # self.assertEqual(1, vr2.modifications_count())
        # self.assertEqual(vx, vr2.version_id())
        # self.assertEqual(va2, b'y')
        # self.assertFalse(vr2.is_ephemeral())
        # self.assertIsNone(vr2.client_identity())
        #
        # client2.close()



if __name__ == '__main__':
    unittest.main()
