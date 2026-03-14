from petscan import service_store as store
from tests.service_test_support import ServiceTestCase


class ServiceStoreTests(ServiceTestCase):
    def test_get_psid_lock_returns_same_lock_for_same_psid(self):
        first = store.get_psid_lock(123456)
        second = store.get_psid_lock(123456)
        self.assertIs(first, second)

    def test_get_psid_lock_uses_bounded_lock_stripes(self):
        stripe_count = len(store._LOCK_STRIPES)
        base_psid = 42

        first = store.get_psid_lock(base_psid)
        colliding = store.get_psid_lock(base_psid + stripe_count)
        self.assertIs(first, colliding)

        unique_lock_ids = {
            id(store.get_psid_lock(psid)) for psid in range(stripe_count * 4)
        }
        self.assertEqual(len(unique_lock_ids), stripe_count)
