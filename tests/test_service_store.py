import json
from datetime import datetime, timedelta, timezone

from petscan import service_store as store
from tests.service_test_support import ServiceTestCase


class ServiceStoreTests(ServiceTestCase):
    @staticmethod
    def _write_meta(psid: int, loaded_at: str) -> None:
        store_path = store.store_path(psid)
        store_path.mkdir(parents=True, exist_ok=True)
        payload = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": loaded_at,
            "structure": {"row_count": 1, "field_count": 0, "fields": []},
        }
        store.meta_path(psid).write_text(json.dumps(payload), encoding="utf-8")

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

    def test_prune_expired_stores_removes_only_stores_older_than_one_day(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        stale_psid = 998001
        fresh_psid = 998002
        self._cleanup_store(stale_psid)
        self._cleanup_store(fresh_psid)

        self._write_meta(stale_psid, (now - timedelta(hours=25)).isoformat())
        self._write_meta(fresh_psid, (now - timedelta(hours=23)).isoformat())

        removed = store.prune_expired_stores(now=now)

        self.assertIn(stale_psid, removed)
        self.assertNotIn(fresh_psid, removed)
        self.assertFalse(store.store_path(stale_psid).exists())
        self.assertTrue(store.store_path(fresh_psid).exists())

    def test_prune_expired_stores_respects_excluded_psids(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        stale_psid = 998003
        self._cleanup_store(stale_psid)
        self._write_meta(stale_psid, (now - timedelta(hours=26)).isoformat())

        removed = store.prune_expired_stores(now=now, exclude_psids=[stale_psid])

        self.assertNotIn(stale_psid, removed)
        self.assertTrue(store.store_path(stale_psid).exists())
