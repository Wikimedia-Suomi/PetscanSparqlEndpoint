import gzip
import json
import shutil
from pathlib import Path
from typing import Any, Dict

from django.conf import settings
from django.test import SimpleTestCase

from petscan import service_store as store

PRIMARY_EXAMPLE_FILE = "petscan-43641756.json"
SECONDARY_EXAMPLE_FILE = "petscan-43642782.json"
PRIMARY_EXAMPLE_PSID = 43641756
PRIMARY_RECORD_COUNT = 2638
SECONDARY_RECORD_COUNT = 23

STORE_REBUILD_TEST_PSID = 999987
STORE_GIL_TEST_PSID = 999991

EXAMPLES_DIR = Path(settings.BASE_DIR) / "data" / "examples"


class ServiceTestCase(SimpleTestCase):
    def _resolve_example_path(self, file_name: str) -> Path:
        candidate = EXAMPLES_DIR / file_name
        if candidate.exists():
            return candidate

        if file_name.endswith(".json"):
            gz_candidate = EXAMPLES_DIR / "{}.gz".format(file_name)
            if gz_candidate.exists():
                return gz_candidate
        elif file_name.endswith(".json.gz"):
            plain_candidate = EXAMPLES_DIR / file_name[:-3]
            if plain_candidate.exists():
                return plain_candidate

        raise FileNotFoundError("Example payload not found: {}".format(candidate))

    def _load_payload(self, file_name: str) -> Dict[str, Any]:
        payload_path = self._resolve_example_path(file_name)
        if payload_path.suffix == ".gz":
            with gzip.open(payload_path, mode="rt", encoding="utf-8") as payload_file:
                return json.load(payload_file)
        return json.loads(payload_path.read_text(encoding="utf-8"))

    def _cleanup_store(self, psid: int) -> None:
        self.addCleanup(shutil.rmtree, store.store_path(psid), ignore_errors=True)
