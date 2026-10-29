from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.lookups import named_ids, required_id
from nemo_app.nemo.metadata import MetadataRepository


class ToolMetadataTests(unittest.TestCase):
    def test_consumable_labs_resolve_category_names_with_changed_ids(self) -> None:
        class Client:
            identity_hash = "test-consumables"

            def fetch_all(self, endpoint):
                if endpoint == "consumable_categories/":
                    return [
                        {"id": 91, "name": "AFM Tips and Probes"},
                        {"id": 92, "name": "Photoresists"},
                    ]
                if endpoint == "consumables/":
                    return [
                        {"name": "Probe", "category": 91},
                        {"name": "Resist", "category": 92},
                        {"name": "Unknown", "category": 999},
                    ]
                raise AssertionError(endpoint)

        with tempfile.TemporaryDirectory() as folder:
            metadata = MetadataRepository(Client(), JsonTTLCache(Path(folder)))
            self.assertEqual(
                metadata.consumable_labs(),
                {
                    "Probe": "SMCL",
                    "Resist": "Cleanroom",
                },
            )

    def test_required_lookup_rejects_missing_and_ambiguous_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing required name"):
            required_id({}, "Local", source="account_types/")
        with self.assertRaisesRegex(ValueError, "Ambiguous"):
            named_ids(
                [{"id": 1, "name": "Local"}, {"id": 2, "name": "LOCAL"}], source="account_types/"
            )

    def test_categories_hidden_tools_and_shared_cache(self) -> None:
        class Client:
            identity_hash = "test-tools"
            calls = 0

            def fetch_all(self, endpoint):
                self.calls += 1
                assert endpoint == "tools/"
                return [
                    {
                        "id": 26,
                        "name": "Oxford PECVD",
                        "visible": False,
                        "_category": "Cleanroom/Deposition & Anneal",
                    },
                    {"id": 79, "name": "FTIR", "_category": "SMCL"},
                    {"id": 15, "name": "TEM", "_category": "Electron Microscopy/Imaging"},
                    {"id": 99, "name": "Unassigned", "_category": None},
                ]

        with tempfile.TemporaryDirectory() as folder:
            client = Client()
            metadata = MetadataRepository(client, JsonTTLCache(Path(folder)))
            self.assertEqual(metadata.tools()[26], "Oxford PECVD")
            self.assertEqual(
                metadata.tool_labs(),
                {
                    "Oxford PECVD": "Cleanroom",
                    "FTIR": "SMCL",
                    "TEM": "Electron Microscopy Lab",
                },
            )
            self.assertEqual(client.calls, 1)
            metadata.tools(use_cache=False)
            self.assertEqual(client.calls, 2)
