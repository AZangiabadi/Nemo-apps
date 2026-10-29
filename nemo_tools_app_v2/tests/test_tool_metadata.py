from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.metadata import MetadataRepository


class ToolMetadataTests(unittest.TestCase):
    def test_categories_hidden_tools_and_shared_cache(self) -> None:
        class Client:
            identity_hash = "test-tools"
            calls = 0

            def fetch_all(self, endpoint):
                self.calls += 1
                assert endpoint == "tools/"
                return [
                    {"id": 26, "name": "Oxford PECVD", "visible": False,
                     "_category": "Cleanroom/Deposition & Anneal"},
                    {"id": 79, "name": "FTIR", "_category": "SMCL"},
                    {"id": 15, "name": "TEM", "_category": "Electron Microscopy/Imaging"},
                    {"id": 99, "name": "Unassigned", "_category": None},
                ]

        with tempfile.TemporaryDirectory() as folder:
            client = Client()
            metadata = MetadataRepository(client, JsonTTLCache(Path(folder)))
            self.assertEqual(metadata.tools()[26], "Oxford PECVD")
            self.assertEqual(metadata.tool_labs(), {
                "Oxford PECVD": "Cleanroom", "FTIR": "SMCL",
                "TEM": "Electron Microscopy Lab",
            })
            self.assertEqual(client.calls, 1)
            metadata.tools(use_cache=False)
            self.assertEqual(client.calls, 2)
