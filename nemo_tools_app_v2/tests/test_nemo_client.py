from __future__ import annotations

import unittest

from nemo_app.nemo.client import NemoClient


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class NemoClientTests(unittest.TestCase):
    def test_fetch_all_follows_paginated_responses(self) -> None:
        client = NemoClient("token", base_url="https://nemo.example/api/")
        calls: list[str] = []

        def get(url: str, **_options):
            calls.append(url)
            if len(calls) == 1:
                return _Response(
                    {
                        "results": [{"id": 1}],
                        "next": "https://nemo.example/api/projects/?page=2",
                    }
                )
            return _Response({"results": [{"id": 2}], "next": None})

        client.session.get = get  # type: ignore[method-assign]
        self.assertEqual(client.fetch_all("projects/"), [{"id": 1}, {"id": 2}])
        self.assertEqual(calls[-1], "https://nemo.example/api/projects/?page=2")

    def test_dry_run_writes_are_deterministic_and_network_free(self) -> None:
        client = NemoClient("token", dry_run=True)
        first = client.post("users/", {"username": "ada"})
        second = client.post("users/", {"username": "grace"})
        patched = client.patch("users/-1/", {"projects": [2]})
        self.assertEqual((first["id"], second["id"]), (-1, -2))
        self.assertEqual(patched, {"projects": [2]})
        self.assertEqual(len(client.actions), 3)

    def test_pagination_cannot_send_the_token_to_another_host(self) -> None:
        client = NemoClient("token", base_url="https://nemo.example/api/")
        client.session.get = lambda *_args, **_options: _Response(  # type: ignore[method-assign]
            {"results": [], "next": "https://attacker.example/collect"}
        )
        with self.assertRaisesRegex(ValueError, "configured API host"):
            client.fetch_all("projects/")


if __name__ == "__main__":
    unittest.main()
