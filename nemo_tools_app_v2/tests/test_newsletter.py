from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from nemo_app.config import AppConfig
from nemo_app.jobs.handlers import run_job
from nemo_app.mailchimp import MailchimpClient
from nemo_app.newsletter import compare_newsletter, contact_activity
from nemo_app.web.app import create_app


def response(http_status=200, **data):
    result = Mock()
    result.status_code = http_status
    result.json.return_value = data
    return result


class MailchimpTests(unittest.TestCase):
    def test_pi_flag_uses_managed_projects_and_combines_duplicate_active_accounts(self):
        users = [
            {"is_active": True, "email": "pi@example.edu", "managed_projects": []},
            {"is_active": True, "email": "PI@example.edu", "managed_projects": [12]},
            {"is_active": True, "email": "user@example.edu", "managed_projects": []},
            {"is_active": False, "email": "user@example.edu", "managed_projects": [13]},
            {"is_active": True, "email": "unknown@example.edu"},
        ]
        client = Mock()
        client.audience_info.return_value = {"name": "Test"}
        client.members.return_value = {}
        rows = compare_newsletter(users, client)["contacts"]
        flags = {row["email"]: row["is_pi"] for row in rows}
        self.assertEqual(
            flags,
            {
                "pi@example.edu": True,
                "user@example.edu": False,
                "unknown@example.edu": False,
            },
        )

    def test_activity_uses_latest_login_and_usage_across_duplicate_emails(self):
        users = [
            {
                "id": 1,
                "email": " ADA@example.edu ",
                "is_active": True,
                "last_login": "2026-09-30T20:00:00-04:00",
            },
            {
                "id": 2,
                "email": "ada@example.edu",
                "is_active": True,
                "last_login": "2026-10-01T01:00:00+00:00",
            },
            {"id": 3, "email": "inactive@example.edu", "is_active": False},
            {"id": 4, "email": "new@example.edu", "is_active": True, "last_login": None},
        ]
        events = [
            {"user": 1, "start": "2026-08-31T23:00:00-04:00", "end": "2026-09-01T01:00:00-04:00"},
            {
                "user": {"id": 2},
                "start": "2026-09-30T23:00:00-04:00",
                "end": "2026-10-01T01:00:00-04:00",
            },
            {"user": 2, "start": "2026-10-02T10:00:00-04:00", "end": None},
            {"user": 3, "start": "2026-10-03T10:00:00-04:00", "end": None},
            {"user": 1, "start": "invalid", "end": "invalid"},
        ]
        result = contact_activity(users, events, timezone=dt.UTC)
        self.assertEqual(result["ada@example.edu"]["last_login"], "2026-10-01T01:00:00+00:00")
        self.assertEqual(result["ada@example.edu"]["last_tool_use"], "2026-10-02T10:00:00-04:00")
        self.assertTrue(result["ada@example.edu"]["tool_in_use"])
        self.assertIsNone(result["new@example.edu"]["last_tool_use"])
        self.assertNotIn("inactive@example.edu", result)

    def test_comparison_deduplicates_active_users_and_preserves_all_existing_statuses(self):
        users = [
            {"is_active": True, "email": " New@Example.edu ", "first_name": "New"},
            {"is_active": True, "email": "new@example.edu"},
            {"is_active": False, "email": "inactive@example.edu"},
            {"is_active": True, "email": "invalid@example"},
        ]
        statuses = ["subscribed", "unsubscribed", "pending", "cleaned", "transactional"]
        users.extend({"is_active": True, "email": f"{s}@example.edu"} for s in statuses)
        client = Mock()
        client.audience_info.return_value = {"name": "Test", "double_optin": True}
        client.members.return_value = {f"{s}@example.edu": s for s in statuses}
        result = compare_newsletter(users, client)
        self.assertEqual(result["active_unique_emails"], 6)
        self.assertEqual(result["invalid_or_missing_emails"], 1)
        self.assertEqual(result["counts"]["missing"], 1)
        self.assertTrue(all(result["counts"][s] == 1 for s in statuses))
        client.add_new.assert_not_called()

    def test_members_paginate_and_do_not_filter_out_unsubscribed(self):
        client = MailchimpClient("test-key-us11", "abc123")
        client.session = Mock()
        client.session.request.side_effect = [
            response(
                members=[{"email_address": "A@example.edu", "status": "subscribed"}], total_items=2
            ),
            response(
                members=[{"email_address": "b@example.edu", "status": "unsubscribed"}],
                total_items=2,
            ),
        ]
        self.assertEqual(client.members()["b@example.edu"], "unsubscribed")
        self.assertEqual(client.session.request.call_args.kwargs["params"]["offset"], 1)
        self.assertNotIn("status", client.session.request.call_args.kwargs["params"])
        self.assertFalse(client.session.request.call_args.kwargs["allow_redirects"])

    def test_existing_contacts_never_receive_a_write(self):
        for status in ("unsubscribed", "cleaned", "pending", "subscribed", "transactional"):
            client = MailchimpClient("test-us11", "abc123")
            client.session = Mock()
            client.session.request.return_value = response(status=status)
            result = client.add_new({"email": "a@example.edu"}, "pending")
            self.assertEqual(result, "skipped_existing_" + status)
            self.assertEqual(client.session.request.call_count, 1)
            self.assertEqual(client.session.request.call_args.args[0], "GET")

    def test_new_add_is_create_only_and_handles_duplicate_race(self):
        contact = {"email": "a@example.edu", "first_name": "Ada", "last_name": "User"}
        for final_response, expected in (
            (response(status="pending"), "pending"),
            (response(400, title="Member Exists"), "skipped_existing"),
        ):
            client = MailchimpClient("test-us11", "abc123")
            client.session = Mock()
            client.session.request.side_effect = [response(404), final_response]
            self.assertEqual(client.add_new(contact, "pending"), expected)
            call = client.session.request.call_args
            self.assertEqual(call.args[0], "POST")
            self.assertEqual(call.kwargs["json"]["status"], "pending")

    def test_errors_do_not_expose_credentials(self):
        client = MailchimpClient("secret-key-us11", "abc123")
        client.session = Mock()
        client.session.request.side_effect = requests.ConnectionError("secret-key-us11")
        with self.assertRaisesRegex(ValueError, "could not be reached") as error:
            client.audience_info()
        self.assertNotIn("secret-key", str(error.exception))
        for key in ("", "secret-evil.example.com", "secret-us11/redirect"):
            with self.assertRaises(ValueError):
                MailchimpClient(key, "abc123")


class NewsletterWebTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.config = replace(
            AppConfig.from_env(base_dir=root),
            data_dir=root / "data",
            access_password="",
            job_secret_key="",
            environment="development",
            read_only_api_token="test-nemo-token",
            mailchimp_api_key="",
        )
        self.app = create_app(self.config)
        self.app.config.update(TESTING=True)
        self.client = self.app.test_client()
        self.client.get("/tools/newsletter")
        with self.client.session_transaction() as session:
            self.csrf = session["csrf_token"]
        self.store = self.app.extensions["job_store"]

    def tearDown(self):
        self.directory.cleanup()

    def test_server_key_is_reused_without_being_shown_or_overridden(self):
        self.app.extensions["nemo_config"] = replace(
            self.config, mailchimp_api_key="server-mailchimp-secret-us11"
        )
        page = self.client.get("/tools/newsletter").data
        self.assertIn(b"Using the saved Mailchimp API key", page)
        self.assertNotIn(b'name="mailchimp_key"', page)
        self.assertNotIn(b"server-mailchimp-secret", page)
        response = self.client.post(
            "/tools/newsletter",
            data={
                "csrf_token": self.csrf,
                "audience_id": "abc123",
                "mailchimp_key": "override-us12",
            },
        )
        self.assertEqual(response.status_code, 302)
        job = self.store.claim_next("test")
        self.assertEqual(job.secrets["mailchimp_key"], "server-mailchimp-secret-us11")
        self.assertNotIn(b"server-mailchimp-secret", self.config.database_path.read_bytes())
        self.assertNotIn("server-mailchimp-secret", repr(self.app.extensions["nemo_config"]))

    def comparison(self, double_optin=False):
        result = self.client.post(
            "/tools/newsletter",
            data={
                "csrf_token": self.csrf,
                "mailchimp_key": "secret-mailchimp-us11",
                "audience_id": "abc123",
            },
        )
        self.assertEqual(result.status_code, 302)
        job = self.store.claim_next("test")
        self.store.complete(
            job.id,
            summary="Compared",
            result={
                "files": [],
                "data": {
                    "audience_name": "Test",
                    "double_optin": double_optin,
                    "counts": {"missing": 1},
                    "active_unique_emails": 2,
                    "invalid_or_missing_emails": 0,
                    "contacts": [
                        {
                            "email": "new@example.edu",
                            "first_name": "New",
                            "last_name": "User",
                            "status": "missing",
                        },
                        {"email": "unsub@example.edu", "status": "unsubscribed"},
                    ],
                },
            },
        )
        return job.id

    def test_keys_are_encrypted_and_never_returned_in_status_or_review(self):
        identifier = self.comparison()
        status = self.client.get(f"/jobs/{identifier}/status")
        review = self.client.get(f"/tools/newsletter/review/{identifier}")
        self.assertEqual(review.status_code, 200)
        for content in (status.data, review.data, self.config.database_path.read_bytes()):
            self.assertNotIn(b"secret-mailchimp-us11", content)
        self.assertNotIn(b'name="emails" value="unsub@example.edu"', review.data)
        self.assertEqual(
            self.store.get_secrets(identifier)["mailchimp_key"], "secret-mailchimp-us11"
        )

    def test_review_requires_selection_and_permission_and_rejects_unsubscribed(self):
        identifier = self.comparison()
        url = f"/tools/newsletter/review/{identifier}"
        data = {
            "csrf_token": self.csrf,
            "emails": "new@example.edu",
            "consent_note": "Opted in during onboarding",
            "subscription_status": "pending",
        }
        self.assertEqual(self.client.post(url, data=data).status_code, 400)
        data["consent_confirmed"] = "on"
        data["emails"] = "unsub@example.edu"
        self.assertEqual(self.client.post(url, data=data).status_code, 400)
        data["emails"] = "new@example.edu"
        self.assertEqual(self.client.post(url, data=data).status_code, 302)
        job = self.store.claim_next("test")
        self.assertEqual(job.kind, "newsletter_add")
        self.assertEqual(job.secrets["mailchimp_key"], "secret-mailchimp-us11")
        self.assertEqual(job.payload["subscription_status"], "pending")

    def test_double_optin_cannot_be_bypassed_or_submission_forged(self):
        identifier = self.comparison(double_optin=True)
        data = {
            "csrf_token": self.csrf,
            "emails": "new@example.edu",
            "consent_confirmed": "on",
            "consent_note": "Onboarding opt-in",
            "subscription_status": "subscribed",
        }
        url = f"/tools/newsletter/review/{identifier}"
        self.assertEqual(self.client.post(url, data=data).status_code, 400)
        data["subscription_status"] = "pending"
        del data["csrf_token"]
        self.assertEqual(self.client.post(url, data=data).status_code, 400)

    def test_worker_rechecks_active_users_and_preserves_existing_subscription(self):
        identifier = self.comparison()
        self.client.post(
            f"/tools/newsletter/review/{identifier}",
            data={
                "csrf_token": self.csrf,
                "emails": "new@example.edu",
                "consent_confirmed": "on",
                "consent_note": "Onboarding opt-in",
                "subscription_status": "pending",
            },
        )
        job = self.store.claim_next("test")
        with (
            patch("nemo_app.jobs.handlers.MailchimpClient") as mailchimp,
            patch("nemo_app.jobs.handlers.JobContext.read_only_client") as nemo,
        ):
            nemo.return_value.fetch_all.return_value = []
            run_job(self.config, self.store, job)
            mailchimp.return_value.add_new.assert_not_called()
        result = self.store.get(job.id)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.result["data"]["outcomes"][0]["result"], "skipped_inactive")

    def test_expiration_discards_retained_comparison_credentials(self):
        identifier = self.comparison()
        self.store.expire_before(dt.datetime.now(dt.UTC) + dt.timedelta(days=1))
        with self.assertRaises(ValueError):
            self.store.get_secrets(identifier)
        self.assertEqual(self.client.get(f"/tools/newsletter/review/{identifier}").status_code, 404)

    def test_review_shows_activity_and_converts_to_facility_timezone(self):
        identifier = self.comparison()
        with self.app.test_request_context():
            formatter = self.app.jinja_env.filters["newsletter_datetime"]
            self.assertEqual(formatter("2026-10-01T01:00:00+00:00"), "Sep 30, 2026 09:00 PM EDT")
            self.assertEqual(formatter(None), "Not recorded")
        page = self.client.get(f"/tools/newsletter/review/{identifier}").data
        self.assertIn(b"Last NEMO login", page)
        self.assertIn(b"Last tool use", page)
        self.assertIn(b"Unavailable", page)

    def test_tool_history_failure_is_explicit_without_blocking_comparison(self):
        self.client.post(
            "/tools/newsletter",
            data={
                "csrf_token": self.csrf,
                "mailchimp_key": "test-us11",
                "audience_id": "abc123",
            },
        )
        job = self.store.claim_next("test")
        with (
            patch("nemo_app.jobs.handlers.MailchimpClient") as mailchimp,
            patch("nemo_app.jobs.handlers.JobContext.read_only_client") as nemo,
        ):
            nemo.return_value.fetch_all.side_effect = [
                [{"id": 1, "is_active": True, "email": "new@example.edu"}],
                requests.HTTPError(),
            ]
            mailchimp.return_value.audience_info.return_value = {"name": "Test"}
            mailchimp.return_value.members.return_value = {}
            run_job(self.config, self.store, job)
        self.assertFalse(self.store.get(job.id).result["data"]["tool_activity_available"])
        self.assertEqual(self.store.get(job.id).status, "completed")

    def test_worker_comparison_uses_read_only_clients_and_outputs_review(self):
        self.client.post(
            "/tools/newsletter",
            data={
                "csrf_token": self.csrf,
                "mailchimp_key": "test-us11",
                "audience_id": "abc123",
            },
        )
        job = self.store.claim_next("test")
        with (
            patch("nemo_app.jobs.handlers.MailchimpClient") as mailchimp,
            patch("nemo_app.jobs.handlers.JobContext.read_only_client") as nemo,
        ):
            nemo.return_value.fetch_all.return_value = [
                {"is_active": True, "email": "new@example.edu"}
            ]
            mailchimp.return_value.audience_info.return_value = {"name": "Test"}
            mailchimp.return_value.members.return_value = {}
            run_job(self.config, self.store, job)
            mailchimp.return_value.add_new.assert_not_called()
        result = self.store.get(job.id)
        self.assertEqual(result.result["data"]["counts"], {"missing": 1})
        self.assertEqual(self.client.get(f"/tools/newsletter/review/{job.id}").status_code, 200)
        self.assertEqual(self.store.get_secrets(job.id)["mailchimp_key"], "test-us11")
