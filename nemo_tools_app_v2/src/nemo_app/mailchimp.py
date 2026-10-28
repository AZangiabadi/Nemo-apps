from __future__ import annotations

import hashlib
import re
from typing import Any

import requests


class MailchimpClient:
    """Audience comparison and create-only additions; existing statuses are never changed."""

    def __init__(self, api_key: str, audience: str):
        key = api_key.strip()
        server = key.rsplit("-", 1)[-1]
        if not key or not re.fullmatch(r"us\d+", server):
            raise ValueError("Enter a Mailchimp API key ending in a server suffix such as -us11.")
        if not re.fullmatch(r"[a-zA-Z0-9]+", audience):
            raise ValueError("Enter a valid Mailchimp audience ID.")
        self.base_url = f"https://{server}.api.mailchimp.com/3.0/lists/{audience}"
        self.session = requests.Session()
        self.session.auth = ("nemo-tools", key)
        self.session.headers.update({"Accept": "application/json"})

    def _request(self, method: str, path: str = "", **kwargs) -> requests.Response:
        try:
            return self.session.request(
                method, self.base_url + path, timeout=30, allow_redirects=False, **kwargs
            )
        except requests.RequestException:
            raise ValueError("Mailchimp could not be reached. Retry the comparison.") from None

    @staticmethod
    def _json(response: requests.Response) -> dict[str, Any]:
        if not 200 <= response.status_code < 300:
            if response.status_code in (401, 403):
                raise ValueError(
                    "Mailchimp access denied. Check the API key and account permissions."
                )
            raise ValueError(f"Mailchimp returned HTTP {response.status_code}. Check the audience.")
        try:
            result = response.json()
        except ValueError:
            raise ValueError("Mailchimp returned an invalid response.") from None
        if not isinstance(result, dict):
            raise ValueError("Mailchimp returned an invalid response.")
        return result

    def audience_info(self) -> dict[str, Any]:
        return self._json(self._request("GET", params={"fields": "id,name,double_optin"}))

    def members(self) -> dict[str, str]:
        members: dict[str, str] = {}
        offset = 0
        while True:
            result = self._json(
                self._request(
                    "GET",
                    "/members",
                    params={
                        "count": 1000,
                        "offset": offset,
                        "fields": "members.email_address,members.status,total_items",
                    },
                )
            )
            page = result.get("members")
            if not isinstance(page, list) or "total_items" not in result:
                raise ValueError("Mailchimp returned an incomplete audience listing.")
            for member in page:
                members[str(member["email_address"]).strip().lower()] = str(member["status"])
            offset += len(page)
            if offset >= int(result["total_items"]):
                return members
            if not page:
                raise ValueError("Mailchimp pagination ended before all contacts were retrieved.")

    def member_status(self, email: str) -> str | None:
        digest = hashlib.md5(email.strip().lower().encode(), usedforsecurity=False).hexdigest()
        response = self._request("GET", f"/members/{digest}")
        if response.status_code == 404:
            return None
        return str(self._json(response).get("status", "unknown"))

    def add_new(self, contact: dict[str, str], status: str) -> str:
        if status not in {"pending", "subscribed"}:
            raise ValueError("Choose a valid subscription action.")
        # Recheck immediately before the write. POST is create-only, so a race
        # with another sync cannot overwrite an existing unsubscribe choice.
        current = self.member_status(contact["email"])
        if current is not None:
            return "skipped_existing_" + current
        response = self._request(
            "POST",
            "/members",
            json={
                "email_address": contact["email"],
                "status": status,
                "merge_fields": {"FNAME": contact["first_name"], "LNAME": contact["last_name"]},
            },
        )
        if response.status_code == 400:
            try:
                title = response.json().get("title", "")
            except (ValueError, AttributeError):
                title = ""
            if title == "Member Exists":
                return "skipped_existing"
            return "rejected_by_mailchimp"
        self._json(response)
        return status
