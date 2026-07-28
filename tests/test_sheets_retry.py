"""Unit tests for open_spreadsheet_with_retry.

Run from the repo root:
    python -m pytest tests/

Covers the behavior that is easy to get wrong: retry ONLY on transient API
codes, and never on a permission or not-found error. Retrying a 403 turns a
clear "the service account lost access" failure into a slow one with the same
outcome, and retrying a 404 does the same for a bad sheet ID.

Ported from dj-availability-checker/dj_core.py, where this was written for
Streamlit Cloud cold starts hitting a brief Google API blip. The dashboard is
the app actually deployed to Streamlit Cloud and had no retry at all.
"""
import json
import os
import sys

import gspread
import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import (  # noqa: E402
    _TRANSIENT_API_CODES,
    open_spreadsheet_with_retry,
)


def _api_error(status_code):
    """Build a real gspread APIError carrying the given HTTP status."""
    response = requests.models.Response()
    response.status_code = status_code
    response._content = json.dumps(
        {"error": {"code": status_code, "message": "test", "status": "TEST"}}
    ).encode()
    response.headers["Content-Type"] = "application/json"
    return gspread.exceptions.APIError(response)


class FakeClient:
    """Stands in for a gspread client. Raises the queued side effects in order."""

    def __init__(self, side_effects):
        self.side_effects = list(side_effects)
        self.calls = []

    def open_by_key(self, spreadsheet_id):
        self.calls.append(spreadsheet_id)
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


@pytest.fixture
def no_sleep(monkeypatch):
    """Record backoff durations instead of actually sleeping."""
    slept = []
    monkeypatch.setattr("dashboard.time.sleep", lambda s: slept.append(s))
    return slept


def test_returns_sheet_on_first_attempt(no_sleep):
    client = FakeClient(["SHEET"])
    assert open_spreadsheet_with_retry(client, "abc") == "SHEET"
    assert len(client.calls) == 1
    assert no_sleep == []


@pytest.mark.parametrize("code", sorted(_TRANSIENT_API_CODES))
def test_retries_every_transient_code_then_succeeds(code, no_sleep):
    client = FakeClient([_api_error(code), "SHEET"])
    assert open_spreadsheet_with_retry(client, "abc") == "SHEET"
    assert len(client.calls) == 2


def test_gives_up_after_max_attempts(no_sleep):
    client = FakeClient([_api_error(503), _api_error(503), _api_error(503)])
    with pytest.raises(gspread.exceptions.APIError):
        open_spreadsheet_with_retry(client, "abc", max_attempts=3)
    assert len(client.calls) == 3


def test_does_not_retry_permission_error(no_sleep):
    """403 means the service account lost access. Retrying just adds latency."""
    client = FakeClient([_api_error(403), "SHEET"])
    with pytest.raises(gspread.exceptions.APIError):
        open_spreadsheet_with_retry(client, "abc")
    assert len(client.calls) == 1
    assert no_sleep == []


def test_does_not_retry_not_found(no_sleep):
    """404 means a bad sheet ID. Same reasoning as 403."""
    client = FakeClient([_api_error(404), "SHEET"])
    with pytest.raises(gspread.exceptions.APIError):
        open_spreadsheet_with_retry(client, "abc")
    assert len(client.calls) == 1


def test_does_not_swallow_unrelated_exceptions(no_sleep):
    client = FakeClient([ValueError("boom")])
    with pytest.raises(ValueError):
        open_spreadsheet_with_retry(client, "abc")
    assert len(client.calls) == 1


def test_backoff_is_exponential(no_sleep):
    client = FakeClient([_api_error(503), _api_error(503), "SHEET"])
    assert open_spreadsheet_with_retry(client, "abc", max_attempts=3) == "SHEET"
    assert no_sleep == [1, 2]


def test_no_sleep_after_the_final_failed_attempt(no_sleep):
    """Sleeping after the last attempt delays the raise for no benefit."""
    client = FakeClient([_api_error(503), _api_error(503)])
    with pytest.raises(gspread.exceptions.APIError):
        open_spreadsheet_with_retry(client, "abc", max_attempts=2)
    assert no_sleep == [1]
