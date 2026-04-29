"""Unit tests for the dashboard's pure data-transform functions.

Run from the repo root:
    python -m pytest tests/

Streamlit imports are tolerated at module-load time but never invoked here —
all functions under test are pure, taking dataframes in and returning dicts/lists.
"""
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import (  # noqa: E402
    AVG_DEAL_SIZE,
    LEAD_TIME_BUCKETS,
    _bucket_lead_time,
    calculate_days_to_decision_by_source,
    calculate_full_reasons,
    calculate_lead_time_buckets,
    calculate_survival_curve,
    calculate_velocity_weekly,
)


def _row(event_date, inquiry_date, decision_date, resolution, source="Email", venue="Test Venue"):
    return {
        "Event Date": event_date,
        "Inquiry Date": inquiry_date,
        "Decision Date": decision_date,
        "Resolution": resolution,
        "Initial Contact": source,
        "Venue (if known)": venue,
        "Level of interaction": "Meaningful email interaction",
    }


# -- _bucket_lead_time ------------------------------------------------------

@pytest.mark.parametrize("days,expected", [
    (0, "<3 mo"),
    (89, "<3 mo"),
    (90, "3-6 mo"),
    (179, "3-6 mo"),
    (180, "6-12 mo"),
    (364, "6-12 mo"),
    (365, "12+ mo"),
    (730, "12+ mo"),
])
def test_bucket_lead_time_boundaries(days, expected):
    assert _bucket_lead_time(days) == expected


def test_bucket_lead_time_invalid_inputs():
    assert _bucket_lead_time(-1) is None
    assert _bucket_lead_time(None) is None
    assert _bucket_lead_time(pd.NaT) is None


# -- calculate_lead_time_buckets -------------------------------------------

def test_lead_time_buckets_groups_by_bucket():
    df = pd.DataFrame([
        # 30-day lead time, booked in 5 days → <3 mo, Booked
        _row("12/15/26", "2026-11-15", "2026-11-20", "Booked"),
        # 200-day lead time, didn't book → 6-12 mo, Didn't Book
        _row("12/15/26", "2026-05-29", "2026-07-15", "Didn't Book"),
        # 400-day lead time, cold → 12+ mo, Cold
        _row("12/15/26", "2025-11-10", "2026-02-01", "Cold"),
    ])
    out = calculate_lead_time_buckets(df, event_year=2026)

    assert out["<3 mo"]["count"] == 1
    assert out["<3 mo"]["booked"] == 1
    assert out["<3 mo"]["conversion_rate"] == 100.0

    assert out["6-12 mo"]["booked"] == 0
    assert out["6-12 mo"]["conversion_rate"] == 0.0

    assert out["12+ mo"]["count"] == 1


def test_lead_time_buckets_excludes_capacity_outcomes_from_denominator():
    # Two rows in the same bucket: one Booked, one Full (capacity)
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-15", "2026-11-20", "Booked"),
        _row("12/15/26", "2026-11-15", "2026-11-16", "Full"),
    ])
    out = calculate_lead_time_buckets(df, event_year=2026)
    bucket = out["<3 mo"]
    # Total count is 2, but eligible (excluding Full) is 1, so conversion = 100%
    assert bucket["count"] == 2
    assert bucket["eligible"] == 1
    assert bucket["booked"] == 1
    assert bucket["conversion_rate"] == 100.0


def test_lead_time_buckets_filters_other_event_years():
    df = pd.DataFrame([
        _row("12/15/27", "2027-11-15", "2027-11-20", "Booked"),  # 2027 event
        _row("12/15/26", "2026-11-15", "2026-11-20", "Booked"),
    ])
    out = calculate_lead_time_buckets(df, event_year=2026)
    total = sum(b["count"] for b in out.values())
    assert total == 1


def test_lead_time_buckets_empty_df():
    assert calculate_lead_time_buckets(pd.DataFrame(columns=["Event Date"])) == {}


# -- calculate_days_to_decision_by_source ----------------------------------

def test_dtd_by_source_computes_median_for_booked():
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-15", "2026-11-20", "Booked", source="Knot"),  # 5
        _row("12/15/26", "2026-11-15", "2026-11-25", "Booked", source="Knot"),  # 10
        _row("12/15/26", "2026-11-15", "2026-12-05", "Booked", source="Knot"),  # 20
    ])
    out = calculate_days_to_decision_by_source(df, event_year=2026, min_count=3)

    assert "Knot" in out
    assert out["Knot"]["count"] == 3
    assert out["Knot"]["booked_count"] == 3
    assert out["Knot"]["median_days_booked"] == 10  # median of [5, 10, 20]


def test_dtd_by_source_filters_min_count():
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-15", "2026-11-20", "Booked", source="Referral"),
        _row("12/15/26", "2026-11-15", "2026-11-25", "Booked", source="Knot"),
        _row("12/15/26", "2026-11-15", "2026-12-05", "Booked", source="Knot"),
        _row("12/15/26", "2026-11-15", "2026-12-15", "Booked", source="Knot"),
    ])
    out = calculate_days_to_decision_by_source(df, event_year=2026, min_count=3)
    assert "Knot" in out
    assert "Referral" not in out  # only 1 row, below min_count


def test_dtd_by_source_handles_no_bookings_for_source():
    df = pd.DataFrame([
        _row("12/15/26", "2026-09-01", "2026-10-15", "Cold", source="Knot"),
        _row("12/15/26", "2026-09-01", "2026-10-20", "Didn't Book", source="Knot"),
        _row("12/15/26", "2026-09-01", "2026-10-25", "Cold", source="Knot"),
    ])
    out = calculate_days_to_decision_by_source(df, event_year=2026, min_count=3)
    assert out["Knot"]["booked_count"] == 0
    assert out["Knot"]["median_days_booked"] is None


# -- calculate_velocity_weekly ---------------------------------------------

def test_velocity_returns_requested_week_count():
    today = datetime.now()
    rows = []
    for i in range(15):
        decision = today - timedelta(days=3 + i * 4)
        inquiry = decision - timedelta(days=5)
        rows.append({
            "Event Date": "12/15/26",
            "Inquiry Date": inquiry.strftime("%Y-%m-%d"),
            "Decision Date": decision.strftime("%Y-%m-%d"),
            "Resolution": "Booked" if i < 10 else "Didn't Book",
            "Initial Contact": "Email",
            "Venue (if known)": "",
            "Level of interaction": "",
        })
    df = pd.DataFrame(rows)
    out = calculate_velocity_weekly(df, weeks=4, window_weeks=8)

    assert len(out) == 4
    latest = out[-1]
    assert latest["opps"] > 0
    assert latest["velocity_dollars_per_day"] > 0
    assert 0 < latest["win_rate_pct"] <= 100


def test_velocity_formula_matches_definition():
    """Sanity: velocity = opps × deal × win_rate / cycle, on a hand-computed window."""
    today = datetime.now()
    rows = []
    # 4 booked at 7-day cycle, 1 lost at 7-day cycle, all in last week
    for i in range(5):
        decision = today - timedelta(days=2)
        inquiry = decision - timedelta(days=7)
        rows.append({
            "Event Date": "12/15/26",
            "Inquiry Date": inquiry.strftime("%Y-%m-%d"),
            "Decision Date": decision.strftime("%Y-%m-%d"),
            "Resolution": "Booked" if i < 4 else "Didn't Book",
            "Initial Contact": "Email",
            "Venue (if known)": "",
            "Level of interaction": "",
        })
    df = pd.DataFrame(rows)
    out = calculate_velocity_weekly(df, weeks=1, window_weeks=8)

    assert len(out) == 1
    w = out[-1]
    assert w["opps"] == 5
    assert w["booked"] == 4
    assert abs(w["win_rate_pct"] - 80) < 0.001
    assert abs(w["avg_cycle_days"] - 7) < 0.001
    expected = (5 * AVG_DEAL_SIZE * 0.8) / 7
    assert abs(w["velocity_dollars_per_day"] - expected) < 0.01


def test_velocity_excludes_capacity_outcomes_from_denominator():
    today = datetime.now()
    decision = today - timedelta(days=2)
    inquiry = decision - timedelta(days=7)
    rows = [
        {
            "Event Date": "12/15/26",
            "Inquiry Date": inquiry.strftime("%Y-%m-%d"),
            "Decision Date": decision.strftime("%Y-%m-%d"),
            "Resolution": res,
            "Initial Contact": "Email",
            "Venue (if known)": "",
            "Level of interaction": "",
        }
        for res in ["Booked", "Full", "We turn down"]
    ]
    df = pd.DataFrame(rows)
    out = calculate_velocity_weekly(df, weeks=1, window_weeks=8)
    w = out[-1]
    # Full + Turn down excluded → only the Booked counts as an opp
    assert w["opps"] == 1
    assert w["booked"] == 1
    assert w["win_rate_pct"] == 100


def test_velocity_handles_empty_df():
    df = pd.DataFrame(columns=["Inquiry Date", "Decision Date", "Resolution"])
    assert calculate_velocity_weekly(df) == []


# -- calculate_survival_curve ----------------------------------------------

def test_survival_curve_cdf_progression():
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-01", "2026-11-05", "Booked"),  # day 4
        _row("12/15/26", "2026-11-01", "2026-11-15", "Booked"),  # day 14
    ])
    out = calculate_survival_curve(df, event_year=2026, max_days=30)
    booked = out["Booked"]

    # Day 0: nobody decided yet
    assert booked[0]["pct_decided"] == 0
    # Day 4: one of two decided (50%)
    assert abs(booked[4]["pct_decided"] - 50) < 0.001
    # Day 14: both decided (100%)
    assert abs(booked[14]["pct_decided"] - 100) < 0.001
    # n_total stays constant across the curve
    assert all(p["n_total"] == 2 for p in booked)


def test_survival_curve_separates_booked_from_lost():
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-01", "2026-11-05", "Booked"),
        _row("12/15/26", "2026-11-01", "2026-11-30", "Cold"),
        _row("12/15/26", "2026-11-01", "2026-12-15", "Didn't Book"),
    ])
    out = calculate_survival_curve(df, event_year=2026, max_days=60)
    assert out["Booked"][-1]["n_total"] == 1
    assert out["Lost"][-1]["n_total"] == 2  # Cold + Didn't Book


def test_survival_curve_handles_empty_df():
    df = pd.DataFrame(columns=["Event Date", "Inquiry Date", "Decision Date", "Resolution"])
    out = calculate_survival_curve(df)
    assert out == {"Booked": [], "Lost": []}


# -- calculate_full_reasons -------------------------------------------------

def _full_row(event_date, capacity_status):
    return {
        "Event Date": event_date,
        "Resolution": "Full",
        "Capacity Status (if Full)": capacity_status,
        "Inquiry Date": "",
        "Decision Date": "",
        "Initial Contact": "",
        "Venue (if known)": "",
        "Level of interaction": "",
    }


def test_full_reasons_groups_three_categories():
    df = pd.DataFrame([
        _full_row("12/15/26", "True Capacity"),
        _full_row("12/15/26", "True Capacity"),
        _full_row("12/15/26", "Artificial Cap (capacity known)"),
        _full_row("12/15/26", "Artificial Cap (holding for AAG)"),
    ])
    out = calculate_full_reasons(df, event_year=2026)
    assert out["total_full"] == 4
    assert out["breakdown"]["True Capacity"] == 2
    assert out["breakdown"]["Artificial Cap (capacity known)"] == 1
    assert out["breakdown"]["Artificial Cap (holding for AAG)"] == 1
    assert out["true_capacity_total"] == 2
    assert out["artificial_total"] == 2
    assert out["artificial_potential_revenue"] == 2 * AVG_DEAL_SIZE
    assert out["unspecified_total"] == 0


def test_full_reasons_counts_unspecified_separately():
    df = pd.DataFrame([
        _full_row("12/15/26", "True Capacity"),
        _full_row("12/15/26", ""),
        _full_row("12/15/26", "  "),
    ])
    out = calculate_full_reasons(df, event_year=2026)
    assert out["total_full"] == 3
    assert out["true_capacity_total"] == 1
    assert out["artificial_total"] == 0
    assert out["unspecified_total"] == 2


def test_full_reasons_filters_other_event_years():
    df = pd.DataFrame([
        _full_row("12/15/27", "True Capacity"),  # 2027 event
        _full_row("12/15/26", "Artificial Cap (capacity known)"),
    ])
    out = calculate_full_reasons(df, event_year=2026)
    assert out["total_full"] == 1
    assert out["artificial_total"] == 1


def test_full_reasons_ignores_non_full_resolutions():
    df = pd.DataFrame([
        _full_row("12/15/26", "True Capacity"),
        # A Booked row that happens to have Capacity Status filled — ignore it.
        {**_full_row("12/15/26", "Artificial Cap (capacity known)"), "Resolution": "Booked"},
    ])
    out = calculate_full_reasons(df, event_year=2026)
    assert out["total_full"] == 1


def test_full_reasons_empty_df():
    df = pd.DataFrame(columns=["Event Date", "Resolution", "Capacity Status (if Full)"])
    out = calculate_full_reasons(df, event_year=2026)
    assert out == {
        "breakdown": {},
        "total_full": 0,
        "true_capacity_total": 0,
        "artificial_total": 0,
        "artificial_potential_revenue": 0,
        "unspecified_total": 0,
    }


def test_full_reasons_missing_column_returns_empty():
    df = pd.DataFrame([{
        "Event Date": "12/15/26",
        "Resolution": "Full",
        "Inquiry Date": "",
        "Decision Date": "",
    }])
    out = calculate_full_reasons(df, event_year=2026)
    assert out == {}


# -- bucket order constant -------------------------------------------------

def test_lead_time_buckets_constant_is_ordered():
    """Charts and tables iterate LEAD_TIME_BUCKETS in display order; lock the order."""
    assert LEAD_TIME_BUCKETS == ["<3 mo", "3-6 mo", "6-12 mo", "12+ mo"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
