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
    build_venue_tier_lookup,
    calculate_days_to_decision_by_source,
    calculate_full_reasons,
    calculate_growth_target_activity,
    calculate_lead_time_buckets,
    calculate_metrics_by_recommended_status,
    calculate_metrics_by_tier,
    calculate_survival_curve,
    calculate_survival_curve_by_lead_time,
    calculate_velocity_weekly,
    find_outreach_targets,
    find_research_targets,
    normalize_venue_name,
    venue_to_tier_info,
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


# -- calculate_survival_curve_by_lead_time --------------------------------

def test_survival_by_lead_time_groups_bookers_by_bucket():
    df = pd.DataFrame([
        # Short-lead booker: inquired ~30 days out, decided in 4 days
        _row("12/15/26", "2026-11-15", "2026-11-19", "Booked"),
        # Long-lead booker: inquired ~14 months out, decided in 14 days
        _row("12/15/26", "2025-10-01", "2025-10-15", "Booked"),
        # Lost: should not appear in any curve
        _row("12/15/26", "2026-09-01", "2026-09-30", "Cold"),
    ])
    out = calculate_survival_curve_by_lead_time(df, event_year=2026, max_days=30)

    # Short-lead bucket present; long-lead bucket present
    assert "<3 mo" in out
    assert "12+ mo" in out
    # Each has exactly one booker
    assert out["<3 mo"][0]["n_total"] == 1
    assert out["12+ mo"][0]["n_total"] == 1


def test_survival_by_lead_time_excludes_lost_outcomes():
    df = pd.DataFrame([
        _row("12/15/26", "2026-11-15", "2026-11-19", "Booked"),  # included
        _row("12/15/26", "2026-11-15", "2026-11-19", "Cold"),    # excluded
        _row("12/15/26", "2026-11-15", "2026-11-19", "Didn't Book"),  # excluded
    ])
    out = calculate_survival_curve_by_lead_time(df, event_year=2026, max_days=30)
    assert out["<3 mo"][0]["n_total"] == 1


def test_survival_by_lead_time_cdf_progression_per_bucket():
    df = pd.DataFrame([
        # Two short-lead bookers: one decides at day 2, one at day 10
        _row("12/15/26", "2026-11-15", "2026-11-17", "Booked"),
        _row("12/15/26", "2026-11-15", "2026-11-25", "Booked"),
    ])
    out = calculate_survival_curve_by_lead_time(df, event_year=2026, max_days=30)
    short = out["<3 mo"]
    # Day 0: nobody decided yet (0%)
    assert short[0]["pct_decided"] == 0
    # Day 2: one of two decided (50%)
    assert abs(short[2]["pct_decided"] - 50) < 0.001
    # Day 10: both decided (100%)
    assert abs(short[10]["pct_decided"] - 100) < 0.001
    # n_total stays constant
    assert all(p["n_total"] == 2 for p in short)


def test_survival_by_lead_time_handles_empty_df():
    df = pd.DataFrame(columns=["Event Date", "Inquiry Date", "Decision Date", "Resolution"])
    out = calculate_survival_curve_by_lead_time(df)
    assert out == {}


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


# -- normalize_venue_name --------------------------------------------------

def test_normalize_strips_trailing_parens():
    assert normalize_venue_name("Nestldown (NO FOG, NO TAPE)") == "nestldown"
    assert normalize_venue_name("Kohl Mansion (FOG OK)") == "kohl mansion"


def test_normalize_handles_empty():
    assert normalize_venue_name("") == ""
    assert normalize_venue_name(None) == ""
    assert normalize_venue_name("   ") == ""


def test_normalize_lowercases_and_strips_punctuation():
    assert normalize_venue_name("CASA REAL!!") == "casa real"
    assert normalize_venue_name("B.R. Cohn") == "b r cohn"


# -- build_venue_tier_lookup + venue_to_tier_info --------------------------

def _venue(name, tier="Tier 4", growth_target=False, recommended_status="Unknown", former_names="", wedding_venue=True):
    return {
        "name": name,
        "tier": tier,
        "growth_target": growth_target,
        "recommended_status": recommended_status,
        "former_names": former_names,
        "wedding_venue": wedding_venue,
    }


def test_lookup_finds_canonical_and_former_names():
    venues = [
        _venue("Nestldown", tier="Tier 1", former_names="Nestldown (NO FOG, NO TAPE)"),
        _venue("Casa Real", tier="Tier 2", former_names="Casa Real at Ruby Hill"),
    ]
    lookup = build_venue_tier_lookup(venues)
    # Canonical match
    info = venue_to_tier_info("Nestldown", lookup)
    assert info["tier"] == "Tier 1"
    # Trailing-paren variant maps via normalization
    info = venue_to_tier_info("Nestldown (NO FOG, NO TAPE)", lookup)
    assert info["tier"] == "Tier 1"
    # Former-name match
    info = venue_to_tier_info("Casa Real at Ruby Hill", lookup)
    assert info["tier"] == "Tier 2"


def test_lookup_unknown_defaults_to_tier_4():
    lookup = build_venue_tier_lookup([_venue("Nestldown", tier="Tier 1")])
    info = venue_to_tier_info("Some Random Venue", lookup)
    assert info["tier"] == "Tier 4"


def test_lookup_blank_returns_none():
    lookup = build_venue_tier_lookup([_venue("Nestldown", tier="Tier 1")])
    assert venue_to_tier_info("", lookup) is None
    assert venue_to_tier_info(None, lookup) is None


# -- calculate_metrics_by_tier ---------------------------------------------

def _inquiry_row(event_date, venue, resolution, inquiry_offset_days=10, decision_offset_days=5):
    """Helper: build an inquiry row 5 days after a 10-day-pre-event inquiry."""
    from datetime import datetime, timedelta
    event = datetime.strptime(event_date, "%m/%d/%y")
    inquiry = event - timedelta(days=inquiry_offset_days)
    decision = inquiry + timedelta(days=decision_offset_days)
    return {
        "Event Date": event_date,
        "Inquiry Date": inquiry.strftime("%Y-%m-%d"),
        "Decision Date": decision.strftime("%Y-%m-%d"),
        "Resolution": resolution,
        "Venue (if known)": venue,
        "Initial Contact": "",
        "Level of interaction": "",
    }


def test_metrics_by_tier_groups_and_rolls_tier_3_4():
    venues = [
        _venue("Nestldown", tier="Tier 1"),
        _venue("Kennedy Middle School", tier="Tier 2"),
        _venue("Random Hall", tier="Tier 3"),
        _venue("Some Old Place", tier="Tier 4"),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([
        _inquiry_row("12/15/26", "Nestldown", "Booked"),
        _inquiry_row("12/16/26", "Kennedy Middle School", "Booked"),
        _inquiry_row("12/17/26", "Kennedy Middle School", "Didn't Book"),
        _inquiry_row("12/18/26", "Random Hall", "Booked"),
        _inquiry_row("12/19/26", "Some Old Place", "Cold"),
    ])
    out = calculate_metrics_by_tier(df, lookup, event_year=2026)
    assert out["Tier 1"]["count"] == 1
    assert out["Tier 1"]["conversion_rate"] == 100.0
    assert out["Tier 2"]["count"] == 2
    assert out["Tier 2"]["conversion_rate"] == 50.0
    # Tier 3 + Tier 4 rolled into Tier 3+
    assert out["Tier 3+"]["count"] == 2
    assert out["Tier 3+"]["booked"] == 1


def test_metrics_by_tier_excludes_blank_venues():
    venues = [_venue("Nestldown", tier="Tier 1")]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([
        _inquiry_row("12/15/26", "Nestldown", "Booked"),
        _inquiry_row("12/16/26", "", "Booked"),  # excluded
    ])
    out = calculate_metrics_by_tier(df, lookup, event_year=2026)
    assert out["Tier 1"]["count"] == 1
    assert "Tier 3+" not in out


def test_metrics_by_tier_excludes_non_wedding_venues():
    """Schools, single-org recurring venues, etc. are excluded from wedding-pipeline analytics."""
    venues = [
        _venue("Nestldown", tier="Tier 1", wedding_venue=True),
        _venue("Kennedy Middle School", tier="Tier 2", wedding_venue=False),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([
        _inquiry_row("12/15/26", "Nestldown", "Booked"),
        _inquiry_row("12/16/26", "Kennedy Middle School", "Booked"),  # excluded — not a wedding venue
    ])
    out = calculate_metrics_by_tier(df, lookup, event_year=2026)
    assert out["Tier 1"]["count"] == 1
    assert "Tier 2" not in out  # school inquiry filtered out


def test_research_targets_excludes_non_wedding_venues():
    venues = [
        _venue("Wedding T2", tier="Tier 2", recommended_status="Unknown", wedding_venue=True),
        _venue("School T2", tier="Tier 2", recommended_status="Unknown", wedding_venue=False),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([])
    targets = find_research_targets(venues, lookup, df, event_year=2026)
    names = {t["name"] for t in targets}
    assert names == {"Wedding T2"}


def test_outreach_targets_excludes_non_wedding_venues():
    venues = [
        _venue("Wedding GT", growth_target=True, recommended_status="No, with hard evidence", wedding_venue=True),
        _venue("School GT", growth_target=True, recommended_status="No, with hard evidence", wedding_venue=False),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([])
    targets = find_outreach_targets(venues, lookup, df, event_year=2026)
    names = {t["name"] for t in targets}
    assert names == {"Wedding GT"}


def test_growth_target_activity_excludes_non_wedding_venues():
    venues = [
        _venue("Wedding GT", tier="Tier 2", growth_target=True, wedding_venue=True),
        _venue("School GT", tier="Tier 2", growth_target=True, wedding_venue=False),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([])
    out = calculate_growth_target_activity(venues, lookup, df, event_year=2026)
    names = {v["name"] for v in out}
    assert names == {"Wedding GT"}


# -- calculate_metrics_by_recommended_status -------------------------------

def test_metrics_by_recommended_status_groups():
    venues = [
        _venue("On List", recommended_status="Yes, with hard evidence"),
        _venue("Off List", recommended_status="No, with hard evidence"),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([
        _inquiry_row("12/15/26", "On List", "Booked"),
        _inquiry_row("12/16/26", "On List", "Booked"),
        _inquiry_row("12/17/26", "Off List", "Didn't Book"),
    ])
    out = calculate_metrics_by_recommended_status(df, lookup, event_year=2026)
    assert out["Yes, with hard evidence"]["conversion_rate"] == 100.0
    assert out["No, with hard evidence"]["conversion_rate"] == 0.0


# -- find_research_targets / find_outreach_targets -------------------------

def test_research_targets_only_T1_T2_with_unknown_status():
    venues = [
        _venue("T1 unknown", tier="Tier 1", recommended_status="Unknown"),
        _venue("T1 known", tier="Tier 1", recommended_status="Yes, with hard evidence"),
        _venue("T2 unknown", tier="Tier 2", recommended_status="Unknown"),
        _venue("T3 unknown", tier="Tier 3", recommended_status="Unknown"),  # excluded
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([])  # no inquiries → counts all 0
    targets = find_research_targets(venues, lookup, df, event_year=2026)
    names = {t["name"] for t in targets}
    assert names == {"T1 unknown", "T2 unknown"}


def test_outreach_targets_growth_target_AND_negative_status():
    venues = [
        _venue("Growth+No", growth_target=True, recommended_status="No, with hard evidence"),
        _venue("Growth+Unlikely", growth_target=True, recommended_status="Unlikely"),
        _venue("Growth+Unknown", growth_target=True, recommended_status="Unknown"),  # excluded
        _venue("NoGrowth+No", growth_target=False, recommended_status="No, with hard evidence"),  # excluded
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([])
    targets = find_outreach_targets(venues, lookup, df, event_year=2026)
    names = {t["name"] for t in targets}
    assert names == {"Growth+No", "Growth+Unlikely"}


# -- calculate_growth_target_activity --------------------------------------

def test_growth_target_activity_counts_inquiries_and_bookings():
    venues = [
        _venue("Casa Real", tier="Tier 2", growth_target=True),
        _venue("Plain Venue", tier="Tier 3", growth_target=False),
    ]
    lookup = build_venue_tier_lookup(venues)
    df = pd.DataFrame([
        _inquiry_row("12/15/26", "Casa Real", "Booked"),
        _inquiry_row("12/16/26", "Casa Real", "Didn't Book"),
        _inquiry_row("12/17/26", "Casa Real", "Cold"),
        _inquiry_row("12/18/26", "Plain Venue", "Booked"),  # not a growth target
    ])
    out = calculate_growth_target_activity(venues, lookup, df, event_year=2026)
    assert len(out) == 1
    assert out[0]["name"] == "Casa Real"
    assert out[0]["inquiries"] == 3
    assert out[0]["booked"] == 1


# -- bucket order constant -------------------------------------------------

def test_lead_time_buckets_constant_is_ordered():
    """Charts and tables iterate LEAD_TIME_BUCKETS in display order; lock the order."""
    assert LEAD_TIME_BUCKETS == ["<3 mo", "3-6 mo", "6-12 mo", "12+ mo"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
