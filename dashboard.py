"""
Big Fun DJ Operations Dashboard
A read-only status board showing booking pace, lead metrics, and capacity.
"""

import re
import time
from datetime import datetime, timedelta
from functools import lru_cache

import gspread
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from google.oauth2.service_account import Credentials

# =============================================================================
# CONFIGURATION
# =============================================================================

BOOKING_SNAPSHOTS_SHEET_ID = "1JV5S1hbtYcXhVoeqsYVw_nhUvRoOlSBt5BYZ0ffxFkU"
INQUIRY_TRACKER_SHEET_ID = "1ng-OytB9LJ8Fmfazju4cfFJRRa6bqfRIZA8GYEWhJRs"
AVAILABILITY_MATRIX_SHEET_ID = "1lXwHECkQJy7h87L5oKbo0hDTpalDgKFTbBQJ4pIerFo"

# FileMaker URL loaded from secrets (not in public repo)
def get_filemaker_url():
    try:
        return st.secrets["filemaker"]["base_url"]
    except (KeyError, FileNotFoundError):
        return ""  # Will fail gracefully if not configured

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# BIG FUN's standard event rate (post-January-2026 schedule). The modal and
# median rate from a sample of 63 events booked at the new rates. The mean of
# the same sample is $2,231 — lower because some events get policy-driven
# discounts (school events, shorter packages) that shouldn't dilute the
# typical-wedding baseline. Used as the deal-size constant in the
# pipeline-velocity formula and for "potential declined revenue" framing.
AVG_DEAL_SIZE = 2299

LEAD_TIME_BUCKETS = ["<3 mo", "3-6 mo", "6-12 mo", "12+ mo"]

# Resolutions that aren't sales failures (capacity/policy outcomes). Excluded
# from conversion-rate denominators and pipeline-velocity calculations so the
# numbers reflect actual sales performance, not how full the calendar is.
CAPACITY_RESOLUTIONS = {"Full", "We turn down"}

# Airtable Venues table (BIG FUN Disc Jockeys base). Field IDs are stable
# across renames; field names aren't.
AIRTABLE_BASE_ID = "appPMPQxGhQa6pWDz"
AIRTABLE_VENUES_TABLE_ID = "tblHtQx3eq0EFrRAq"
AIRTABLE_FIELD_NAME = "fldUtX1ExbnUCHBIu"
AIRTABLE_FIELD_FORMER_NAMES = "fldGsvjPqCRrhEdm1"
AIRTABLE_FIELD_TIER = "fldeseALwwueuZmcF"
AIRTABLE_FIELD_GROWTH_TARGET = "fld0ocfdRuSXnxp3M"
AIRTABLE_FIELD_RECOMMENDED_STATUS = "fld0JQJ2kvyySfKuD"
AIRTABLE_FIELD_WEDDING_VENUE = "fldt3DrdaWBnrYFsQ"

# =============================================================================
# AUTHENTICATION
# =============================================================================

@st.cache_resource
def get_google_client():
    """Initialize Google Sheets client with service account credentials."""
    try:
        # Try Streamlit Cloud secrets first
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except (KeyError, FileNotFoundError):
        # Fall back to local credentials file
        creds = Credentials.from_service_account_file(
            "your-credentials.json", scopes=SCOPES
        )
    return gspread.authorize(creds)


# =============================================================================
# DATA FETCHING
# =============================================================================

def get_airtable_pat():
    """Read the Airtable PAT from Streamlit Cloud secrets, falling back to
    ~/.airtable-pat for local dev. Mirrors the gcp_service_account pattern."""
    try:
        return str(st.secrets["airtable_pat"]).strip()
    except (KeyError, FileNotFoundError):
        from pathlib import Path
        local = Path.home() / ".airtable-pat"
        if local.exists():
            return local.read_text().strip()
        return ""


@st.cache_data(ttl=3600)
def get_venue_tiers_from_airtable():
    """Fetch the Venues table and return a list of {name, former_names, tier,
    growth_target, recommended_status} dicts. Returns [] if PAT isn't set."""
    pat = get_airtable_pat()
    if not pat:
        return []

    headers = {"Authorization": f"Bearer {pat}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_VENUES_TABLE_ID}"
    params = {"pageSize": 100, "returnFieldsByFieldId": "true"}
    out = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            f = rec.get("fields", {})
            name = f.get(AIRTABLE_FIELD_NAME, "")
            if not name:
                continue
            tier_raw = f.get(AIRTABLE_FIELD_TIER)
            tier_str = tier_raw.get("name") if isinstance(tier_raw, dict) else (tier_raw or "")
            rec_status = f.get(AIRTABLE_FIELD_RECOMMENDED_STATUS)
            rec_status_str = rec_status.get("name") if isinstance(rec_status, dict) else (rec_status or "")
            out.append({
                "name": name,
                "former_names": f.get(AIRTABLE_FIELD_FORMER_NAMES, "") or "",
                "tier": tier_str,
                "growth_target": bool(f.get(AIRTABLE_FIELD_GROWTH_TARGET, False)),
                "recommended_status": rec_status_str,
                "wedding_venue": bool(f.get(AIRTABLE_FIELD_WEDDING_VENUE, False)),
            })
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
    return out


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_year_comparison_data():
    """Fetch YoY booking comparison from Booking Snapshots sheet."""
    client = get_google_client()
    sheet = client.open_by_key(BOOKING_SNAPSHOTS_SHEET_ID)
    worksheet = sheet.worksheet("Year Comparison")
    
    # Use get_all_values() to handle any header weirdness
    all_values = worksheet.get_all_values()
    if not all_values:
        return pd.DataFrame()
    
    # First row is headers
    headers = all_values[0]
    
    # Create DataFrame with remaining rows
    df = pd.DataFrame(all_values[1:], columns=headers)
    return df


@st.cache_data(ttl=3600)
def get_inquiry_tracker_data():
    """Fetch all inquiry data from the Inquiry Tracker sheet."""
    client = get_google_client()
    sheet = client.open_by_key(INQUIRY_TRACKER_SHEET_ID)
    # Read the raw form source, not the derived "Master View" tab.
    # Master View is an array formula; gspread returns empty for some spilled cells.
    worksheet = sheet.worksheet("Form Responses 1")

    # Explicit range beats get_all_values() here — get_all_values has been
    # dropping columns B and C for reasons we haven't pinned down.
    all_values = worksheet.get_values("A1:P")
    if not all_values:
        return pd.DataFrame()
    
    # First row is headers
    headers = all_values[0]
    
    # Make headers unique by appending index to duplicates/empties
    seen = {}
    unique_headers = []
    for i, h in enumerate(headers):
        if h == '' or h in seen:
            # Create unique name for empty or duplicate
            base = h if h else f'Column_{i}'
            count = seen.get(base, 0)
            unique_headers.append(f"{base}_{count}" if count > 0 else base)
            seen[base] = count + 1
        else:
            unique_headers.append(h)
            seen[h] = 1
    
    # Create DataFrame with remaining rows
    df = pd.DataFrame(all_values[1:], columns=unique_headers)

    # Track pre-dedup count
    pre_dedup_count = len(df)

    # Count pre-dedup 2026 cancellations so the UI can show
    # "peak bookings" (current Booked + cancelled after booking).
    # Done before smart_dedup strips cancellations that paired with Bookeds.
    # Also split into pre-booking vs post-booking based on whether any Booked
    # row exists for the same (event date, venue) key.
    canceled_2026 = 0
    canceled_post_booking_2026 = 0
    canceled_pre_booking_2026 = 0
    if "Resolution" in df.columns and "Event Date" in df.columns and "Venue (if known)" in df.columns:
        def _is_2026(ed):
            s = str(ed).strip()
            if not s:
                return False
            for fmt in ["%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"]:
                try:
                    return pd.to_datetime(s, format=fmt).year == 2026
                except Exception:
                    continue
            dt = pd.to_datetime(s, errors="coerce")
            return pd.notna(dt) and dt.year == 2026

        is_2026_event = df["Event Date"].apply(_is_2026)
        canceled_2026 = int(((df["Resolution"] == "Canceled") & is_2026_event).sum())

        # Match the same key shape as smart_dedup so post-booking detection
        # agrees with how the deduplicator pairs cancellations with bookings.
        cancel_key = (
            df["Event Date"].astype(str).str.strip()
            + "|"
            + df["Venue (if known)"].astype(str).str.strip().str.lower()
        )
        booked_keys_2026 = set(cancel_key[(df["Resolution"] == "Booked") & is_2026_event])
        canceled_post_booking_2026 = int(
            (
                (df["Resolution"] == "Canceled")
                & is_2026_event
                & cancel_key.isin(booked_keys_2026)
            ).sum()
        )
        canceled_pre_booking_2026 = canceled_2026 - canceled_post_booking_2026
    
    # Deduplicate by (Event Date, Venue), with special handling for multiple bookings
    # - Multiple Booked entries = separate clients, keep all
    # - Canceled after any Booked = one cancellation, reduce count by 1
    # - Non-Booked only = keep newest
    if "Timestamp" in df.columns and "Event Date" in df.columns and "Venue (if known)" in df.columns:
        # Parse timestamp for sorting
        df["_parsed_timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        
        # Sort by timestamp descending (newest first)
        df = df.sort_values("_parsed_timestamp", ascending=False)
        
        # Smart deduplication
        def smart_dedup(group):
            if len(group) == 1:
                return group
            
            resolution_col = "Resolution" if "Resolution" in group.columns else None
            if not resolution_col:
                return group.head(1)
            
            # Find Booked and Canceled rows
            booked_mask = group[resolution_col].str.lower().str.strip() == "booked"
            canceled_mask = group[resolution_col].str.lower().str.strip() == "canceled"
            
            booked_rows = group[booked_mask].sort_values("_parsed_timestamp", ascending=False)
            canceled_rows = group[canceled_mask]
            
            if len(booked_rows) == 0:
                # No bookings - keep newest row only
                return group.head(1)
            
            # Count valid cancellations (timestamp after ANY booking)
            earliest_booking_ts = booked_rows["_parsed_timestamp"].min()
            valid_cancellations = 0
            for _, cancel_row in canceled_rows.iterrows():
                cancel_ts = cancel_row["_parsed_timestamp"]
                if pd.notna(cancel_ts) and pd.notna(earliest_booking_ts) and cancel_ts > earliest_booking_ts:
                    valid_cancellations += 1
            
            # Net bookings = booked - cancellations (minimum 0)
            net_bookings = max(0, len(booked_rows) - valid_cancellations)
            
            if net_bookings == 0:
                # All bookings canceled - return newest canceled row
                return canceled_rows.head(1) if len(canceled_rows) > 0 else group.head(1)
            
            # Return the newest N booked rows
            return booked_rows.head(net_bookings)
        
        # Group on a derived key so Event Date / Venue stay as real columns.
        # pandas 2.3+ drops grouping columns from apply() results by default,
        # which was silently removing these columns from the DataFrame.
        # Case-normalize the venue so "el Prado" and "el PRADO" group together
        # (so a Canceled row cancels its Booked counterpart even if the venue
        # casing differs between submissions).
        df["_dedup_key"] = (
            df["Event Date"].astype(str).str.strip()
            + "|"
            + df["Venue (if known)"].astype(str).str.strip().str.lower()
        )
        df = df.groupby("_dedup_key", group_keys=False).apply(smart_dedup)

        # Clean up temp columns (pandas 2.3+ may have already dropped _dedup_key
        # as the grouping column, so ignore missing).
        df = df.drop(columns=["_parsed_timestamp", "_dedup_key"], errors="ignore")
    
    # Store dedup stats in a special row (will be filtered out later)
    # Actually, let's add columns instead
    df["_dedup_pre"] = pre_dedup_count
    df["_dedup_post"] = len(df)
    df["_canceled_2026_predup"] = canceled_2026
    df["_canceled_post_booking_2026"] = canceled_post_booking_2026
    df["_canceled_pre_booking_2026"] = canceled_pre_booking_2026

    return df


@st.cache_data(ttl=3600)
def get_dj_booking_counts(year=2026):
    """Count BOOKED events per DJ from the Availability Matrix."""
    client = get_google_client()
    sheet = client.open_by_key(AVAILABILITY_MATRIX_SHEET_ID)
    
    try:
        worksheet = sheet.worksheet(str(year))
    except Exception:
        return {}
    
    all_values = worksheet.get_all_values()
    if not all_values:
        return {}
    
    # Column mappings based on year (from SYSTEM_REFERENCE.md)
    # 2026: A=Date, D=Henry, E=Woody, F=Paul, G=Stefano, H=Felipe, I=TBA, K=Stephanie
    # Columns are 0-indexed: D=3, E=4, F=5, G=6, H=7, I=8, K=10
    if year == 2026:
        dj_columns = {
            "Henry": 3,
            "Woody": 4,
            "Paul": 5,
            "Stefano": 6,
            "Felipe": 7,
            "Stephanie": 10
        }
        tba_col = 8
    elif year == 2027:
        # 2027: D=Henry, E=Woody, F=Paul, G=Stefano, H=Stephanie, I=TBA, L=Felipe
        dj_columns = {
            "Henry": 3,
            "Woody": 4,
            "Paul": 5,
            "Stefano": 6,
            "Stephanie": 7,
            "Felipe": 11
        }
        tba_col = 8
    else:
        # 2025: D=Henry, E=Woody, F=Paul, G=Stefano, H=Felipe, I=TBA, K=Stephanie
        dj_columns = {
            "Henry": 3,
            "Woody": 4,
            "Paul": 5,
            "Stefano": 6,
            "Felipe": 7,
            "Stephanie": 10
        }
        tba_col = 8
    
    # Count BOOKED for each DJ.
    # "BOOKED, BACKUP" (e.g., Woody at Nestldown for a morning gig + evening backup)
    # and "WEDFAIRE" (wedding fair) both count as a booked event.
    counts = {}
    for dj, col_idx in dj_columns.items():
        count = 0
        for row in all_values[1:]:  # Skip header
            if col_idx < len(row):
                cell_value = str(row[col_idx]).strip().upper()
                if (
                    cell_value == "BOOKED"
                    or cell_value == "WEDFAIRE"
                    or cell_value.startswith("BOOKED,")
                    or cell_value.startswith("BOOKED ")
                ):
                    count += 1
        counts[dj] = count
    
    # Count TBA (unassigned) bookings
    # TBA can be: "BOOKED", "BOOKED x 2", "AAG", "BOOKED, AAG", etc.
    tba_count = 0
    for row in all_values[1:]:
        if tba_col < len(row):
            cell_value = str(row[tba_col]).strip().upper()
            if not cell_value:
                continue
            
            added = 0
            # Count each BOOKED mention
            if "BOOKED X " in cell_value:
                # "BOOKED x 2" -> 2
                try:
                    num = int(cell_value.split("X")[1].strip().split()[0])
                    added = num
                except (IndexError, ValueError):
                    added = 1
            elif "BOOKED" in cell_value:
                added = 1
            
            # Add AAG if present (separate from BOOKED)
            if "AAG" in cell_value:
                if "BOOKED" not in cell_value:
                    added = 1  # Just AAG alone
                else:
                    added += 1  # AAG in addition to BOOKED
            
            if added > 0:
                tba_count += added
    
    counts["TBA"] = tba_count
    
    return counts


@st.cache_data(ttl=3600)
def get_upcoming_events(days_ahead=14):
    """Fetch upcoming events from FileMaker gig database."""
    filemaker_url = get_filemaker_url()
    if not filemaker_url:
        return []  # Skip if FileMaker URL not configured
    
    today = datetime.now()
    events = []
    
    # Query FileMaker for multiple days using the multi-day endpoint
    # Endpoint returns ±3 days (7 day window), step by 6 to ensure overlap
    for offset in range(0, days_ahead + 4, 6):  # +4 ensures we capture the end
        query_date = today + timedelta(days=offset)
        # Format date without leading zeros (works on all platforms)
        date_str = f"{query_date.month}/{query_date.day}/{query_date.year}"
        
        try:
            url = f"{filemaker_url}/availabilityMDjson.php?date={date_str}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    events.extend(data)
        except Exception as e:
            st.warning(f"Could not fetch events for {date_str}: {e}")
    
    # Deduplicate and filter to date range
    seen = set()
    unique_events = []
    end_date = today + timedelta(days=days_ahead)
    
    for event in events:
        event_key = (event.get("event_date"), event.get("venue_name"), event.get("client_name"))
        if event_key not in seen:
            seen.add(event_key)
            # Parse event date and filter
            try:
                event_date = datetime.strptime(event.get("event_date", ""), "%Y-%m-%d")
                if today.date() <= event_date.date() <= end_date.date():
                    unique_events.append(event)
            except ValueError:
                pass
    
    # Sort by date
    unique_events.sort(key=lambda x: x.get("event_date", ""))
    return unique_events


# =============================================================================
# DATA PROCESSING
# =============================================================================

def calculate_booking_pace(df):
    """Calculate current booking pace vs last year."""
    if "Day" not in df.columns:
        found = ", ".join(df.columns[:10])
        return None, None, None, f"Year Comparison missing 'Day' column. Found: {found}"

    today = datetime.now()

    # Get current year and last year - check both string and int column names
    current_year = today.year
    last_year = today.year - 1

    # Find the actual column names (might be int or string)
    current_col = None
    last_col = None
    for col in df.columns:
        if str(col) == str(current_year):
            current_col = col
        if str(col) == str(last_year):
            last_col = col
    
    if current_col is None:
        return None, None, None, f"Column '{current_year}' not found. Available: {list(df.columns)[:10]}"
    
    # Find today's row by matching the Day column
    # Sheet format: "mmm d" → "Feb 3" (no leading zero)
    # Use %-d for no leading zero on Mac/Linux
    try:
        today_month_day = today.strftime("%b %-d")  # "Feb 3"
    except ValueError:
        # Windows doesn't support %-d, fall back to manual
        today_month_day = today.strftime("%b %d").lstrip("0").replace(" 0", " ")
    
    # Find the most recent row that has data for the current year
    # (today's row might not be populated yet)
    best_row = None
    best_day = None
    
    for idx, row in df.iterrows():
        day_str = str(row.get("Day", "")).strip()
        current_val = row.get(current_col, "")
        
        # Skip rows without current year data
        # Empty cells may come back as "", None, or 0
        if current_val == "" or current_val is None or current_val == 0:
            continue
            
        try:
            # Parse the day string
            normalized = " ".join(day_str.split())
            parsed = datetime.strptime(f"{normalized} {today.year}", "%b %d %Y")
            
            # Only consider days up to today
            if parsed.date() <= today.date():
                best_row = row
                best_day = day_str
        except ValueError:
            continue
    
    today_row = best_row
    
    if today_row is None:
        sample_days = df["Day"].head(5).tolist() if "Day" in df.columns else []
        return None, None, None, f"No matching day found. Sample: {sample_days}"
    
    current_count = today_row.get(current_col, 0)
    last_year_count = today_row.get(last_col, 0) if last_col else 0
    
    # Handle empty or non-numeric values
    try:
        current_count = int(current_count) if current_count else 0
        last_year_count = int(last_year_count) if last_year_count else 0
    except (ValueError, TypeError):
        current_count = 0
        last_year_count = 0
    
    diff = current_count - last_year_count
    
    return current_count, last_year_count, diff, None


def create_booking_pace_chart(df, days=30):
    """Create a line chart comparing booking pace YoY for the last N days."""
    today = datetime.now()
    current_year = today.year
    last_year = current_year - 1
    
    # Find column names
    current_col = None
    last_col = None
    for col in df.columns:
        if str(col) == str(current_year):
            current_col = col
        if str(col) == str(last_year):
            last_col = col
    
    if current_col is None:
        return None
    
    # Build data for last N days
    chart_data = []
    
    for idx, row in df.iterrows():
        day_str = str(row.get("Day", "")).strip()
        current_val = row.get(current_col, "")
        last_val = row.get(last_col, "") if last_col else ""
        
        # Skip rows without current year data
        if current_val == "" or current_val is None or current_val == 0:
            continue
        
        try:
            normalized = " ".join(day_str.split())
            parsed = datetime.strptime(f"{normalized} {current_year}", "%b %d %Y")
            
            # Only include last N days up to today
            days_ago = (today.date() - parsed.date()).days
            if 0 <= days_ago <= days:
                chart_data.append({
                    "date": parsed,
                    "day_str": day_str,
                    str(current_year): int(current_val) if current_val else 0,
                    str(last_year): int(last_val) if last_val else 0
                })
        except (ValueError, TypeError):
            continue
    
    if not chart_data:
        return None
    
    # Sort by date
    chart_data.sort(key=lambda x: x["date"])
    
    # Create Plotly figure
    fig = go.Figure()
    
    dates = [d["day_str"] for d in chart_data]
    current_values = [d[str(current_year)] for d in chart_data]
    last_values = [d[str(last_year)] for d in chart_data]
    
    # 2026 line (primary)
    fig.add_trace(go.Scatter(
        x=dates,
        y=current_values,
        mode='lines+markers',
        name=str(current_year),
        line=dict(color='#00D4AA', width=3),
        marker=dict(size=6)
    ))
    
    # 2025 line (comparison)
    fig.add_trace(go.Scatter(
        x=dates,
        y=last_values,
        mode='lines+markers',
        name=str(last_year),
        line=dict(color='#888888', width=2, dash='dot'),
        marker=dict(size=4)
    ))
    
    # Style the chart
    fig.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#FFFFFF'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            showgrid=False,
            tickangle=-45,
            dtick=7  # Show every 7th label
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.1)'
        ),
        hovermode='x unified'
    )
    
    return fig


def create_booking_pace_chart_ytd(df):
    """Create a YTD line chart comparing booking pace YoY (weekly data points)."""
    today = datetime.now()
    current_year = today.year
    last_year = current_year - 1
    
    # Find column names
    current_col = None
    last_col = None
    for col in df.columns:
        if str(col) == str(current_year):
            current_col = col
        if str(col) == str(last_year):
            last_col = col
    
    if current_col is None:
        return None
    
    # Build weekly data points from Jan 1 to today
    chart_data = []
    
    for idx, row in df.iterrows():
        day_str = str(row.get("Day", "")).strip()
        current_val = row.get(current_col, "")
        last_val = row.get(last_col, "") if last_col else ""
        
        # Skip rows without current year data
        if current_val == "" or current_val is None or current_val == 0:
            continue
        
        try:
            normalized = " ".join(day_str.split())
            parsed = datetime.strptime(f"{normalized} {current_year}", "%b %d %Y")
            
            # Only include dates from Jan 1 to today
            if parsed.date() <= today.date():
                # Check if this is approximately a Monday (or first/last of visible range)
                is_monday = parsed.weekday() == 0
                is_first = parsed.month == 1 and parsed.day <= 3
                is_latest = (today.date() - parsed.date()).days <= 1
                
                if is_monday or is_first or is_latest:
                    chart_data.append({
                        "date": parsed,
                        "day_str": day_str,
                        str(current_year): int(current_val) if current_val else 0,
                        str(last_year): int(last_val) if last_val else 0
                    })
        except (ValueError, TypeError):
            continue
    
    if not chart_data:
        return None
    
    # Sort by date
    chart_data.sort(key=lambda x: x["date"])
    
    # Create Plotly figure
    fig = go.Figure()
    
    dates = [d["day_str"] for d in chart_data]
    current_values = [d[str(current_year)] for d in chart_data]
    last_values = [d[str(last_year)] for d in chart_data]
    
    # Current year line
    fig.add_trace(go.Scatter(
        x=dates,
        y=current_values,
        mode='lines+markers',
        name=str(current_year),
        line=dict(color='#00D4AA', width=3),
        marker=dict(size=6)
    ))
    
    # Last year line
    fig.add_trace(go.Scatter(
        x=dates,
        y=last_values,
        mode='lines+markers',
        name=str(last_year),
        line=dict(color='#888888', width=2, dash='dot'),
        marker=dict(size=4)
    ))
    
    # Style the chart
    fig.update_layout(
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#FFFFFF'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            showgrid=False,
            tickangle=-45
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.1)'
        ),
        hovermode='x unified'
    )
    
    return fig


REQUIRED_INQUIRY_COLUMNS = [
    "Event Date",
    "Inquiry Date",
    "Decision Date",
    "Resolution",
    "Venue (if known)",
    "Level of interaction",
    "Initial Contact",
]


def _check_required_columns(df, required, source):
    """Raise a clear error listing ALL missing columns, not just the first one hit."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{source} missing columns: {', '.join(missing)}")


def calculate_lead_metrics(df):
    """Calculate lead time and conversion metrics for 2026 events."""
    if df is None or df.empty:
        return {}
    _check_required_columns(df, REQUIRED_INQUIRY_COLUMNS, "Inquiry Tracker")
    # Filter for 2026 events (by Event Date, not Timestamp)
    def is_2026_event(event_date_str):
        if not event_date_str or str(event_date_str).strip() == "":
            return False
        try:
            # Try various date formats
            for fmt in ["%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"]:
                try:
                    dt = pd.to_datetime(event_date_str, format=fmt)
                    return dt.year == 2026
                except:
                    continue
            # Fallback to pandas auto-parse
            dt = pd.to_datetime(event_date_str, errors="coerce")
            return pd.notna(dt) and dt.year == 2026
        except:
            return False
    
    df_2026_events = df[df["Event Date"].apply(is_2026_event)].copy()
    
    if df_2026_events.empty:
        return {}
    
    # For conversion calculations, only use rows with BOTH Inquiry Date AND Decision Date
    df_with_dates = df_2026_events[
        (df_2026_events["Inquiry Date"].astype(str).str.strip() != "") &
        (df_2026_events["Decision Date"].astype(str).str.strip() != "")
    ].copy()
    
    if df_with_dates.empty:
        return {}
    
    metrics = {}
    
    # DEBUG: Track filtering steps
    metrics["_debug"] = {
        "total_2026_events": len(df_2026_events),
        "with_both_dates": len(df_with_dates),
        "booked_before_filter": len(df_2026_events[df_2026_events["Resolution"] == "Booked"]),
        "booked_with_dates": len(df_with_dates[df_with_dates["Resolution"] == "Booked"]),
    }
    
    # Find booked events missing dates
    booked_2026 = df_2026_events[df_2026_events["Resolution"] == "Booked"]
    booked_missing_inquiry = booked_2026[booked_2026["Inquiry Date"].astype(str).str.strip() == ""]
    booked_missing_decision = booked_2026[booked_2026["Decision Date"].astype(str).str.strip() == ""]
    
    metrics["_debug"]["booked_missing_inquiry_date"] = len(booked_missing_inquiry)
    metrics["_debug"]["booked_missing_decision_date"] = len(booked_missing_decision)
    
    if len(booked_missing_inquiry) > 0:
        # Get event dates and venues of missing
        missing_info = []
        for _, row in booked_missing_inquiry.iterrows():
            missing_info.append(f"{row.get('Event Date', '?')} - {row.get('Venue (if known)', '?')[:30]}")
        metrics["_debug"]["missing_inquiry_details"] = missing_info[:10]  # Limit to 10
    
    if len(booked_missing_decision) > 0:
        missing_info = []
        for _, row in booked_missing_decision.iterrows():
            missing_info.append(f"{row.get('Event Date', '?')} - {row.get('Venue (if known)', '?')[:30]}")
        metrics["_debug"]["missing_decision_details"] = missing_info[:10]
    
    # Total counts by resolution (only rows with both dates)
    resolution_counts = df_with_dates["Resolution"].value_counts().to_dict()
    metrics["total_inquiries"] = len(df_with_dates)
    metrics["booked"] = resolution_counts.get("Booked", 0)
    metrics["didnt_book"] = resolution_counts.get("Didn't Book", 0)
    metrics["full"] = resolution_counts.get("Full", 0)
    metrics["cold"] = resolution_counts.get("Cold", 0)
    metrics["we_turn_down"] = resolution_counts.get("We turn down", 0)

    # Pre-dedup count of 2026 cancellations (see get_inquiry_tracker_data).
    # Lets the UI show that total Booked passed through a higher peak.
    if "_canceled_2026_predup" in df.columns and len(df) > 0:
        metrics["canceled_pre_dedup_2026"] = int(df["_canceled_2026_predup"].iloc[0])
        metrics["canceled_post_booking_2026"] = int(df["_canceled_post_booking_2026"].iloc[0])
        metrics["canceled_pre_booking_2026"] = int(df["_canceled_pre_booking_2026"].iloc[0])
    else:
        metrics["canceled_pre_dedup_2026"] = 0
        metrics["canceled_post_booking_2026"] = 0
        metrics["canceled_pre_booking_2026"] = 0
    metrics["canceled"] = resolution_counts.get("Canceled", 0)
    
    # Conversion rate (simple)
    if metrics["total_inquiries"] > 0:
        metrics["conversion_rate_simple"] = metrics["booked"] / metrics["total_inquiries"] * 100
    else:
        metrics["conversion_rate_simple"] = 0
    
    # Conversion rate (adjusted) - excludes capacity constraints and non-engagements
    # Exclude: Full, We turn down, Cold ONLY when "Never acknowledged"
    cold_never_acknowledged = len(df_with_dates[
        (df_with_dates["Resolution"] == "Cold") &
        (df_with_dates["Level of interaction"].str.strip().str.lower() == "never acknowledged")
    ])
    
    adjusted_denominator = (metrics["total_inquiries"] 
                           - metrics["full"] 
                           - metrics["we_turn_down"]
                           - cold_never_acknowledged)
    if adjusted_denominator > 0:
        metrics["conversion_rate"] = metrics["booked"] / adjusted_denominator * 100
    else:
        metrics["conversion_rate"] = 0
    
    # Store for display
    metrics["cold_never_acknowledged"] = cold_never_acknowledged
    
    # Lead time calculations (Event Date - Inquiry Date)
    lead_times_by_resolution = {}
    days_to_decision_by_resolution = {}
    
    for _, row in df_with_dates.iterrows():
        resolution = row["Resolution"]
        
        # Calculate lead time (Event Date - Inquiry Date)
        try:
            event_date = pd.to_datetime(row["Event Date"], format="%m/%d/%y", errors="coerce")
            if pd.isna(event_date):
                event_date = pd.to_datetime(row["Event Date"], errors="coerce")
            
            inquiry_date = pd.to_datetime(row["Inquiry Date"], errors="coerce")
            
            if pd.notna(event_date) and pd.notna(inquiry_date):
                lead_time_days = (event_date - inquiry_date).days
                if lead_time_days >= 0:  # Sanity check
                    if resolution not in lead_times_by_resolution:
                        lead_times_by_resolution[resolution] = []
                    lead_times_by_resolution[resolution].append(lead_time_days)
        except Exception:
            pass
        
        # Calculate days to decision (Decision Date - Inquiry Date)
        try:
            decision_date = pd.to_datetime(row["Decision Date"], errors="coerce")
            inquiry_date = pd.to_datetime(row["Inquiry Date"], errors="coerce")
            
            if pd.notna(decision_date) and pd.notna(inquiry_date):
                days_to_decision = (decision_date - inquiry_date).days
                if days_to_decision >= 0:
                    if resolution not in days_to_decision_by_resolution:
                        days_to_decision_by_resolution[resolution] = []
                    days_to_decision_by_resolution[resolution].append(days_to_decision)
        except Exception:
            pass
    
    # Calculate averages and medians
    metrics["lead_times"] = {}
    for resolution, times in lead_times_by_resolution.items():
        if times:
            avg_days = sum(times) / len(times)
            sorted_times = sorted(times)
            median_days = sorted_times[len(sorted_times) // 2]
            metrics["lead_times"][resolution] = {
                "avg_days": avg_days,
                "avg_months": avg_days / 30.44,
                "median_days": median_days,
                "median_months": median_days / 30.44,
                "count": len(times)
            }
    
    metrics["days_to_decision"] = {}
    for resolution, times in days_to_decision_by_resolution.items():
        if times:
            metrics["days_to_decision"][resolution] = {
                "avg_days": sum(times) / len(times),
                "median_days": sorted(times)[len(times) // 2],
                "count": len(times)
            }
    
    # Conversion by source
    # Exclude Full and Turn-away from denominator (capacity constraints, not sales failures)
    source_counts = df_with_dates.groupby("Initial Contact")["Resolution"].value_counts().unstack(fill_value=0)
    metrics["by_source"] = {}
    for source in source_counts.index:
        row = source_counts.loc[source]
        booked = row.get("Booked", 0)
        full = row.get("Full", 0)
        turn_down = row.get("We turn down", 0)
        
        # Adjusted total excludes capacity constraints
        adjusted_total = row.sum() - full - turn_down
        
        if adjusted_total > 0:
            metrics["by_source"][source] = {
                "total": int(adjusted_total),
                "booked": int(booked),
                "conversion_rate": booked / adjusted_total * 100
            }
    
    # Level of interaction analysis
    # Exclude Full and Turn-away from denominator (capacity constraints, not sales failures)
    interaction_counts = df_with_dates.groupby("Level of interaction")["Resolution"].value_counts().unstack(fill_value=0)
    metrics["by_interaction"] = {}
    for interaction in interaction_counts.index:
        row = interaction_counts.loc[interaction]
        booked = row.get("Booked", 0)
        full = row.get("Full", 0)
        turn_down = row.get("We turn down", 0)
        
        # Adjusted total excludes capacity constraints
        adjusted_total = row.sum() - full - turn_down
        
        if adjusted_total > 0:
            metrics["by_interaction"][interaction] = {
                "total": int(adjusted_total),
                "booked": int(booked),
                "conversion_rate": booked / adjusted_total * 100
            }
    
    # AAG house DJ bookings (venue handoffs, not sales conversions)
    # These are: Allied Arts Guild venue, Booked, Never acknowledged
    venue_col = "Venue (if known)"
    if venue_col in df_2026_events.columns:
        # Match variations: "Allied Arts Guild", "AAG", etc.
        aag_bookings = df_2026_events[
            (df_2026_events[venue_col].astype(str).str.contains("Allied Arts|AAG", case=False, na=False, regex=True)) &
            (df_2026_events["Resolution"] == "Booked") &
            (df_2026_events["Level of interaction"].astype(str).str.lower().str.contains("never", na=False))
        ]
        metrics["aag_house_bookings"] = len(aag_bookings)
    else:
        metrics["aag_house_bookings"] = 0
    
    return metrics


# =============================================================================
# LEAD-TIME / VELOCITY / SURVIVAL METRICS
# =============================================================================

def _bucket_lead_time(days):
    """Map a lead-time-at-inquiry value (days) to a bucket label."""
    if days is None or pd.isna(days) or days < 0:
        return None
    if days < 90:
        return "<3 mo"
    if days < 180:
        return "3-6 mo"
    if days < 365:
        return "6-12 mo"
    return "12+ mo"


def _parse_event_date(value):
    """Inquiry Tracker's Event Date field uses several formats; try them in order."""
    s = str(value).strip()
    if not s:
        return pd.NaT
    for fmt in ["%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"]:
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            continue
    return pd.to_datetime(s, errors="coerce")


def _is_year(value, year):
    dt = _parse_event_date(value)
    return pd.notna(dt) and dt.year == year


def _iter_dated_rows(df, event_year=None):
    """Yield dicts for rows that have both Inquiry Date and Decision Date parsed."""
    for _, row in df.iterrows():
        if event_year is not None and not _is_year(row.get("Event Date", ""), event_year):
            continue
        inquiry_dt = pd.to_datetime(row.get("Inquiry Date"), errors="coerce")
        decision_dt = pd.to_datetime(row.get("Decision Date"), errors="coerce")
        if pd.isna(inquiry_dt) or pd.isna(decision_dt):
            continue
        event_dt = _parse_event_date(row.get("Event Date", ""))
        yield {
            "event_dt": event_dt,
            "inquiry_dt": inquiry_dt,
            "decision_dt": decision_dt,
            "resolution": str(row.get("Resolution", "")).strip(),
            "source": str(row.get("Initial Contact", "")).strip(),
        }


def calculate_lead_time_buckets(df, event_year=2026):
    """Conversion rate and decision velocity per lead-time-at-inquiry bucket."""
    rows = []
    for r in _iter_dated_rows(df, event_year):
        if pd.isna(r["event_dt"]):
            continue
        lead_days = (r["event_dt"] - r["inquiry_dt"]).days
        if lead_days < 0:
            continue
        rows.append({
            "bucket": _bucket_lead_time(lead_days),
            "resolution": r["resolution"],
            "lead_days": lead_days,
            "decision_days": (r["decision_dt"] - r["inquiry_dt"]).days,
        })

    if not rows:
        return {}

    bdf = pd.DataFrame(rows)
    out = {}
    for bucket in LEAD_TIME_BUCKETS:
        sub = bdf[bdf["bucket"] == bucket]
        if sub.empty:
            continue
        eligible = sub[~sub["resolution"].isin(CAPACITY_RESOLUTIONS)]
        booked = int((sub["resolution"] == "Booked").sum())
        denom = len(eligible)
        decisions = sub["decision_days"].dropna()
        out[bucket] = {
            "count": len(sub),
            "booked": booked,
            "eligible": denom,
            "conversion_rate": (booked / denom * 100) if denom > 0 else 0.0,
            "median_days_to_decision": float(decisions.median()) if len(decisions) > 0 else 0.0,
            "avg_days_to_decision": float(decisions.mean()) if len(decisions) > 0 else 0.0,
            "by_resolution": sub["resolution"].value_counts().to_dict(),
        }
    return out


def calculate_days_to_decision_by_source(df, event_year=2026, min_count=3):
    """Days-to-decision per Initial Contact source. Filters out sources with < min_count rows."""
    rows = []
    for r in _iter_dated_rows(df, event_year):
        days = (r["decision_dt"] - r["inquiry_dt"]).days
        if days < 0:
            continue
        rows.append({
            "source": r["source"] or "(blank)",
            "days": days,
            "resolution": r["resolution"],
        })

    if not rows:
        return {}

    sdf = pd.DataFrame(rows)
    out = {}
    for source, group in sdf.groupby("source"):
        if len(group) < min_count:
            continue
        booked = group[group["resolution"] == "Booked"]
        out[source] = {
            "count": int(len(group)),
            "booked_count": int(len(booked)),
            "median_days_all": float(group["days"].median()),
            "median_days_booked": float(booked["days"].median()) if len(booked) > 0 else None,
            "avg_days_booked": float(booked["days"].mean()) if len(booked) > 0 else None,
        }
    return out


def calculate_velocity_weekly(df, weeks=26, window_weeks=8, avg_deal_size=AVG_DEAL_SIZE):
    """
    Trailing-window pipeline velocity, computed weekly.

    Pipeline velocity = (qualified opps × avg deal size × win rate) / avg cycle days.
    For each of the last `weeks` weeks, look back `window_weeks` weeks of decisions
    and compute the formula. Uses all rows with both dates regardless of event year,
    so the metric reflects sales-process performance across the calendar.
    """
    rows = []
    for _, row in df.iterrows():
        inq = pd.to_datetime(row.get("Inquiry Date"), errors="coerce")
        dec = pd.to_datetime(row.get("Decision Date"), errors="coerce")
        if pd.isna(inq) or pd.isna(dec):
            continue
        cycle = (dec - inq).days
        if cycle < 0:
            continue
        rows.append({
            "decision_date": dec,
            "resolution": str(row.get("Resolution", "")).strip(),
            "cycle": cycle,
        })

    if not rows:
        return []

    vdf = pd.DataFrame(rows)
    today = datetime.now().date()

    weekly = []
    for w in range(weeks - 1, -1, -1):
        end = today - timedelta(days=7 * w)
        start = end - timedelta(days=7 * window_weeks)
        window = vdf[
            (vdf["decision_date"].dt.date > start)
            & (vdf["decision_date"].dt.date <= end)
        ]
        eligible = window[~window["resolution"].isin(CAPACITY_RESOLUTIONS)]
        opps = len(eligible)
        booked_count = int((eligible["resolution"] == "Booked").sum())
        win_rate = (booked_count / opps) if opps > 0 else 0.0
        avg_cycle = float(eligible["cycle"].mean()) if opps > 0 else 0.0
        velocity = ((opps * avg_deal_size * win_rate) / avg_cycle) if avg_cycle > 0 else 0.0
        weekly.append({
            "week_ending": end,
            "opps": opps,
            "booked": booked_count,
            "win_rate_pct": float(win_rate * 100),
            "avg_cycle_days": avg_cycle,
            "velocity_dollars_per_day": float(velocity),
        })
    return weekly


def calculate_survival_curve(df, event_year=2026, max_days=120):
    """
    For each eventual outcome (Booked, Lost), the cumulative % of that cohort
    that has decided by day N after inquiry. Plotted as 100% - this gives a
    "% still open" survival curve.
    """
    bookers = []
    losers = []
    for r in _iter_dated_rows(df, event_year):
        days = (r["decision_dt"] - r["inquiry_dt"]).days
        if days < 0:
            continue
        if r["resolution"] == "Booked":
            bookers.append(days)
        elif r["resolution"] in ("Didn't Book", "Cold"):
            losers.append(days)

    def cdf(days_list):
        if not days_list:
            return []
        sorted_days = sorted(days_list)
        n = len(sorted_days)
        out = []
        idx = 0
        for d in range(0, max_days + 1):
            while idx < n and sorted_days[idx] <= d:
                idx += 1
            out.append({
                "day": d,
                "pct_decided": idx / n * 100,
                "n_decided": idx,
                "n_total": n,
            })
        return out

    return {
        "Booked": cdf(bookers),
        "Lost": cdf(losers),
    }


def calculate_survival_curve_by_lead_time(df, event_year=2026, max_days=120):
    """For each lead-time-at-inquiry bucket, the cumulative % of bookers in
    that bucket who have decided by day N. Used to set bucket-specific
    stale-lead thresholds — short-lead couples decide faster than long-lead
    ones, so a single global threshold mis-fits both ends.

    Only bookers are tracked; the lost cohort would add visual noise to what's
    primarily a "when can I stop chasing" question.
    """
    bookers_by_bucket: dict[str, list[int]] = {b: [] for b in LEAD_TIME_BUCKETS}
    for r in _iter_dated_rows(df, event_year):
        if r["resolution"] != "Booked":
            continue
        if pd.isna(r["event_dt"]):
            continue
        lead_days = (r["event_dt"] - r["inquiry_dt"]).days
        if lead_days < 0:
            continue
        bucket = _bucket_lead_time(lead_days)
        if bucket is None:
            continue
        decision_days = (r["decision_dt"] - r["inquiry_dt"]).days
        if decision_days < 0:
            continue
        bookers_by_bucket[bucket].append(decision_days)

    curves: dict[str, list[dict]] = {}
    for bucket, days_list in bookers_by_bucket.items():
        if not days_list:
            continue
        sorted_days = sorted(days_list)
        n = len(sorted_days)
        curve = []
        idx = 0
        for d in range(0, max_days + 1):
            while idx < n and sorted_days[idx] <= d:
                idx += 1
            curve.append({
                "day": d,
                "pct_decided": idx / n * 100,
                "n_decided": idx,
                "n_total": n,
            })
        curves[bucket] = curve

    return curves


def normalize_venue_name(name):
    """Normalize a venue name for lookup: strip trailing parens, lowercase,
    remove punctuation, collapse whitespace. Matches the Inquiry Tracker's
    free-text 'Venue (if known)' to Airtable's canonical names + Former Names."""
    if not name:
        return ""
    s = str(name).strip()
    while True:
        new_s = re.sub(r"\s*\([^()]*\)\s*$", "", s).strip()
        if new_s == s or not new_s:
            break
        s = new_s
    s = re.sub(r"[^\w\s]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def build_venue_tier_lookup(venues):
    """Build a normalized-name → venue-info dict from Airtable rows.

    Indexes both the canonical Name and every Former Name. Earlier-listed
    entries win on collision, so the canonical name is registered first.
    """
    lookup = {}
    for v in venues or []:
        info = {
            "canonical_name": v["name"],
            "tier": v.get("tier", ""),
            "growth_target": bool(v.get("growth_target", False)),
            "recommended_status": v.get("recommended_status", "") or "Unknown",
            "wedding_venue": bool(v.get("wedding_venue", True)),
        }
        key = normalize_venue_name(v["name"])
        if key and key not in lookup:
            lookup[key] = info
        former = v.get("former_names", "") or ""
        for line in former.splitlines():
            fkey = normalize_venue_name(line)
            if fkey and fkey not in lookup:
                lookup[fkey] = info
    return lookup


def venue_to_tier_info(name, lookup, default_tier="Tier 4"):
    """Look up tier metadata for an inquiry's venue name. Returns None for
    blank inputs (so callers can exclude them from analysis); returns a default
    Tier 4 record for non-blank but unmatched names."""
    if not name or not str(name).strip():
        return None
    info = lookup.get(normalize_venue_name(name))
    if info:
        return info
    return {
        "canonical_name": str(name).strip(),
        "tier": default_tier,
        "growth_target": False,
        "recommended_status": "Unknown",
        "wedding_venue": True,
    }


def calculate_metrics_by_tier(df, tier_lookup, event_year=2026):
    """Conversion + decision velocity per tier. Tier 3 and Tier 4 are rolled
    into 'Tier 3+' to keep sample sizes meaningful. Inquiries at non-wedding
    venues (Wedding Venue? unchecked) are excluded so wedding-pipeline numbers
    aren't diluted by school events, single-org recurring bookings, etc."""
    rows = []
    for _, row in df.iterrows():
        if not _is_year(row.get("Event Date", ""), event_year):
            continue
        inquiry_dt = pd.to_datetime(row.get("Inquiry Date"), errors="coerce")
        decision_dt = pd.to_datetime(row.get("Decision Date"), errors="coerce")
        if pd.isna(inquiry_dt) or pd.isna(decision_dt):
            continue
        info = venue_to_tier_info(row.get("Venue (if known)", ""), tier_lookup)
        if info is None or not info.get("wedding_venue", True):
            continue
        event_dt = _parse_event_date(row.get("Event Date", ""))
        rows.append({
            "tier": info["tier"],
            "resolution": str(row.get("Resolution", "")).strip(),
            "decision_days": (decision_dt - inquiry_dt).days,
            "lead_days": (event_dt - inquiry_dt).days if pd.notna(event_dt) else None,
        })

    if not rows:
        return {}

    tdf = pd.DataFrame(rows)
    out = {}
    # Roll Tier 3 + Tier 4 together; cells get thin otherwise.
    tdf["tier_group"] = tdf["tier"].replace({"Tier 3": "Tier 3+", "Tier 4": "Tier 3+"})
    for group_name in ["Tier 1", "Tier 2", "Tier 3+"]:
        sub = tdf[tdf["tier_group"] == group_name]
        if sub.empty:
            continue
        eligible = sub[~sub["resolution"].isin(CAPACITY_RESOLUTIONS)]
        booked = int((sub["resolution"] == "Booked").sum())
        denom = len(eligible)
        decisions = sub["decision_days"].dropna()
        leads = sub["lead_days"].dropna()
        out[group_name] = {
            "count": len(sub),
            "eligible": denom,
            "booked": booked,
            "conversion_rate": (booked / denom * 100) if denom > 0 else 0.0,
            "median_days_to_decision": float(decisions.median()) if len(decisions) > 0 else 0.0,
            "median_lead_days": float(leads.median()) if len(leads) > 0 else 0.0,
        }
    return out


def calculate_metrics_by_recommended_status(df, tier_lookup, event_year=2026):
    """Conversion sliced by Airtable's Recommended Status (5-value enum).
    Excludes inquiries at non-wedding venues so the slicing reflects the
    wedding sales pipeline only."""
    rows = []
    for _, row in df.iterrows():
        if not _is_year(row.get("Event Date", ""), event_year):
            continue
        info = venue_to_tier_info(row.get("Venue (if known)", ""), tier_lookup)
        if info is None or not info.get("wedding_venue", True):
            continue
        rows.append({
            "status": info["recommended_status"] or "Unknown",
            "resolution": str(row.get("Resolution", "")).strip(),
        })

    if not rows:
        return {}

    sdf = pd.DataFrame(rows)
    out = {}
    for status, sub in sdf.groupby("status"):
        eligible = sub[~sub["resolution"].isin(CAPACITY_RESOLUTIONS)]
        booked = int((sub["resolution"] == "Booked").sum())
        denom = len(eligible)
        out[status] = {
            "count": len(sub),
            "eligible": denom,
            "booked": booked,
            "conversion_rate": (booked / denom * 100) if denom > 0 else 0.0,
        }
    return out


def find_research_targets(venues, tier_lookup, df, event_year=2026):
    """Tier 1/2 venues whose Recommended Status is Unknown — meaning we don't
    know whether we're on their preferred list. For each, count 2026 inquiries
    so the operator can prioritize venues sending us business."""
    inquiry_counts = {}
    for _, row in df.iterrows():
        if not _is_year(row.get("Event Date", ""), event_year):
            continue
        info = venue_to_tier_info(row.get("Venue (if known)", ""), tier_lookup)
        if info is None:
            continue
        inquiry_counts[info["canonical_name"]] = inquiry_counts.get(info["canonical_name"], 0) + 1

    targets = []
    for v in venues or []:
        if not v.get("wedding_venue", True):
            continue
        if v.get("tier") not in ("Tier 1", "Tier 2"):
            continue
        status = (v.get("recommended_status") or "Unknown")
        if status != "Unknown":
            continue
        targets.append({
            "name": v["name"],
            "tier": v["tier"],
            "inquiries_this_year": inquiry_counts.get(v["name"], 0),
        })
    targets.sort(key=lambda r: (-r["inquiries_this_year"], r["tier"], r["name"]))
    return targets


def find_outreach_targets(venues, tier_lookup, df, event_year=2026):
    """Growth-target venues we know we're not on the preferred list for —
    confirmed outreach candidates."""
    inquiry_counts = {}
    for _, row in df.iterrows():
        if not _is_year(row.get("Event Date", ""), event_year):
            continue
        info = venue_to_tier_info(row.get("Venue (if known)", ""), tier_lookup)
        if info is None:
            continue
        inquiry_counts[info["canonical_name"]] = inquiry_counts.get(info["canonical_name"], 0) + 1

    NEGATIVE_STATUSES = {"Unlikely", "No, with hard evidence"}
    targets = []
    for v in venues or []:
        if not v.get("wedding_venue", True):
            continue
        if not v.get("growth_target"):
            continue
        if v.get("recommended_status") not in NEGATIVE_STATUSES:
            continue
        targets.append({
            "name": v["name"],
            "tier": v.get("tier", ""),
            "recommended_status": v["recommended_status"],
            "inquiries_this_year": inquiry_counts.get(v["name"], 0),
        })
    targets.sort(key=lambda r: (-r["inquiries_this_year"], r["tier"], r["name"]))
    return targets


def calculate_growth_target_activity(venues, tier_lookup, df, event_year=2026):
    """For each Growth Target venue, count 2026 inquiries and how many booked.
    Lets Paul see whether his investment in those venues is showing up."""
    counts = {}
    for _, row in df.iterrows():
        if not _is_year(row.get("Event Date", ""), event_year):
            continue
        info = venue_to_tier_info(row.get("Venue (if known)", ""), tier_lookup)
        if info is None:
            continue
        d = counts.setdefault(info["canonical_name"], {"inquiries": 0, "booked": 0})
        d["inquiries"] += 1
        if str(row.get("Resolution", "")).strip() == "Booked":
            d["booked"] += 1

    out = []
    for v in venues or []:
        if not v.get("wedding_venue", True):
            continue
        if not v.get("growth_target"):
            continue
        c = counts.get(v["name"], {"inquiries": 0, "booked": 0})
        out.append({
            "name": v["name"],
            "tier": v.get("tier", ""),
            "recommended_status": v.get("recommended_status", "") or "Unknown",
            "inquiries": c["inquiries"],
            "booked": c["booked"],
        })
    out.sort(key=lambda r: (-r["inquiries"], -r["booked"], r["tier"], r["name"]))
    return out


# Capacity-status values from the Inquiry Tracker form. Encoded here so the
# transform doesn't need to query the form's allowed-value list at runtime.
FULL_REASON_TRUE_CAPACITY = "True Capacity"
FULL_REASON_ARTIFICIAL_CAP = "Artificial Cap (capacity known)"
FULL_REASON_AAG_HOLD = "Artificial Cap (holding for AAG)"


def calculate_full_reasons(df, event_year=2026, full_reason_col="Capacity Status (if Full)"):
    """
    Break Resolution=Full rows into True Capacity vs Artificial Cap reasons,
    plus an opportunity-cost dollar estimate for the artificial-cap subset.

    The two Artificial Cap variants represent dates we declined to take rather
    than dates we couldn't take — capacity exists but we chose not to use it.
    Multiplying the artificial count by AVG_DEAL_SIZE gives a blunt
    "policy-driven misses" number for hire-vs-policy conversations.

    Returns {} if the source column is missing entirely (older form versions).
    """
    if full_reason_col not in df.columns:
        return {}

    full_rows = df[
        (df["Resolution"] == "Full")
        & df["Event Date"].apply(lambda v: _is_year(v, event_year))
    ]
    total_full = len(full_rows)
    if total_full == 0:
        return {
            "breakdown": {},
            "total_full": 0,
            "true_capacity_total": 0,
            "artificial_total": 0,
            "artificial_potential_revenue": 0,
            "unspecified_total": 0,
        }

    raw = full_rows[full_reason_col].astype(str).str.strip()
    nonblank = raw[raw != ""]
    breakdown = nonblank.value_counts().to_dict()

    artificial_total = sum(v for k, v in breakdown.items() if k.startswith("Artificial Cap"))
    return {
        "breakdown": breakdown,
        "total_full": total_full,
        "true_capacity_total": int(breakdown.get(FULL_REASON_TRUE_CAPACITY, 0)),
        "artificial_total": artificial_total,
        "artificial_potential_revenue": artificial_total * AVG_DEAL_SIZE,
        "unspecified_total": total_full - sum(breakdown.values()),
    }


# =============================================================================
# CHARTS FOR NEW METRICS
# =============================================================================

def create_velocity_chart(weekly_data):
    """Line chart of pipeline velocity ($/day) over time."""
    if not weekly_data:
        return None

    fig = go.Figure()
    dates = [d["week_ending"].strftime("%b %d") for d in weekly_data]
    velocities = [d["velocity_dollars_per_day"] for d in weekly_data]

    fig.add_trace(go.Scatter(
        x=dates,
        y=velocities,
        mode="lines+markers",
        name="Velocity",
        line=dict(color="#00D4AA", width=3),
        marker=dict(size=6),
        hovertemplate="<b>Week ending %{x}</b><br>$%{y:,.0f}/day<extra></extra>",
    ))

    fig.update_layout(
        height=240,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(showgrid=False, tickangle=-45, dtick=4),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
            tickprefix="$",
            tickformat=",",
        ),
        hovermode="x unified",
        showlegend=False,
    )
    return fig


def create_lead_time_bucket_chart(buckets):
    """Bar chart of conversion rate by lead-time-at-inquiry bucket."""
    if not buckets:
        return None

    labels = [b for b in LEAD_TIME_BUCKETS if b in buckets]
    rates = [buckets[b]["conversion_rate"] for b in labels]
    counts = [buckets[b]["count"] for b in labels]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=labels,
        y=rates,
        marker_color="#00D4AA",
        text=[f"{r:.0f}% (n={c})" for r, c in zip(rates, counts)],
        textposition="auto",
        hovertemplate="<b>%{x}</b><br>Conversion: %{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(title="Lead time at inquiry"),
        yaxis=dict(
            title="Conversion rate",
            ticksuffix="%",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
        ),
        showlegend=False,
    )
    return fig


def create_survival_curve_chart(curves):
    """Line chart of % still undecided vs days since inquiry, split by eventual outcome."""
    if not curves:
        return None

    fig = go.Figure()
    color_map = {"Booked": "#00D4AA", "Lost": "#FF6B6B"}

    for category, points in curves.items():
        if not points:
            continue
        days = [p["day"] for p in points]
        still_open = [100 - p["pct_decided"] for p in points]
        n_total = points[0]["n_total"]
        fig.add_trace(go.Scatter(
            x=days,
            y=still_open,
            mode="lines",
            name=f"{category} (n={n_total})",
            line=dict(color=color_map.get(category, "#888"), width=3),
            hovertemplate=(
                f"<b>{category}</b><br>Day %{{x}}: %{{y:.0f}}%% still open<extra></extra>"
            ),
        ))

    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(
            title="Days since inquiry",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis=dict(
            title="% still undecided",
            ticksuffix="%",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
            range=[0, 100],
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def create_survival_curve_by_lead_time_chart(curves):
    """Multi-line survival curve, one per lead-time-at-inquiry bucket.
    Y-axis = % of that bucket's bookers still undecided at day N."""
    if not curves:
        return None

    fig = go.Figure()
    color_map = {
        "<3 mo": "#FF6B6B",       # red — fastest decisions expected
        "3-6 mo": "#FFB347",      # orange
        "6-12 mo": "#4ECDC4",     # teal
        "12+ mo": "#7B68EE",      # purple — slowest decisions expected
    }

    for bucket in LEAD_TIME_BUCKETS:
        if bucket not in curves:
            continue
        points = curves[bucket]
        if not points:
            continue
        days = [p["day"] for p in points]
        still_open = [100 - p["pct_decided"] for p in points]
        n_total = points[0]["n_total"]
        fig.add_trace(go.Scatter(
            x=days,
            y=still_open,
            mode="lines",
            name=f"{bucket} (n={n_total})",
            line=dict(color=color_map.get(bucket, "#888"), width=3),
            hovertemplate=(
                f"<b>{bucket}</b><br>Day %{{x}}: %{{y:.0f}}%% still open<extra></extra>"
            ),
        ))

    fig.update_layout(
        height=320,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(
            title="Days since inquiry",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis=dict(
            title="% of bookers still undecided",
            ticksuffix="%",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
            range=[0, 100],
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def create_full_reason_chart(full_reasons):
    """Bar chart of Full reasons with artificial caps colored as opportunity cost.
    Pre-tracking ('unspecified') Full events are excluded from the chart."""
    if not full_reasons or not full_reasons.get("breakdown"):
        return None

    breakdown = full_reasons["breakdown"]
    order = [
        FULL_REASON_TRUE_CAPACITY,
        FULL_REASON_ARTIFICIAL_CAP,
        FULL_REASON_AAG_HOLD,
    ]
    labels = [k for k in order if k in breakdown]
    values = [breakdown[k] for k in labels]

    color_map = {
        FULL_REASON_TRUE_CAPACITY: "#888888",        # neutral — real capacity
        FULL_REASON_ARTIFICIAL_CAP: "#FFB347",       # opportunity cost
        FULL_REASON_AAG_HOLD: "#FF8C42",             # opportunity cost (slightly darker)
    }
    colors = [color_map[k] for k in labels]

    display_labels = [
        "True Capacity" if k == FULL_REASON_TRUE_CAPACITY
        else "Artificial Cap (known)" if k == FULL_REASON_ARTIFICIAL_CAP
        else "Artificial Cap (AAG hold)"
        for k in labels
    ]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=display_labels,
        y=values,
        marker_color=colors,
        text=values,
        textposition="auto",
        hovertemplate="<b>%{x}</b><br>%{y} events<extra></extra>",
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(title="Reason"),
        yaxis=dict(
            title="Events",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
        ),
        showlegend=False,
    )
    return fig


def get_dj_initials(dj_name):
    """Convert DJ full name to initials."""
    if not dj_name or dj_name == "Unassigned":
        return "TBA"
    
    name_lower = dj_name.lower()
    if "henry" in name_lower:
        return "HK"
    elif "woody" in name_lower:
        return "WM"
    elif "paul" in name_lower:
        return "PB"
    elif "stefano" in name_lower:
        return "SB"
    elif "felipe" in name_lower:
        return "FS"
    elif "stephanie" in name_lower:
        return "SD"
    return "??"


# =============================================================================
# DASHBOARD UI
# =============================================================================

def main():
    st.set_page_config(
        page_title="Big Fun DJ Operations",
        page_icon="🎧",
        layout="wide",
    )
    
    st.title("🎧 Big Fun DJ Operations")
    st.caption(f"Last refreshed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    
    # Add refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    with st.expander("⬇️ Export Data"):
        export_col1, export_col2 = st.columns(2)
        with export_col1:
            try:
                export_df = get_inquiry_tracker_data()
                st.download_button(
                    label="Inquiry Tracker (CSV)",
                    data=export_df.to_csv(index=False),
                    file_name=f"inquiry-tracker-{datetime.now().strftime('%Y-%m-%d')}.csv",
                    mime="text/csv",
                )
            except Exception as e:
                st.error(f"Could not load inquiry data: {str(e)[:80]}")
        with export_col2:
            try:
                export_yoy = get_year_comparison_data()
                st.download_button(
                    label="Booking Pace YoY (CSV)",
                    data=export_yoy.to_csv(index=False),
                    file_name=f"booking-pace-{datetime.now().strftime('%Y-%m-%d')}.csv",
                    mime="text/csv",
                )
            except Exception as e:
                st.error(f"Could not load booking data: {str(e)[:80]}")

    st.divider()
    
    # ==========================================================================
    # ROW 1: Booking Pace + Inquiries Summary
    # ==========================================================================
    
    # Pre-calculate metrics for use across sections
    inquiry_df = None
    metrics = {}
    try:
        inquiry_df = get_inquiry_tracker_data()
        metrics = calculate_lead_metrics(inquiry_df)
    except KeyError as e:
        st.warning(f"⚠️ {e}")
        if inquiry_df is not None:
            st.caption(f"Columns found: {', '.join(inquiry_df.columns[:15])}")
    except Exception as e:
        st.warning(f"Could not load inquiry data: {str(e)[:100]}")
    
    # Load year comparison data for pace metrics and chart
    yoy_df = None
    try:
        yoy_df = get_year_comparison_data()
    except Exception as e:
        pass  # Will show error in the booking pace section
    
    col1, col2 = st.columns(2)
    
    # Booking Pace — live 2026 event count from Inquiry Tracker (event-based).
    # Booking Snapshots is kept only for the historical 2025 comparison, since
    # 2025 events were not tracked in the inquiry form. Starting with the
    # 2027 vs 2026 comparison, this can switch entirely to Inquiry Tracker.
    with col1:
        st.subheader("📈 Booking Pace")
        try:
            current = metrics.get("booked") if metrics else None
            last_year = None
            if yoy_df is not None and not yoy_df.empty:
                _, last_year, _, _ = calculate_booking_pace(yoy_df)

            if current is None:
                st.info("No pace data available yet")
            else:
                diff = (current - last_year) if last_year is not None else None
                st.metric(
                    label=f"2026 Booked (as of today)",
                    value=current,
                    delta=f"{diff:+d} vs 2025" if diff else None,
                    delta_color="normal"
                )
                if last_year is not None:
                    st.caption(f"Same time 2025: {last_year}")
        except Exception as e:
            st.error(f"Could not load booking pace: {type(e).__name__}: {str(e)[:100]}")
    
    # Inquiries Summary
    with col2:
        st.subheader("📊 2026 Inquiries")
        if metrics:
            st.metric("Total Inquiries", metrics.get("total_inquiries", 0))
            
            sub_col1, sub_col2 = st.columns(2)
            with sub_col1:
                booked = metrics.get("booked", 0)
                canceled_post = metrics.get("canceled_post_booking_2026", 0)
                canceled_pre = metrics.get("canceled_pre_booking_2026", 0)
                st.metric("Booked", booked)
                caption_lines = []
                if canceled_post > 0:
                    caption_lines.append(
                        f"{canceled_post} canceled after booking (peak: {booked + canceled_post})"
                    )
                if canceled_pre > 0:
                    caption_lines.append(f"{canceled_pre} canceled before booking")
                if caption_lines:
                    st.caption(" • ".join(caption_lines))
                st.metric("Didn't Book", metrics.get("didnt_book", 0))
            with sub_col2:
                st.metric("Full/Turn-away", metrics.get("full", 0) + metrics.get("we_turn_down", 0))
                st.metric("Cold/Ghosted", metrics.get("cold", 0))
            
            # Debug section
            if metrics.get("_debug"):
                with st.expander("🔍 Debug: Filtering details"):
                    debug = metrics["_debug"]
                    
                    # Show dedup stats if available
                    if inquiry_df is not None and "_dedup_pre" in inquiry_df.columns:
                        pre = inquiry_df["_dedup_pre"].iloc[0] if len(inquiry_df) > 0 else "?"
                        post = inquiry_df["_dedup_post"].iloc[0] if len(inquiry_df) > 0 else "?"
                        removed = pre - post if isinstance(pre, int) and isinstance(post, int) else "?"
                        st.write(f"**Deduplication:** {pre} rows → {post} rows ({removed} duplicates removed)")
                        st.write("---")
                    
                    st.write(f"Total 2026 events in tracker: {debug.get('total_2026_events', '?')}")
                    st.write(f"Booked (before date filter): {debug.get('booked_before_filter', '?')}")
                    st.write(f"With both Inquiry+Decision dates: {debug.get('with_both_dates', '?')}")
                    st.write(f"Booked (after date filter): {debug.get('booked_with_dates', '?')}")
                    st.write("---")
                    st.write(f"Booked missing Inquiry Date: {debug.get('booked_missing_inquiry_date', 0)}")
                    if debug.get('missing_inquiry_details'):
                        for item in debug['missing_inquiry_details']:
                            st.text(f"  • {item}")
                    st.write(f"Booked missing Decision Date: {debug.get('booked_missing_decision_date', 0)}")
                    if debug.get('missing_decision_details'):
                        for item in debug['missing_decision_details']:
                            st.text(f"  • {item}")
        else:
            st.info("No inquiry data available")
    
    st.divider()
    
    # ==========================================================================
    # ROW 2: Conversion (all metrics)
    # ==========================================================================
    
    st.subheader("🎯 Conversion")
    
    if metrics:
        # Top row: Overall rate + by source
        conv_col1, conv_col2 = st.columns(2)
        
        with conv_col1:
            conversion = metrics.get("conversion_rate", 0)
            conversion_simple = metrics.get("conversion_rate_simple", 0)
            
            st.metric("Overall Conversion Rate", f"{conversion:.0f}%")
            st.caption(f"Excludes: Full, Turn-away, Cold (no response)")
            st.caption(f"Simple (all inquiries): {conversion_simple:.0f}%")
        
        with conv_col2:
            st.markdown("**By Lead Source:**")
            by_source = metrics.get("by_source", {})
            for source, data in sorted(by_source.items(), key=lambda x: -x[1]["conversion_rate"]):
                if data["total"] >= 3:  # Only show sources with meaningful volume
                    st.text(f"{source[:20]}: {data['conversion_rate']:.0f}% ({data['booked']}/{data['total']})")
        
        # Bottom row: By interaction level
        st.markdown("**By Interaction Level:**")
        
        if metrics.get("by_interaction"):
            by_interaction = metrics.get("by_interaction", {})
            
            # Order by typical sales funnel (excluding "Never acknowledged" - those are AAG handoffs)
            interaction_order = [
                "Only acknowledged",
                "Meaningful email interaction",
                "Had phone call/video chat"
            ]
            
            # Find matching keys (case-insensitive partial match)
            matched_interactions = []
            for target in interaction_order:
                for actual_key in by_interaction.keys():
                    if target.lower() in actual_key.lower() or actual_key.lower() in target.lower():
                        matched_interactions.append((target, actual_key))
                        break
            
            if matched_interactions:
                # Add AAG column at the end
                cols = st.columns(len(matched_interactions) + 1)
                
                for idx, (label, actual_key) in enumerate(matched_interactions):
                    data = by_interaction[actual_key]
                    with cols[idx]:
                        short_label = label.replace("Meaningful email interaction", "Email exchange").replace("Had phone call/video chat", "Phone/video call")
                        st.metric(
                            label=short_label,
                            value=f"{data['conversion_rate']:.0f}%",
                            help=f"{data['booked']} booked / {data['total']} total"
                        )
                
                # AAG house DJ bookings (separate from sales funnel)
                with cols[-1]:
                    aag_count = metrics.get("aag_house_bookings", 0)
                    st.metric(
                        label="AAG (house DJ)",
                        value=aag_count,
                        help="Allied Arts Guild bookings via venue handoff"
                    )
    else:
        st.info("No conversion data available")
    
    st.divider()
    
    # ==========================================================================
    # ROW 3: Booking Pace Charts
    # ==========================================================================
    
    # Booking Pace Charts
    try:
        if yoy_df is not None and not yoy_df.empty:
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.caption("**Year to Date (weekly)**")
                ytd_chart = create_booking_pace_chart_ytd(yoy_df)
                if ytd_chart:
                    st.plotly_chart(ytd_chart, use_container_width=True)
            
            with chart_col2:
                st.caption("**Last 30 Days (daily)**")
                daily_chart = create_booking_pace_chart(yoy_df, days=30)
                if daily_chart:
                    st.plotly_chart(daily_chart, use_container_width=True)
    except Exception as e:
        st.caption(f"Could not load pace charts: {str(e)[:50]}")
    
    st.divider()
    
    # ==========================================================================
    # ROW 4: Upcoming Events
    # ==========================================================================
    
    st.subheader("📅 Upcoming Events (Next 14 Days)")
    
    try:
        events = get_upcoming_events(14)
        
        if events:
            # Group by date
            events_by_date = {}
            for event in events:
                date = event.get("event_date", "Unknown")
                if date not in events_by_date:
                    events_by_date[date] = []
                events_by_date[date].append(event)
            
            # Display in columns
            cols = st.columns(min(len(events_by_date), 4))
            
            for idx, (date, day_events) in enumerate(sorted(events_by_date.items())):
                col_idx = idx % 4
                with cols[col_idx]:
                    # Format date
                    try:
                        dt = datetime.strptime(date, "%Y-%m-%d")
                        formatted_date = f"{dt.strftime('%a %b')} {dt.day}"  # "Sat Feb 3"
                    except ValueError:
                        formatted_date = date
                    
                    st.markdown(f"**{formatted_date}**")
                    
                    for event in day_events:
                        dj = event.get("assigned_dj", "TBA")
                        initials = get_dj_initials(dj)
                        venue = event.get("venue_name", "Unknown venue")
                        # Truncate venue name
                        if len(venue) > 20:
                            venue = venue[:17] + "..."
                        
                        st.text(f"[{initials}] {venue}")
                    st.text("")  # Spacer
        else:
            st.info("No upcoming events found")
    except Exception as e:
        st.error(f"Could not load upcoming events: {e}")
    
    st.divider()
    
    # ==========================================================================
    # ROW 5: DJ Bookings by Person
    # ==========================================================================
    
    st.subheader("🎧 Events Booked by DJ (2026)")
    
    try:
        dj_counts = get_dj_booking_counts(2026)
        
        if dj_counts:
            # Separate TBA from assigned DJs
            tba_count = dj_counts.pop("TBA", 0)
            
            # Sort assigned DJs by count descending
            sorted_djs = sorted(dj_counts.items(), key=lambda x: -x[1])
            
            # Create columns for each DJ
            cols = st.columns(len(sorted_djs))
            
            for idx, (dj_name, count) in enumerate(sorted_djs):
                with cols[idx]:
                    st.metric(label=dj_name, value=count)
            
            # Show totals
            assigned_total = sum(dj_counts.values())
            st.caption(f"Assigned: {assigned_total} • Unassigned (TBA): {tba_count} • Total: {assigned_total + tba_count}")
        else:
            st.info("No booking data available")
    except Exception as e:
        st.error(f"Could not load DJ bookings: {str(e)[:100]}")
    
    st.divider()
    
    # ==========================================================================
    # ROW 6: Lead Time Analysis
    # ==========================================================================
    
    st.subheader("⏱️ Lead Time Analysis (2026)")
    
    if metrics and metrics.get("lead_times"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Lead Time by Outcome**")
            lead_times = metrics.get("lead_times", {})
            
            # Create a simple table
            lt_data = []
            for resolution, data in lead_times.items():
                lt_data.append({
                    "Outcome": resolution,
                    "Median": f"{data['median_months']:.1f} mo",
                    "Avg": f"{data['avg_months']:.1f} mo",
                    "Count": data["count"]
                })
            
            if lt_data:
                lt_df = pd.DataFrame(lt_data)
                lt_df = lt_df.sort_values("Count", ascending=False)
                st.dataframe(lt_df, hide_index=True, use_container_width=True)
        
        with col2:
            st.markdown("**Days to Decision by Outcome**")
            days_to_dec = metrics.get("days_to_decision", {})
            
            dtd_data = []
            for resolution, data in days_to_dec.items():
                dtd_data.append({
                    "Outcome": resolution,
                    "Avg Days": f"{data['avg_days']:.0f}",
                    "Median Days": f"{data['median_days']:.0f}",
                    "Count": data["count"]
                })
            
            if dtd_data:
                dtd_df = pd.DataFrame(dtd_data)
                dtd_df = dtd_df.sort_values("Count", ascending=False)
                st.dataframe(dtd_df, hide_index=True, use_container_width=True)
    else:
        st.info("Lead time data requires both Inquiry Date and Decision Date fields")

    # ==========================================================================
    # ROW 7: Pipeline Velocity (8-week trailing window, weekly cadence)
    # ==========================================================================

    st.divider()
    st.subheader("⚡ Pipeline Velocity")
    st.caption(
        f"(Qualified opps × ${AVG_DEAL_SIZE:,} avg deal × win rate) ÷ avg cycle days. "
        "Weekly snapshots over the last 6 months, each computed on a trailing 8-week window."
    )

    velocity_weekly = []
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            velocity_weekly = calculate_velocity_weekly(inquiry_df)
        except Exception as e:
            st.caption(f"Velocity calc failed: {str(e)[:100]}")

    if velocity_weekly:
        latest = velocity_weekly[-1]
        # Compare to ~1 month back (4 weekly snapshots)
        prior = velocity_weekly[-5] if len(velocity_weekly) >= 5 else velocity_weekly[0]

        v_col1, v_col2, v_col3 = st.columns(3)
        with v_col1:
            delta = latest["velocity_dollars_per_day"] - prior["velocity_dollars_per_day"]
            st.metric(
                "Velocity (this week)",
                f"${latest['velocity_dollars_per_day']:,.0f}/day",
                delta=f"{delta:+,.0f}/day vs 4 wk ago" if abs(delta) >= 1 else None,
            )
        with v_col2:
            st.metric("Win rate (window)", f"{latest['win_rate_pct']:.0f}%")
        with v_col3:
            st.metric("Avg cycle (window)", f"{latest['avg_cycle_days']:.0f} days")

        v_chart = create_velocity_chart(velocity_weekly)
        if v_chart:
            st.plotly_chart(v_chart, use_container_width=True)
    else:
        st.info("Not enough decided inquiries to compute velocity")

    # ==========================================================================
    # ROW 8: Conversion by Lead Time at Inquiry
    # ==========================================================================

    st.divider()
    st.subheader("📅 Conversion by Lead Time at Inquiry (2026)")
    st.caption(
        "How far out a couple was from their event when they inquired vs. how often they "
        "booked and how fast they decided. Excludes Full / Turn-away from the denominator."
    )

    lead_buckets = {}
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            lead_buckets = calculate_lead_time_buckets(inquiry_df)
        except Exception as e:
            st.caption(f"Bucket calc failed: {str(e)[:100]}")

    if lead_buckets:
        lt_col1, lt_col2 = st.columns([3, 2])

        with lt_col1:
            lt_chart = create_lead_time_bucket_chart(lead_buckets)
            if lt_chart:
                st.plotly_chart(lt_chart, use_container_width=True)

        with lt_col2:
            st.markdown("**Decision speed by bucket**")
            speed_rows = []
            for b in LEAD_TIME_BUCKETS:
                if b not in lead_buckets:
                    continue
                data = lead_buckets[b]
                speed_rows.append({
                    "Lead time": b,
                    "Median days": f"{data['median_days_to_decision']:.0f}",
                    "Booked / Eligible": f"{data['booked']}/{data['eligible']}",
                })
            if speed_rows:
                st.dataframe(
                    pd.DataFrame(speed_rows),
                    hide_index=True,
                    use_container_width=True,
                )
    else:
        st.info("No lead-time bucket data available")

    # ==========================================================================
    # ROW 9: Days to Decision by Lead Source
    # ==========================================================================

    st.divider()
    st.subheader("🎯 Decision Velocity by Lead Source (2026)")
    st.caption(
        "How fast leads from each source actually decide. Sources with fewer than "
        "3 decisions are hidden."
    )

    source_dtd = {}
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            source_dtd = calculate_days_to_decision_by_source(inquiry_df)
        except Exception as e:
            st.caption(f"Source calc failed: {str(e)[:100]}")

    if source_dtd:
        source_rows = []
        for source, data in source_dtd.items():
            source_rows.append({
                "Source": source[:30],
                "Median days (booked)": (
                    f"{data['median_days_booked']:.0f}"
                    if data["median_days_booked"] is not None else "—"
                ),
                "Median days (all)": f"{data['median_days_all']:.0f}",
                "Booked / Total": f"{data['booked_count']}/{data['count']}",
            })

        # Sort by booked-median ascending; sources with no bookings sink to the bottom
        def sort_key(r):
            v = r["Median days (booked)"]
            return float(v) if v != "—" else float("inf")

        source_rows.sort(key=sort_key)
        st.dataframe(
            pd.DataFrame(source_rows),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No source data available")

    # ==========================================================================
    # ROW 10: Survival / Decision Curve
    # ==========================================================================

    st.divider()
    st.subheader("📉 Decision Curve (2026)")
    st.caption(
        "Of couples who eventually booked or were lost, what % were still undecided "
        "at day N. Booked drops fast; Lost takes longer."
    )

    survival = {}
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            survival = calculate_survival_curve(inquiry_df)
        except Exception as e:
            st.caption(f"Survival calc failed: {str(e)[:100]}")

    if survival and (survival.get("Booked") or survival.get("Lost")):
        s_chart = create_survival_curve_chart(survival)
        if s_chart:
            st.plotly_chart(s_chart, use_container_width=True)
    else:
        st.info("No survival data available")

    # ROW 10b: Decision Curve faceted by lead-time-at-inquiry bucket.
    # Used for setting bucket-specific stale-lead thresholds in MailMaven.
    st.divider()
    st.subheader("📉 Decision Curve by Lead Time (2026 bookers)")
    st.caption(
        "Same survival curve, but split by how far out the couple was when "
        "they inquired. Short-lead couples decide faster than long-lead ones — "
        "so 'when can I stop chasing' should vary by bucket. Read the day where "
        "each curve flattens and use that as your stale-lead threshold for "
        "that bucket."
    )

    survival_by_lt = {}
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            survival_by_lt = calculate_survival_curve_by_lead_time(inquiry_df)
        except Exception as e:
            st.caption(f"Faceted survival calc failed: {str(e)[:100]}")

    if survival_by_lt:
        slt_chart = create_survival_curve_by_lead_time_chart(survival_by_lt)
        if slt_chart:
            st.plotly_chart(slt_chart, use_container_width=True)
    else:
        st.info("No bucket-faceted survival data available")

    # ==========================================================================
    # ROW 11: Capacity Reality (Full reasons)
    # ==========================================================================

    st.divider()
    st.subheader("🏗️ Capacity Reality (2026)")
    st.caption(
        "Splits Full events into True Capacity (we genuinely had no DJ available) "
        "vs Artificial Cap (we declined the date despite having capacity, including "
        "dates held for AAG). Artificial caps represent revenue we chose not to take."
    )

    full_reasons = {}
    if inquiry_df is not None and not inquiry_df.empty:
        try:
            full_reasons = calculate_full_reasons(inquiry_df)
        except Exception as e:
            st.caption(f"Capacity Reality calc failed: {str(e)[:100]}")

    if full_reasons and full_reasons.get("total_full", 0) > 0:
        cap_col1, cap_col2, cap_col3 = st.columns(3)
        with cap_col1:
            categorized = full_reasons["true_capacity_total"] + full_reasons["artificial_total"]
            st.metric("Categorized Full (2026)", categorized)
            unspec = full_reasons.get("unspecified_total", 0)
            if unspec > 0:
                st.caption(f"({unspec} additional Full rows predate the Capacity Status field)")
        with cap_col2:
            artificial = full_reasons["artificial_total"]
            true_cap = full_reasons["true_capacity_total"]
            st.metric("Artificial Cap", artificial)
            if artificial + true_cap > 0:
                share = artificial / (artificial + true_cap) * 100
                st.caption(f"{share:.0f}% of categorized Fulls are policy-driven")
        with cap_col3:
            st.metric(
                "Potential revenue declined",
                f"${full_reasons['artificial_potential_revenue']:,.0f}",
            )
            st.caption(f"= {artificial} × ${AVG_DEAL_SIZE:,} avg deal")

        cap_chart = create_full_reason_chart(full_reasons)
        if cap_chart:
            st.plotly_chart(cap_chart, use_container_width=True)
    else:
        st.info("No 2026 Full events to analyze yet")

    # ==========================================================================
    # ROWS 12-14: Venue tier slicing (reads Airtable Venues table)
    # ==========================================================================

    venues = []
    tier_lookup = {}
    try:
        venues = get_venue_tiers_from_airtable()
        tier_lookup = build_venue_tier_lookup(venues)
    except Exception as e:
        st.caption(f"Could not load venue tiers from Airtable: {str(e)[:120]}")

    if venues and inquiry_df is not None and not inquiry_df.empty:
        # ROW 12: Conversion by Venue Tier
        st.divider()
        st.subheader("🏛️ Conversion by Venue Tier (2026)")
        excluded_count = sum(1 for v in venues if not v.get("wedding_venue", True))
        if excluded_count > 0:
            st.caption(
                f"Slices conversion and decision velocity by venue tier "
                f"(see venue-tier-framework.md). Tier 3 and Tier 4 rolled together "
                f"for sample-size discipline. **{excluded_count} non-wedding venues** "
                "(schools, single-org recurring, etc.) excluded from wedding-pipeline analytics."
            )
        else:
            st.caption(
                "Slices conversion and decision velocity by venue tier "
                "(see venue-tier-framework.md). Tier 3 and Tier 4 rolled together "
                "for sample-size discipline."
            )
        try:
            tier_metrics = calculate_metrics_by_tier(inquiry_df, tier_lookup)
        except Exception as e:
            tier_metrics = {}
            st.caption(f"Tier calc failed: {str(e)[:100]}")
        if tier_metrics:
            tier_rows = []
            for group in ["Tier 1", "Tier 2", "Tier 3+"]:
                if group not in tier_metrics:
                    continue
                m = tier_metrics[group]
                tier_rows.append({
                    "Tier": group,
                    "Conversion": f"{m['conversion_rate']:.0f}%",
                    "Median days to decision": f"{m['median_days_to_decision']:.0f}",
                    "Median lead time (days)": f"{m['median_lead_days']:.0f}",
                    "Booked / Eligible": f"{m['booked']}/{m['eligible']}",
                    "Total inquiries": m["count"],
                })
            st.dataframe(pd.DataFrame(tier_rows), hide_index=True, use_container_width=True)
        else:
            st.info("No tier-classifiable inquiries for 2026 yet")

        # ROW 13: Conversion by Recommended Status
        st.divider()
        st.subheader("🤝 Conversion by Recommended Status (2026)")
        st.caption(
            "How conversion varies based on whether BIG FUN is on the venue's "
            "preferred-vendor list. Hypothesis: 'Yes, with hard evidence' should "
            "outperform 'No, with hard evidence'. Empty rows mean no inquiries "
            "from venues in that status."
        )
        try:
            status_metrics = calculate_metrics_by_recommended_status(inquiry_df, tier_lookup)
        except Exception as e:
            status_metrics = {}
            st.caption(f"Status calc failed: {str(e)[:100]}")
        if status_metrics:
            STATUS_ORDER = [
                "Yes, with hard evidence",
                "Yes/Likely, with no hard evidence",
                "Unknown",
                "Unlikely",
                "No, with hard evidence",
            ]
            status_rows = []
            seen = set()
            for s in STATUS_ORDER:
                if s in status_metrics:
                    m = status_metrics[s]
                    status_rows.append({
                        "Recommended Status": s,
                        "Conversion": f"{m['conversion_rate']:.0f}%",
                        "Booked / Eligible": f"{m['booked']}/{m['eligible']}",
                        "Total inquiries": m["count"],
                    })
                    seen.add(s)
            # Surface anything we didn't expect (defensive)
            for s, m in status_metrics.items():
                if s in seen:
                    continue
                status_rows.append({
                    "Recommended Status": s or "(blank)",
                    "Conversion": f"{m['conversion_rate']:.0f}%",
                    "Booked / Eligible": f"{m['booked']}/{m['eligible']}",
                    "Total inquiries": m["count"],
                })
            st.dataframe(pd.DataFrame(status_rows), hide_index=True, use_container_width=True)
        else:
            st.info("No status-classifiable inquiries for 2026 yet")

        # ROW 14: Action lists — Research / Outreach / Growth Targets
        st.divider()
        st.subheader("🎯 Action lists")
        action_col1, action_col2 = st.columns(2)

        with action_col1:
            st.markdown("**🔍 Research Targets**")
            st.caption(
                "Tier 1/2 venues where we don't know whether we're on their preferred list. "
                "Sorted by 2026 inquiry volume — chase the ones sending us business first."
            )
            try:
                research = find_research_targets(venues, tier_lookup, inquiry_df)
            except Exception as e:
                research = []
                st.caption(f"Research-targets calc failed: {str(e)[:100]}")
            if research:
                research_rows = [
                    {
                        "Venue": t["name"][:40],
                        "Tier": t["tier"],
                        "Inquiries (2026)": t["inquiries_this_year"],
                    }
                    for t in research
                ]
                st.dataframe(pd.DataFrame(research_rows), hide_index=True, use_container_width=True)
            else:
                st.info("All Tier 1/2 venues have a known preferred-list status")

        with action_col2:
            st.markdown("**📤 Outreach Targets**")
            st.caption(
                "Growth-target venues we've confirmed we're not on the preferred list for. "
                "Highest-priority outreach — declared intent + known gap."
            )
            try:
                outreach = find_outreach_targets(venues, tier_lookup, inquiry_df)
            except Exception as e:
                outreach = []
                st.caption(f"Outreach-targets calc failed: {str(e)[:100]}")
            if outreach:
                outreach_rows = [
                    {
                        "Venue": t["name"][:35],
                        "Tier": t["tier"],
                        "Status": t["recommended_status"],
                        "Inquiries (2026)": t["inquiries_this_year"],
                    }
                    for t in outreach
                ]
                st.dataframe(pd.DataFrame(outreach_rows), hide_index=True, use_container_width=True)
            else:
                st.info("No outreach targets — fill in Recommended Status for growth-target venues")

        # Growth Target activity (full width)
        st.markdown("**🌱 Growth Target Activity**")
        st.caption(
            "Recent inquiry and booking counts for every venue you've flagged as a growth target. "
            "If a flagged venue isn't generating inquiries over time, the bet isn't paying off."
        )
        try:
            gt_activity = calculate_growth_target_activity(venues, tier_lookup, inquiry_df)
        except Exception as e:
            gt_activity = []
            st.caption(f"Growth-target calc failed: {str(e)[:100]}")
        if gt_activity:
            gt_rows = [
                {
                    "Venue": v["name"][:45],
                    "Tier": v["tier"],
                    "Status": v["recommended_status"],
                    "2026 Inquiries": v["inquiries"],
                    "2026 Booked": v["booked"],
                }
                for v in gt_activity
            ]
            st.dataframe(pd.DataFrame(gt_rows), hide_index=True, use_container_width=True)
        else:
            st.info("No growth-target venues flagged. Check the Airtable Venues table.")
    elif inquiry_df is not None:
        st.divider()
        st.caption(
            "Venue tier sections require an Airtable Personal Access Token. "
            "Add it to Streamlit secrets as `airtable_pat`, or save to ~/.airtable-pat for local dev."
        )

    # ==========================================================================
    # Footer
    # ==========================================================================

    st.divider()
    st.caption("Big Fun DJ Operations Dashboard • Data refreshes hourly • Click 🔄 to force refresh")


if __name__ == "__main__":
    main()
