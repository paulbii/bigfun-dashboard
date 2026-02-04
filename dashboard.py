"""
Big Fun DJ Operations Dashboard
A read-only status board showing booking pace, lead metrics, and capacity.
"""

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import requests
from functools import lru_cache
import time
import plotly.graph_objects as go

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
    worksheet = sheet.worksheet("Master View")
    
    # Use get_all_values() to handle duplicate/empty headers
    all_values = worksheet.get_all_values()
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
        
        df = df.groupby(["Event Date", "Venue (if known)"], group_keys=False).apply(smart_dedup)
        
        # Clean up temp column
        df = df.drop(columns=["_parsed_timestamp"])
    
    # Store dedup stats in a special row (will be filtered out later)
    # Actually, let's add columns instead
    df["_dedup_pre"] = pre_dedup_count
    df["_dedup_post"] = len(df)
    
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
    
    # Count BOOKED for each DJ
    counts = {}
    for dj, col_idx in dj_columns.items():
        count = 0
        for row in all_values[1:]:  # Skip header
            if col_idx < len(row):
                cell_value = str(row[col_idx]).strip().upper()
                if cell_value == "BOOKED":
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


def calculate_lead_metrics(df):
    """Calculate lead time and conversion metrics for 2026 events."""
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
        (df_with_dates["Level of interaction"] == "Never acknowledged")
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
    except Exception as e:
        st.warning(f"Could not load inquiry data: {str(e)[:100]}")
    
    # Load year comparison data for pace metrics and chart
    yoy_df = None
    try:
        yoy_df = get_year_comparison_data()
    except Exception as e:
        pass  # Will show error in the booking pace section
    
    col1, col2 = st.columns(2)
    
    # Booking Pace
    with col1:
        st.subheader("📈 Booking Pace")
        try:
            if yoy_df is None:
                st.error("Could not load booking data")
            elif yoy_df.empty:
                st.warning("Year comparison data is empty")
            else:
                current, last_year, diff, error = calculate_booking_pace(yoy_df)
                
                if error:
                    st.warning(error)
                elif current is not None:
                    st.metric(
                        label=f"2026 Booked (as of today)",
                        value=current,
                        delta=f"{diff:+d} vs 2025" if diff else None,
                        delta_color="normal"
                    )
                    st.caption(f"Same time 2025: {last_year}")
                else:
                    st.info("No pace data for today yet")
        except Exception as e:
            st.error(f"Could not load booking pace: {type(e).__name__}: {str(e)[:100]}")
    
    # Inquiries Summary
    with col2:
        st.subheader("📊 2026 Inquiries")
        if metrics:
            st.metric("Total Inquiries", metrics.get("total_inquiries", 0))
            
            sub_col1, sub_col2 = st.columns(2)
            with sub_col1:
                st.metric("Booked", metrics.get("booked", 0))
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
    # Footer
    # ==========================================================================
    
    st.divider()
    st.caption("Big Fun DJ Operations Dashboard • Data refreshes hourly • Click 🔄 to force refresh")


if __name__ == "__main__":
    main()
