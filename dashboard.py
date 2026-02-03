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

@st.cache_data(ttl=300)  # Cache for 5 minutes
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


@st.cache_data(ttl=300)
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
    return df


@st.cache_data(ttl=300)
def get_upcoming_events(days_ahead=14):
    """Fetch upcoming events from FileMaker gig database."""
    filemaker_url = get_filemaker_url()
    if not filemaker_url:
        return []  # Skip if FileMaker URL not configured
    
    today = datetime.now()
    events = []
    
    # Query FileMaker for multiple days using the multi-day endpoint
    for offset in range(0, days_ahead, 7):  # Query in weekly chunks
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
    
    today_row = None
    for idx, row in df.iterrows():
        day_str = str(row.get("Day", "")).strip()
        if day_str == today_month_day:
            today_row = row
            break
    
    # If exact match not found, find the most recent day before today
    if today_row is None:
        for idx, row in df.iterrows():
            day_str = str(row.get("Day", "")).strip()
            try:
                # Parse the day string (e.g., "Feb 3" or "Feb  3")
                # Normalize spaces
                normalized = " ".join(day_str.split())
                parsed = datetime.strptime(f"{normalized} {today.year}", "%b %d %Y")
                if parsed.date() <= today.date():
                    today_row = row
            except ValueError:
                continue
    
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


def calculate_lead_metrics(df):
    """Calculate lead time and conversion metrics for 2026 entries."""
    # Filter for 2026 entries with Inquiry Date populated
    df_2026 = df[df["Timestamp"].astype(str).str.contains("2026", na=False)].copy()
    
    # Filter for rows with Inquiry Date
    df_with_dates = df_2026[df_2026["Inquiry Date"].astype(str).str.strip() != ""].copy()
    
    if df_with_dates.empty:
        return {}
    
    metrics = {}
    
    # Total counts by resolution
    resolution_counts = df_2026["Resolution"].value_counts().to_dict()
    metrics["total_inquiries"] = len(df_2026)
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
    cold_never_acknowledged = len(df_2026[
        (df_2026["Resolution"] == "Cold") & 
        (df_2026["Level of interaction"] == "Never acknowledged")
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
    source_counts = df_2026.groupby("Initial Contact")["Resolution"].value_counts().unstack(fill_value=0)
    metrics["by_source"] = {}
    for source in source_counts.index:
        total = source_counts.loc[source].sum()
        booked = source_counts.loc[source].get("Booked", 0)
        if total > 0:
            metrics["by_source"][source] = {
                "total": total,
                "booked": booked,
                "conversion_rate": booked / total * 100
            }
    
    # Level of interaction analysis
    interaction_counts = df_2026.groupby("Level of interaction")["Resolution"].value_counts().unstack(fill_value=0)
    metrics["by_interaction"] = {}
    for interaction in interaction_counts.index:
        total = interaction_counts.loc[interaction].sum()
        booked = interaction_counts.loc[interaction].get("Booked", 0)
        if total > 0:
            metrics["by_interaction"][interaction] = {
                "total": total,
                "booked": booked,
                "conversion_rate": booked / total * 100
            }
    
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
    # ROW 1: Booking Pace + Lead Metrics Summary
    # ==========================================================================
    
    # Pre-calculate metrics for use across columns
    inquiry_df = None
    metrics = {}
    try:
        inquiry_df = get_inquiry_tracker_data()
        metrics = calculate_lead_metrics(inquiry_df)
    except Exception as e:
        st.warning(f"Could not load inquiry data: {str(e)[:100]}")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    # Booking Pace
    with col1:
        st.subheader("📈 Booking Pace")
        try:
            yoy_df = get_year_comparison_data()
            
            # Debug: check what columns we have
            if yoy_df.empty:
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
    
    # Lead Metrics Summary
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
        else:
            st.info("No inquiry data available")
    
    # Conversion Rate
    with col3:
        st.subheader("🎯 Conversion")
        if metrics:
            conversion = metrics.get("conversion_rate", 0)
            conversion_simple = metrics.get("conversion_rate_simple", 0)
            
            st.metric("Conversion Rate", f"{conversion:.0f}%")
            st.caption(f"Excludes: Full, Turn-away, Cold (no response)")
            st.caption(f"Simple (all inquiries): {conversion_simple:.0f}%")
            
            # Show by source
            st.markdown("**By Lead Source:**")
            by_source = metrics.get("by_source", {})
            for source, data in sorted(by_source.items(), key=lambda x: -x[1]["conversion_rate"]):
                if data["total"] >= 3:  # Only show sources with meaningful volume
                    st.text(f"{source[:20]}: {data['conversion_rate']:.0f}% ({data['booked']}/{data['total']})")
        else:
            st.info("No conversion data available")
    
    st.divider()
    
    # ==========================================================================
    # ROW 2: Upcoming Events
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
    # ROW 3: Lead Time Analysis
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
        st.info("Lead time data requires Inquiry Date field (2026 entries)")
    
    st.divider()
    
    # ==========================================================================
    # ROW 4: Conversion by Interaction Level
    # ==========================================================================
    
    st.subheader("📞 Conversion by Interaction Level")
    
    if metrics and metrics.get("by_interaction"):
        by_interaction = metrics.get("by_interaction", {})
        
        # Order by typical sales funnel
        interaction_order = [
            "Never acknowledged",
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
            cols = st.columns(len(matched_interactions))
            
            for idx, (label, actual_key) in enumerate(matched_interactions):
                data = by_interaction[actual_key]
                with cols[idx]:
                    short_label = label.replace("Meaningful email interaction", "Email exchange").replace("Had phone call/video chat", "Phone/video call")
                    st.metric(
                        label=short_label,
                        value=f"{data['conversion_rate']:.0f}%",
                        help=f"{data['booked']} booked / {data['total']} total"
                    )
        else:
            # Show what we actually have
            st.caption(f"Available: {list(by_interaction.keys())}")
    else:
        st.info("No interaction data available")
    
    # ==========================================================================
    # Footer
    # ==========================================================================
    
    st.divider()
    st.caption("Big Fun DJ Operations Dashboard • Data refreshes every 5 minutes • Click 🔄 to force refresh")


if __name__ == "__main__":
    main()
