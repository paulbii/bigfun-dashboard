# Big Fun DJ Operations Dashboard — Reference Guide

> **Purpose:** Document the data processing rules, deduplication logic, and counting methodology used in the operations dashboard.

---

## Overview

The dashboard displays booking metrics, lead tracking, and operational data for Big Fun DJ. It pulls from multiple data sources and applies deduplication logic to handle the reality that inquiry tracker entries can change over time.

---

## Data Sources

| Source | Sheet ID | What It Provides |
|--------|----------|------------------|
| Booking Snapshots | `1JV5S1hbtYcXhVoeqsYVw_nhUvRoOlSBt5BYZ0ffxFkU` | YoY booking pace comparison |
| Inquiry Tracker | `1ng-OytB9LJ8Fmfazju4cfFJRRa6bqfRIZA8GYEWhJRs` | Lead metrics, conversions, lead time |
| Availability Matrix | `1lXwHECkQJy7h87L5oKbo0hDTpalDgKFTbBQJ4pIerFo` | DJ booking counts |
| FileMaker Gig DB | (via API) | Upcoming events |

The Inquiry Tracker uses the **Master View** tab.

---

## Deduplication Logic

### Why Deduplication?

The inquiry tracker can have multiple rows for the same venue + event date because:

1. **Status changes:** An inquiry marked "Cold" later becomes "Booked"
2. **Multiple bookings:** Same venue, same date, different clients (e.g., morning and evening events)
3. **Cancellations:** A "Booked" event later becomes "Canceled"

Without deduplication, the dashboard would double-count.

### Deduplication Rules

For each unique **(Event Date, Venue)** combination:

| Scenario | Rule | Result |
|----------|------|--------|
| Single row | Keep it | 1 entry |
| Cold → Booked | Keep newest (Booked) | 1 booked |
| Multiple Booked entries | Keep all | N booked (separate clients) |
| Booked → Canceled | If cancel timestamp is after any booking, reduce count by 1 | N-1 booked |
| 2 Booked + 1 Canceled | Keep 1 Booked (newest) | 1 booked |
| 2 Booked + 2 Canceled | All canceled | 0 booked |
| Canceled before Booked | Cancel doesn't count (different inquiry) | 1 booked |
| No Booked entries | Keep newest row | 1 entry (non-booked) |

### Cancellation Logic

A cancellation is **valid** (reduces booking count) only if:
- Resolution = "Canceled"
- Timestamp is **after** the earliest Booked timestamp for that venue+date

This prevents a canceled inquiry from affecting a later, separate booking at the same venue.

### Example Scenarios

**Scenario 1: Reopened Lead**
```
Row 1: Nestldown 5/15/26 - Cold (Jan 5)
Row 2: Nestldown 5/15/26 - Booked (Jan 20)
→ Result: 1 Booked (Cold row dropped)
```

**Scenario 2: Two Separate Bookings (AM/PM)**
```
Row 1: Nestldown 4/26/26 - Booked (Jan 6)
Row 2: Nestldown 4/26/26 - Booked (Feb 4)
→ Result: 2 Booked (different clients)
```

**Scenario 3: One Booking Cancels**
```
Row 1: Nestldown 4/26/26 - Booked (Jan 6)
Row 2: Nestldown 4/26/26 - Booked (Feb 4)
Row 3: Nestldown 4/26/26 - Canceled (Feb 10)
→ Result: 1 Booked (cancel after bookings, reduces by 1)
```

**Scenario 4: Cancel Before Booking (Separate Inquiry)**
```
Row 1: Venue X 6/1/26 - Canceled (Jan 3)
Row 2: Venue X 6/1/26 - Booked (Jan 10)
→ Result: 1 Booked (cancel was before, doesn't count)
```

---

## Metrics Calculated

### 2026 Inquiries Section

| Metric | Definition |
|--------|------------|
| Total Inquiries | Unique venue+date combinations after dedup |
| Booked | Resolution = "Booked" |
| Full/Turn-away | Resolution = "We turn down" or Capacity Status = "Full" |
| Didn't Book | Resolution = "Didn't Book" |
| Cold/Ghosted | Resolution = "Cold" or "Ghosted" |

### Conversion Rates

- **By Lead Source:** Groups by "Initial Contact" field
- **By Interaction Level:** Groups by "Level of interaction" field
- **AAG House Bookings:** Counted separately (venue handoff, not sales funnel)

### Lead Time Analysis

- **Lead Time:** Days between Inquiry Date and Event Date
- **Days to Decision:** Days between Inquiry Date and Decision Date

---

## DJ Booking Counts

Pulled directly from the Availability Matrix (not the Inquiry Tracker).

Counts cells with value "BOOKED" in each DJ's column. TBA column handles:
- `BOOKED` → 1
- `BOOKED x 2` → 2
- `AAG` → 1
- `BOOKED, AAG` → 2

---

## Caching

- Google Sheets data: Cached for 1 hour (`ttl=3600`)
- FileMaker upcoming events: Cached for 5 minutes
- Click 🔄 to force refresh

---

## Edge Cases

### Empty Venue Field

If "Venue (if known)" is blank, deduplication uses Event Date + blank as the key. Multiple blank-venue inquiries on the same date will be deduped together.

### Date Format Variations

The Inquiry Tracker accepts various date formats:
- `m/d/yy` (e.g., `4/26/26`)
- `m/d/yyyy` (e.g., `4/26/2026`)
- `mm/dd/yyyy` (e.g., `04/26/2026`)

All are normalized during processing.

### Timestamp Parsing

Timestamps from Google Sheets may come in various formats. Unparseable timestamps are treated as `NaT` (not a time) and excluded from time-based comparisons.

---

*Last updated: February 2026*
