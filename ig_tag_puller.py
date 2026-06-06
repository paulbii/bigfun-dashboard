"""
IG Tag Puller

Daily script that pulls Instagram posts where @bigfundj is tagged, upserts them
into the Airtable IG Tags table, and matches each tagging account against the
Vendors table. Flags unmatched handles for review.

Runs from GitHub Actions on a daily cron. Can also be run locally for testing.

Environment variables (GitHub Secrets / local .env):
    META_APP_ID
    META_APP_SECRET
    IG_BUSINESS_ACCOUNT_ID
    IG_PAGE_ACCESS_TOKEN
    AIRTABLE_PAT

Local fallbacks (if env vars are not set):
    ~/.airtable-pat                         (Airtable PAT)
    ~/.meta-tokens.env                      (key=value pairs for the four META_/IG_ vars)

See META_SETUP.md for how to obtain the Meta values.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

# BIG FUN Disc Jockeys base. Field IDs are stable across renames; field names aren't.
AIRTABLE_BASE_ID = "appPMPQxGhQa6pWDz"

# IG Tags table
IG_TAGS_TABLE_ID = "tblKHbLPvYIzoTG5b"
IG_TAGS_FIELD_POST_URL = "fldz1giI46ndDUAyp"        # url, primary
IG_TAGS_FIELD_POSTED = "flddNCuI6GMsntMsx"          # date
IG_TAGS_FIELD_HANDLE = "fldBUkDnylUs8NvsY"          # singleLineText
IG_TAGS_FIELD_TYPE = "fldiJPr0MxtoNHAJG"            # singleSelect
IG_TAGS_FIELD_CAPTION = "fldZwcZ6hfExiAJj8"         # multilineText
IG_TAGS_FIELD_MEDIA = "fldBbRBDYzoXnT2UA"           # url
IG_TAGS_FIELD_LINKED_VENDOR = "fldIy2TEaVUL0FQdT"   # multipleRecordLinks
IG_TAGS_FIELD_MATCH_STATUS = "fldSUbVptXgrdjxkZ"    # singleSelect
IG_TAGS_FIELD_LAST_SEEN = "fldB7HfHpqM468B8C"       # date
IG_TAGS_FIELD_NOTES = "fldJFWr59IHpJfPaq"           # multilineText

# Vendors table (used for handle matching)
VENDORS_TABLE_ID = "tbljCKtHLvoNCHgNM"
VENDORS_FIELD_NAME = "fldVhxlNcz056zxfq"
VENDORS_FIELD_INSTAGRAM = "fldkBc0RHsbcA14TC"

GRAPH_API_VERSION = "v21.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

# Match status option names. Airtable's typecast: True will auto-create these on first run.
STATUS_MATCHED = "matched"
STATUS_UNMATCHED = "unmatched"


# =============================================================================
# SECRET LOADING
# =============================================================================

def _load_meta_tokens_file() -> dict:
    """Parse ~/.meta-tokens.env if present. Format: KEY=value, one per line, # comments."""
    path = Path.home() / ".meta-tokens.env"
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def load_config() -> dict:
    """Load all needed secrets. Env vars first, fall back to local files."""
    meta_file = _load_meta_tokens_file()

    def get(key: str, file_dict: dict = meta_file) -> str:
        return os.environ.get(key) or file_dict.get(key, "")

    pat = os.environ.get("AIRTABLE_PAT", "")
    if not pat:
        pat_file = Path.home() / ".airtable-pat"
        if pat_file.exists():
            pat = pat_file.read_text().strip()

    cfg = {
        "ig_business_account_id": get("IG_BUSINESS_ACCOUNT_ID"),
        "ig_page_access_token": get("IG_PAGE_ACCESS_TOKEN"),
        "meta_app_id": get("META_APP_ID"),
        "meta_app_secret": get("META_APP_SECRET"),
        "airtable_pat": pat,
    }

    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise SystemExit(
            f"Missing required config values: {', '.join(missing)}.\n"
            "Set them as env vars or in ~/.meta-tokens.env (and ~/.airtable-pat). "
            "See META_SETUP.md for how to obtain the Meta values."
        )
    return cfg


# =============================================================================
# INSTAGRAM GRAPH API
# =============================================================================

def fetch_tagged_media(ig_user_id: str, access_token: str) -> list[dict]:
    """Fetch all media where @bigfundj is photo-tagged.

    Returns a list of media dicts with keys: id, caption, media_type, media_url,
    thumbnail_url, permalink, timestamp, username. The IG Graph API caps results
    at recent media (typically last 25-90 items); older history is not retrievable.
    """
    url = f"{GRAPH_API_BASE}/{ig_user_id}/tags"
    params = {
        "fields": "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,username",
        "access_token": access_token,
        "limit": 50,
    }

    media = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            raise SystemExit(f"IG Graph API error ({r.status_code}): {r.text}")
        data = r.json()
        media.extend(data.get("data", []))

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        # Subsequent calls already include all params in next_url; pass empty params.
        url = next_url
        params = {}

    return media


# =============================================================================
# AIRTABLE
# =============================================================================

def airtable_headers(pat: str) -> dict:
    return {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}


def fetch_vendors_by_handle(pat: str) -> dict[str, str]:
    """Return a {normalized_handle: vendor_record_id} dict for handle matching.

    Normalization: strip leading @, lowercase, strip whitespace. So "@Reverie"
    matches "reverie".
    """
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{VENDORS_TABLE_ID}"
    params = {
        "pageSize": 100,
        "returnFieldsByFieldId": "true",
        "fields[]": [VENDORS_FIELD_NAME, VENDORS_FIELD_INSTAGRAM],
    }
    headers = {"Authorization": f"Bearer {pat}"}

    handle_to_id = {}
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            handle = rec.get("fields", {}).get(VENDORS_FIELD_INSTAGRAM, "") or ""
            normalized = handle.lstrip("@").strip().lower()
            if normalized:
                handle_to_id[normalized] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    return handle_to_id


def find_existing_ig_tag(post_url: str, pat: str) -> str | None:
    """Return the Airtable record ID for an existing IG Tags row matching post_url, or None."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{IG_TAGS_TABLE_ID}"
    # Airtable filterByFormula on URL field
    formula = f'{{IG Post URL}} = "{post_url}"'
    params = {
        "pageSize": 1,
        "filterByFormula": formula,
        "returnFieldsByFieldId": "true",
        "fields[]": [IG_TAGS_FIELD_POST_URL],
    }
    headers = {"Authorization": f"Bearer {pat}"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    records = r.json().get("records", [])
    return records[0]["id"] if records else None


def upsert_ig_tag(record_data: dict, pat: str) -> tuple[str, bool]:
    """Upsert a record into IG Tags by IG Post URL. Returns (record_id, created)."""
    post_url = record_data[IG_TAGS_FIELD_POST_URL]
    existing_id = find_existing_ig_tag(post_url, pat)

    if existing_id:
        # PATCH only the fields that should refresh on re-poll (Last Seen, and re-link
        # Linked Vendor / Match Status in case the Vendors table changed).
        refresh_fields = {
            IG_TAGS_FIELD_LAST_SEEN: record_data[IG_TAGS_FIELD_LAST_SEEN],
            IG_TAGS_FIELD_LINKED_VENDOR: record_data.get(IG_TAGS_FIELD_LINKED_VENDOR, []),
            IG_TAGS_FIELD_MATCH_STATUS: record_data[IG_TAGS_FIELD_MATCH_STATUS],
        }
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{IG_TAGS_TABLE_ID}/{existing_id}"
        body = {"fields": refresh_fields, "typecast": True}
        r = requests.patch(url, headers=airtable_headers(pat), json=body, timeout=30)
        r.raise_for_status()
        return existing_id, False

    # Create
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{IG_TAGS_TABLE_ID}"
    body = {"records": [{"fields": record_data}], "typecast": True}
    r = requests.post(url, headers=airtable_headers(pat), json=body, timeout=30)
    r.raise_for_status()
    new_id = r.json()["records"][0]["id"]
    return new_id, True


# =============================================================================
# CORE LOGIC
# =============================================================================

def map_media_type(api_type: str) -> str:
    """IG Graph API returns IMAGE, VIDEO, CAROUSEL_ALBUM. Map to readable strings."""
    return {
        "IMAGE": "Image",
        "VIDEO": "Video",
        "CAROUSEL_ALBUM": "Carousel",
    }.get(api_type, api_type or "Unknown")


def build_record_data(media: dict, vendor_lookup: dict[str, str], today_iso: str) -> dict:
    """Translate an IG media item into an Airtable IG Tags row payload."""
    handle_normalized = (media.get("username") or "").lstrip("@").strip().lower()
    handle_display = f"@{handle_normalized}" if handle_normalized else ""

    vendor_id = vendor_lookup.get(handle_normalized)
    linked_vendor = [vendor_id] if vendor_id else []
    match_status = STATUS_MATCHED if vendor_id else STATUS_UNMATCHED

    # Date-only for Posted (the Airtable field is a date, not datetime).
    timestamp_str = media.get("timestamp", "")
    posted_date = timestamp_str[:10] if timestamp_str else ""

    # Prefer media_url for images; for videos, fall back to thumbnail_url.
    media_url = media.get("media_url") or media.get("thumbnail_url") or ""

    return {
        IG_TAGS_FIELD_POST_URL: media.get("permalink", ""),
        IG_TAGS_FIELD_POSTED: posted_date,
        IG_TAGS_FIELD_HANDLE: handle_display,
        IG_TAGS_FIELD_TYPE: map_media_type(media.get("media_type", "")),
        IG_TAGS_FIELD_CAPTION: media.get("caption", "") or "",
        IG_TAGS_FIELD_MEDIA: media_url,
        IG_TAGS_FIELD_LINKED_VENDOR: linked_vendor,
        IG_TAGS_FIELD_MATCH_STATUS: match_status,
        IG_TAGS_FIELD_LAST_SEEN: today_iso,
    }


def main() -> int:
    cfg = load_config()
    today_iso = datetime.now(timezone.utc).date().isoformat()

    print(f"[{today_iso}] Fetching vendor handle lookup...")
    vendor_lookup = fetch_vendors_by_handle(cfg["airtable_pat"])
    print(f"  Loaded {len(vendor_lookup)} vendor handles for matching.")

    print(f"[{today_iso}] Fetching tagged media from Instagram...")
    media_items = fetch_tagged_media(
        cfg["ig_business_account_id"], cfg["ig_page_access_token"]
    )
    print(f"  Got {len(media_items)} tagged posts from IG.")

    if not media_items:
        print("Nothing to upsert. Done.")
        return 0

    created = 0
    refreshed = 0
    matched = 0
    unmatched_handles: set[str] = set()

    for media in media_items:
        if not media.get("permalink"):
            print(f"  Skipping media with no permalink: {media.get('id')}")
            continue

        record = build_record_data(media, vendor_lookup, today_iso)
        try:
            _, was_created = upsert_ig_tag(record, cfg["airtable_pat"])
        except requests.HTTPError as e:
            print(f"  Airtable error for {record[IG_TAGS_FIELD_POST_URL]}: {e}")
            print(f"    Response body: {e.response.text if e.response else 'no response'}")
            continue

        if was_created:
            created += 1
        else:
            refreshed += 1

        if record[IG_TAGS_FIELD_MATCH_STATUS] == STATUS_MATCHED:
            matched += 1
        else:
            unmatched_handles.add(record[IG_TAGS_FIELD_HANDLE])

    print(f"\nSummary:")
    print(f"  Created: {created}")
    print(f"  Refreshed: {refreshed}")
    print(f"  Matched to vendor: {matched}")
    print(f"  Unmatched handles needing review: {len(unmatched_handles)}")
    if unmatched_handles:
        print(f"    {sorted(unmatched_handles)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
