# Big Fun DJ Operations Dashboard

A read-only status board showing booking pace, lead metrics, and upcoming events for Big Fun DJ.

## What It Shows

- **Booking Pace**: 2026 vs 2025 year-over-year comparison
- **Lead Metrics**: Total inquiries, booked, didn't book, turn-aways
- **Conversion Rates**: Overall and by lead source
- **Upcoming Events**: Next 14 days from FileMaker gig database
- **Lead Time Analysis**: How far ahead people inquire, by outcome
- **Interaction Analysis**: Conversion rates by engagement level (email vs phone)

## Data Sources

| Source | What It Provides |
|--------|------------------|
| Booking Snapshots Sheet | YoY booking pace comparison |
| Inquiry Tracker Sheet | Lead metrics, conversion, lead time |
| FileMaker Gig Database | Upcoming events with venue/DJ details |

## Local Development

### Prerequisites
- Python 3.9+
- Google Cloud service account with Sheets API access
- `your-credentials.json` in project root

### Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/bigfun-dashboard.git
cd bigfun-dashboard

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Add your credentials file
cp /path/to/your-credentials.json ./your-credentials.json

# Run locally
streamlit run dashboard.py
```

Open http://localhost:8501 in your browser.

## Streamlit Cloud Deployment

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial dashboard"
   git remote add origin https://github.com/YOUR_USERNAME/bigfun-dashboard.git
   git push -u origin main
   ```

2. **Deploy on Streamlit Cloud**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Click "New app"
   - Select your repo → `dashboard.py`
   - Click "Deploy"

3. **Add Secrets**
   - In Streamlit Cloud, go to your app → Settings → Secrets
   - Paste the contents of your service account JSON in TOML format:
   
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "your-project-id"
   private_key_id = "abc123..."
   private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   client_email = "your-service-account@your-project.iam.gserviceaccount.com"
   client_id = "123456789"
   auth_uri = "https://accounts.google.com/o/oauth2/auth"
   token_uri = "https://oauth2.googleapis.com/token"
   auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
   client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
   ```

## Configuration

Sheet IDs are configured at the top of `dashboard.py`:

```python
BOOKING_SNAPSHOTS_SHEET_ID = "1JV5S1hbtYcXhVoeqsYVw_nhUvRoOlSBt5BYZ0ffxFkU"
INQUIRY_TRACKER_SHEET_ID = "1ng-OytB9LJ8Fmfazju4cfFJRRa6bqfRIZA8GYEWhJRs"
```

## Caching

Data is cached for 5 minutes to reduce API calls. Click the 🔄 button to force a refresh.

## Future Enhancements

- [ ] Capacity snapshot from Availability Matrix
- [ ] Fully booked dates list
- [ ] DJ workload distribution
- [ ] Auto-refresh on timer
- [ ] Mobile-optimized layout

---

## IG Tag Puller (sibling tool)

Standalone script that pulls Instagram posts where @bigfundj is tagged and upserts them into the Airtable `IG Tags` table, matching each tagging account against the Vendors table. Lives in `ig_tag_puller.py`. Runs daily via GitHub Actions (`.github/workflows/ig-tags-daily.yml`).

**Why it lives here:** reuses the Airtable patterns already in `dashboard.py`. Not part of the Streamlit app, runs as a scheduled job.

### One-time Meta setup

Follow [META_SETUP.md](META_SETUP.md). Produces four credentials:
- `META_APP_ID`
- `META_APP_SECRET`
- `IG_BUSINESS_ACCOUNT_ID`
- `IG_PAGE_ACCESS_TOKEN`

### Local testing

Save the four Meta credentials to `~/.meta-tokens.env` (one `KEY=value` per line), and your Airtable PAT to `~/.airtable-pat`. Then:

```bash
python ig_tag_puller.py
```

The script prints how many tags it pulled, how many were created vs refreshed, and lists handles that didn't match a vendor (your queue to review).

### GitHub Actions setup

After local testing works, wire up GitHub Secrets:

1. Repo on GitHub → Settings → Secrets and variables → Actions
2. Add five repository secrets, names matching the env vars above plus `AIRTABLE_PAT`
3. The workflow at `.github/workflows/ig-tags-daily.yml` runs daily at 14:00 UTC (~7am PT)
4. Manually trigger from the Actions tab the first time to verify it works end-to-end

### Token expiration

Long-lived Page Access Tokens last ~60 days. When the script starts failing with auth errors, repeat steps 4-5 of `META_SETUP.md` to get a fresh token, then update the `IG_PAGE_ACCESS_TOKEN` GitHub Secret. No code changes needed.

---

*Part of the Big Fun DJ automation suite*
