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

*Part of the Big Fun DJ automation suite*
