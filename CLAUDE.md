# bigfun-dashboard

Streamlit operations dashboard for BIG FUN Disc Jockeys. Read-only — aggregates Google Sheets and FileMaker into a single status board for Paul, Henry, and Woody.

## Read first

- `README.md` — what it shows, data sources, deployment.

## Notes

- Live URL: https://bigfun-dashboard.streamlit.app
- Read-only. No write paths — display-only code, so TDD usually skips here. Tests in `tests/` cover data shaping; add to those when changing aggregation logic.
- Data is cached for 5 minutes. If a number looks wrong, check whether it's stale before chasing the data source.
- Sheet IDs live at the top of `dashboard.py`. Credentials live in Streamlit secrets, not the repo.
- Local dev: `streamlit run dashboard.py` then open http://localhost:8501.

## Before merging

- Run locally and click through every section. Verify numbers match a spot-check against the underlying Sheet.
