# Market Data Worker v0.3.0 add-on

Adds:
- Sina per-sector full-day minute backfill
- Zstandard Parquet archive
- CrateDB 1m upload
- CrateDB-generated 5m and daily tables
- Idempotent 1m upserts
- Row-count validation and status JSON

Copy files into the existing market-data-worker repository, apply main_patch.py.txt, rebuild the image, then run fundflow-backfill --date 2026-11-20.
