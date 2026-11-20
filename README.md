# Market Data Worker v0.1.0

Initial deployable skeleton for OpenWrt Docker + GitHub Actions/GHCR.

Implemented: AKShare calendar update, Sina 229-sector collector, local CSV/pending/status, Serv00 batch POST hook, Parquet maintenance skeleton.

Not yet wired: existing ETF/index repair logic, Serv00 ingest endpoint implementation, CrateDB upload/aggregation. These require the current ETF API files/schema and Serv00 endpoint details.
