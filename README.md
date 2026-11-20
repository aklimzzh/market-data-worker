# Market Data Worker v0.2.0

Implemented:
- Sina 229-sector stable pagination, retries and per-request metrics
- Minute-bucket timestamps and market phases
- Local CSV, atomic status JSON and pending queue
- Pending retry
- Serv00 PostgreSQL schema
- Flask ingest/latest Blueprint
- AKShare calendar-update API rename

All OpenWrt data is mounted from /mnt/ssd/moneyflow/data to /data and configuration from /mnt/ssd/moneyflow/config to /config.
