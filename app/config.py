import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/config"))
SERV00_INGEST_URL = os.getenv("SERV00_INGEST_URL", "").strip()
SERV00_API_TOKEN = os.getenv("SERV00_API_TOKEN", "").strip()
SINA_INTERVAL = int(os.getenv("SINA_INTERVAL", "60"))
COLLECT_AUCTION = os.getenv("COLLECT_AUCTION", "true").lower() in {"1", "true", "yes"}
