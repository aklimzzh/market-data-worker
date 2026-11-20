import os
from pathlib import Path
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/config"))
SERV00_INGEST_URL = os.getenv("SERV00_INGEST_URL", "")
SERV00_API_TOKEN = os.getenv("SERV00_API_TOKEN", "")
SINA_INTERVAL = int(os.getenv("SINA_INTERVAL", "60"))
THS_INTERVAL = int(os.getenv("THS_INTERVAL", "600"))
TZ = os.getenv("TZ", "Asia/Shanghai")
