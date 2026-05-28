from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
ANALYTICS_DIR = RAW_DIR / "analytics"
MIGRATIONS_DIR = RAW_DIR / "migrations"
ONCHAIN_DIR = RAW_DIR / "onchain"
ORCHESTRATOR_DIR = RAW_DIR / "orchestrator"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
FEATURES_DIR = DATA_DIR / "features"
LABELS_DIR = DATA_DIR / "labels"

CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"
ENV_FILE = PROJECT_ROOT / ".env"
TMP_DIR = PROJECT_ROOT / "tmp"


PUMPPORTAL_WS_CONFIG = CONFIG_DIR / "pumpportal_ws.yaml"
TELEGRAM_CONFIG = CONFIG_DIR / "telegram.yaml"