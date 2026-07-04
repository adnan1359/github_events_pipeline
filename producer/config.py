"""
Central configuration for the GitHub Events producer.

Reads from a .env file at the project root if present, otherwise falls back to
sensible defaults that point at the Aiven service and the local certs/ folder.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CERTS_DIR = PROJECT_ROOT / "certs"

load_dotenv(PROJECT_ROOT / ".env")


def _cert_path(env_var: str, default_name: str) -> str:
    """Resolve a cert path from env, or default to certs/<default_name>."""
    value = os.getenv(env_var)
    if value:
        p = Path(value)
        return str(p if p.is_absolute() else PROJECT_ROOT / p)
    return str(CERTS_DIR / default_name)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-3ca0846-adnananam1359-fbe9.f.aivencloud.com:12815",
)

SSL_CA_LOCATION = _cert_path("KAFKA_SSL_CA", "ca.pem")
SSL_CERT_LOCATION = _cert_path("KAFKA_SSL_CERT", "service.cert")
SSL_KEY_LOCATION = _cert_path("KAFKA_SSL_KEY", "service.key")

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SSL",
    "ssl.ca.location": SSL_CA_LOCATION,
    "ssl.certificate.location": SSL_CERT_LOCATION,
    "ssl.key.location": SSL_KEY_LOCATION,

    "acks": "all",                 
    "enable.idempotence": True,    
    "retries": 5,
    "linger.ms": 50,              
    "compression.type": "snappy",
    "queue.buffering.max.messages": 200000, 
    "queue.buffering.max.kbytes": 524288,
}

# ── Topics ───────────────────────────────────────────────────────────────────
TOPICS = {
    "raw": "github.events.raw",
    "push": "github.events.push",
    "pr": "github.events.pr",
    "stars": "github.events.stars",
}

# Aiven free-tier limits
NUM_PARTITIONS = 2
REPLICATION_FACTOR = 2

# ── Data source 
GHARCHIVE_BASE_URL = "https://data.gharchive.org"


def validate_certs() -> list[str]:
    """Return a list of missing cert files (empty if all present)."""
    missing = []
    for path in (SSL_CA_LOCATION, SSL_CERT_LOCATION, SSL_KEY_LOCATION):
        if not Path(path).exists():
            missing.append(path)
    return missing
