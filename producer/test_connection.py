"""
Quick sanity check — run this FIRST, before anything else.

Verifies that:
  1. Your three cert files are present in certs/
  2. The SSL handshake with Aiven succeeds
  3. Lists any topics that already exist

Usage:
    python test_connection.py
"""

import sys

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONFIG, validate_certs
from confluent_kafka.admin import AdminClient


def main() -> int:
    print("=" * 64)
    print("  Hi Adnan! Testing your Aiven Kafka connection...")
    print("=" * 64)

    missing = validate_certs()
    if missing:
        print("\n[FAILED] Missing certificate file(s):")
        for path in missing:
            print(f"   - {path}")
        print("\nDownload them from the Aiven Console and drop them in certs/")
        print("(see certs/README.md for the exact filenames).")
        return 1

    print(f"Bootstrap server : {KAFKA_BOOTSTRAP_SERVERS}")
    print("Certs            : found all 3 ✓")
    print("Connecting...")

    admin = AdminClient(KAFKA_CONFIG)
    try:
        metadata = admin.list_topics(timeout=15)
    except Exception as exc:
        print(f"\n[FAILED] Could not connect: {exc}")
        return 1

    print(f"\n[OK] Connected! Cluster has {len(metadata.brokers)} broker(s).")
    print("\nExisting topics:")
    user_topics = {n: t for n, t in metadata.topics.items() if not n.startswith("__")}
    if user_topics:
        for name in sorted(user_topics):
            n_parts = len(user_topics[name].partitions)
            print(f"   - {name}  ({n_parts} partitions)")
    else:
        print("   (none yet — run:  python create_topics.py)")

    print("\nAll good. You're ready to produce events. 🚀")
    return 0


if __name__ == "__main__":
    sys.exit(main())
