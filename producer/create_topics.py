"""
Create the four GitHub-events topics on Aiven Kafka (idempotent).

You can also create them by hand in the Aiven Console — this script just
automates it. Partitions/replication are capped at 2 to match the free tier.

Usage:
    python create_topics.py
"""
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from config import KAFKA_CONFIG, NUM_PARTITIONS, REPLICATION_FACTOR, TOPICS


def main() -> int:
    admin = AdminClient(KAFKA_CONFIG)

    try:
        existing = set(admin.list_topics(timeout=15).topics)
    except Exception as exc:  # noqa: BLE001
        print(f"[FAILED] Could not reach the cluster: {exc}")
        return 1

    to_create = []
    for topic in TOPICS.values():
        if topic in existing:
            print(f"[skip]    {topic} already exists")
        else:
            to_create.append(
                NewTopic(
                    topic,
                    num_partitions=NUM_PARTITIONS,
                    replication_factor=REPLICATION_FACTOR,
                    config={"retention.ms": str(24 * 60 * 60 * 1000)},  # 24h retention
                )
            )

    if not to_create:
        print("\nAll topics already present. Nothing to do.")
        return 0

    futures = admin.create_topics(to_create)
    exit_code = 0
    for topic, future in futures.items():
        try:
            future.result() 
            print(f"[created] {topic}  ({NUM_PARTITIONS} partitions, RF={REPLICATION_FACTOR})")
        except Exception as exc: 
            print(f"[error]   {topic}: {exc}")
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
