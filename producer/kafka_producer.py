"""
GitHub Events → Aiven Kafka producer.

Pulls one hour of public events from GH Archive (https://www.gharchive.org/),
and produces every event to `github.events.raw`. Push / PR / Watch events are
*also* routed to their dedicated topics so downstream consumers can subscribe
to just what they care about.

Examples
--------
# Produce the most recently completed hour (good for a cron / Airflow run):
python kafka_producer.py

# Backfill a specific known-good hour (great for first-time testing):
python kafka_producer.py --date 2024-01-15 --hour 12

# Only send the first 500 events (fast smoke test):
python kafka_producer.py --date 2024-01-15 --hour 12 --limit 500
"""
import argparse
import gzip
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import requests
from confluent_kafka import Producer

from config import GHARCHIVE_BASE_URL, KAFKA_CONFIG, TOPICS, validate_certs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("producer")

# event.type -> dedicated topic (everything also goes to the raw topic)
EVENT_ROUTING = {
    "PushEvent": TOPICS["push"],
    "PullRequestEvent": TOPICS["pr"],
    "WatchEvent": TOPICS["stars"],
}


class Stats:
    """Lightweight delivery counters."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def callback(self, err, msg):
        if err is not None:
            self.failed += 1
            if self.failed <= 5:  # avoid log spam
                logger.error("Delivery failed: %s", err)
        else:
            self.delivered += 1


def fetch_gharchive_hour(date_str: str, hour: int):
    """Stream + decompress one hour of GH Archive data. Yields event dicts."""
    url = f"{GHARCHIVE_BASE_URL}/{date_str}-{hour}.json.gz"
    logger.info("Fetching %s", url)

    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()

    count = 0
    with gzip.GzipFile(fileobj=resp.raw) as gz:
        for line in gz:
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                continue
    logger.info("Decompressed %d events", count)


def _produce_one(producer: Producer, topic: str, key: bytes, payload: bytes, stats: Stats) -> None:
    """Produce a single message, blocking briefly on BufferError to drain the queue."""
    while True:
        try:
            producer.produce(topic, key=key, value=payload, callback=stats.callback)
            return
        except BufferError:
            logger.debug("Producer queue full; draining...")
            producer.poll(1)


def produce_events(events, producer: Producer, stats: Stats, limit: int | None) -> int:
    """Produce events to Kafka. Returns the number of source events processed."""
    processed = 0
    for event in events:
        if limit is not None and processed >= limit:
            break

        event_id = str(event.get("id", "unknown"))
        key = event_id.encode("utf-8")
        payload = json.dumps(event).encode("utf-8")

        _produce_one(producer, TOPICS["raw"], key, payload, stats)

        topic = EVENT_ROUTING.get(event.get("type"))
        if topic:
            _produce_one(producer, topic, key, payload, stats)

        if processed % 100 == 0:
            producer.poll(0)

        processed += 1

        if processed % 5000 == 0:
            logger.info("...queued %d events", processed)

    logger.info("Flushing remaining messages (this may take a few minutes for large batches)...")
    remaining = producer.flush(timeout=300)  
    if remaining > 0:
        raise RuntimeError(
            f"flush() timed out with {remaining} messages still in queue or transit. "
            "Increase flush timeout or check broker connectivity."
        )
    return processed


def previous_hour() -> tuple[str, int]:
    """(date_str, hour) for the most recently completed UTC hour."""
    t = datetime.now(timezone.utc) - timedelta(hours=1)
    return t.strftime("%Y-%m-%d"), t.hour


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GitHub Events → Aiven Kafka producer")
    p.add_argument("--date", help="UTC date, YYYY-MM-DD (default: previous hour's date)")
    p.add_argument("--hour", type=int, help="UTC hour 0-23 (default: previous hour)")
    p.add_argument("--limit", type=int, help="Only produce the first N events (smoke test)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    missing = validate_certs()
    if missing:
        logger.error("Missing cert file(s): %s — see certs/README.md", ", ".join(missing))
        return 1

    if args.date and args.hour is not None:
        date_str, hour = args.date, args.hour
    else:
        date_str, hour = previous_hour()
        logger.info("No --date/--hour given; using previous hour %s-%02d", date_str, hour)

    producer = Producer(KAFKA_CONFIG)
    stats = Stats()

    try:
        events = fetch_gharchive_hour(date_str, hour)
        processed = produce_events(events, producer, stats, args.limit)
    except requests.HTTPError as exc:
        logger.error("GH Archive fetch failed (%s). That hour may not exist yet.", exc)
        return 1
    except RuntimeError as exc:
        logger.error("Producer flush failed: %s", exc)
        return 1

    logger.info(
        "Done. source events=%d | delivered=%d | failed=%d",
        processed, stats.delivered, stats.failed,
    )
    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
