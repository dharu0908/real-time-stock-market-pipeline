
import random
import json
import time
import uuid
import logging
import argparse
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC,
    TICKERS,
    PRICES
)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("tick_producer")


# ─────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────
_current_prices = PRICES.copy()


# ─────────────────────────────────────────────
# TOPIC CHECK / CREATE
# ─────────────────────────────────────────────
def ensure_topic():
    admin = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    topics = admin.list_topics(timeout=5).topics

    if TOPIC in topics:
        log.info(f"Topic '{TOPIC}' is available.")
        return

    log.warning(f"Topic '{TOPIC}' not found. Creating...")

    futures = admin.create_topics([
        NewTopic(
            TOPIC,
            num_partitions=3,
            replication_factor=1
        )
    ])

    for topic, future in futures.items():
        try:
            future.result()
            log.info(f"Created topic '{topic}'")
        except Exception as e:
            log.error(f"Failed creating topic '{topic}': {e}")


# ─────────────────────────────────────────────
# PRODUCER
# ─────────────────────────────────────────────
def build_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
        "retries": 3
    })


# ─────────────────────────────────────────────
# PRICE SIMULATION
# ─────────────────────────────────────────────
def next_tick(ticker: str):
    old_price = _current_prices[ticker]

    pct_change = random.gauss(0, 0.001)
    new_price = round(old_price * (1 + pct_change), 4)

    _current_prices[ticker] = new_price

    spread = round(new_price * 0.0002, 4)

    return {
        "event_id": str(uuid.uuid4()),
        "ticker": ticker,
        "price": new_price,
        "bid": round(new_price - spread, 4),
        "ask": round(new_price + spread, 4),
        "volume": random.randint(100, 10000),
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }


# ─────────────────────────────────────────────
# DELIVERY CALLBACK
# ─────────────────────────────────────────────
def on_delivery(err, msg):
    if err:
        log.error(f"Delivery failed: {err}")


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main(rps=10, duration=None):
    log.info("Starting Kafka tick producer")
    log.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"Topic: {TOPIC}")
    log.info(f"Tickers: {TICKERS}")
    log.info(f"RPS: {rps}")

    ensure_topic()

    producer = build_producer()

    sleep_time = 1 / rps
    produced = 0

    start_time = time.time()
    last_log = start_time

    try:
        while True:
            now = time.time()

            if duration and (now - start_time) >= duration:
                break

            ticker = random.choice(TICKERS)

            message = next_tick(ticker)

            producer.produce(
                topic=TOPIC,
                key=ticker,
                value=json.dumps(message),
                on_delivery=on_delivery
            )

            producer.poll(0)

            produced += 1

            if now - last_log >= 10:
                elapsed = now - start_time

                log.info(
                    f"Sent {produced} messages | "
                    f"Rate: {produced / elapsed:.1f} msg/sec"
                )

                last_log = now

            time.sleep(
                max(
                    0,
                    sleep_time + random.uniform(-0.01, 0.01)
                )
            )

    except KeyboardInterrupt:
        log.warning("Stopping producer (KeyboardInterrupt)")

    finally:
        producer.flush()
        log.info(
            f"Producer stopped. Total messages sent: {produced}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rps",
        type=int,
        default=10
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=None
    )

    args = parser.parse_args()

    main(
        rps=args.rps,
        duration=args.duration
    )

