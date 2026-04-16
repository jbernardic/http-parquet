#!/usr/bin/env python3
"""
Simulates a real-time application streaming events to POST /ingest.

Sends a mix of single-object and batched payloads at a configurable rate.
Prints a live stats line every second.

Usage:
    python ingest_load.py                          # defaults
    python ingest_load.py --url http://host:9090   # custom host
    python ingest_load.py --rate 200               # 200 events/sec
    python ingest_load.py --batch-size 50          # batch up to 50 events per request
    python ingest_load.py --single-ratio 0.2       # 20% single-object, 80% batch requests
"""

import argparse
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "search", "logout", "signup"]
PAGES = ["/home", "/product/42", "/cart", "/checkout", "/search", "/profile"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
COUNTRIES = ["US", "DE", "GB", "FR", "JP", "BR", "AU", "CA"]


def random_event() -> dict:
    event_type = random.choice(EVENT_TYPES)
    base = {
        "event": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100_000),
        "page": random.choice(PAGES),
        "country": random.choice(COUNTRIES),
        "browser": random.choice(BROWSERS),
        "latency_ms": random.randint(10, 3000),
    }
    if event_type == "purchase":
        base["amount_usd"] = round(random.uniform(5.0, 500.0), 2)
        base["item_count"] = random.randint(1, 10)
    if event_type == "search":
        base["query"] = random.choice(["shoes", "laptop", "gift", "sale", "new arrivals"])
        base["results"] = random.randint(0, 200)
    return base


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.requests_ok = 0
        self.requests_err = 0
        self.events_sent = 0
        self.start = time.monotonic()
        self._last_print = self.start
        self._last_events = 0
        self._last_requests = 0

    def record_ok(self, event_count: int):
        self.requests_ok += 1
        self.events_sent += event_count

    def record_err(self):
        self.requests_err += 1

    def maybe_print(self):
        now = time.monotonic()
        elapsed = now - self._last_print
        if elapsed < 1.0:
            return
        interval_events = self.events_sent - self._last_events
        interval_requests = (self.requests_ok + self.requests_err) - self._last_requests
        eps = interval_events / elapsed
        rps = interval_requests / elapsed
        total_elapsed = now - self.start
        print(
            f"\r[{total_elapsed:7.1f}s] "
            f"events: {self.events_sent:>8,}  "
            f"eps: {eps:>7.1f}  "
            f"req/s: {rps:>6.1f}  "
            f"ok: {self.requests_ok:>6,}  "
            f"err: {self.requests_err:>4,}",
            end="",
            flush=True,
        )
        self._last_print = now
        self._last_events = self.events_sent
        self._last_requests = self.requests_ok + self.requests_err


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(url: str, rate: float, batch_size: int, single_ratio: float):
    session = requests.Session()
    stats = Stats()
    interval = 1.0 / rate  # seconds between individual events
    next_send = time.monotonic()

    print(f"Sending to {url}  rate={rate} events/s  batch_size={batch_size}  single_ratio={single_ratio:.0%}")
    print("Press Ctrl+C to stop.\n")

    while True:
        # Decide how many events to bundle into this request
        if random.random() < single_ratio:
            events = [random_event()]
        else:
            count = random.randint(max(1, batch_size // 2), batch_size)
            events = [random_event() for _ in range(count)]

        payload = events[0] if len(events) == 1 else events

        try:
            resp = session.post(url, json=payload, timeout=5)
            if resp.status_code == 202:
                stats.record_ok(len(events))
            else:
                stats.record_err()
                print(f"\nUnexpected {resp.status_code}: {resp.text[:120]}", flush=True)
        except requests.exceptions.ConnectionError:
            stats.record_err()
            print("\nConnection refused — is the server running?", flush=True)
            time.sleep(2)
        except requests.exceptions.Timeout:
            stats.record_err()
            print("\nRequest timed out", flush=True)

        stats.maybe_print()

        # Pace to the requested event rate (batch counts as batch_size slots)
        next_send += interval * len(events)
        sleep_for = next_send - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # We're behind; reset the target so we don't spiral into a burst
            next_send = time.monotonic()


def main():
    parser = argparse.ArgumentParser(description="http-parquet load generator")
    parser.add_argument("--url", default="http://localhost:8080/ingest",
                        help="Ingest endpoint (default: http://localhost:8080/ingest)")
    parser.add_argument("--tenant-id", default="default",
                        help="Tenant id used for storage")
    parser.add_argument("--rate", type=float, default=50.0,
                        help="Target events per second (default: 50)")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Max events per batched request (default: 20)")
    parser.add_argument("--single-ratio", type=float, default=0.3,
                        help="Fraction of requests sent as single objects vs arrays (default: 0.3)")
    args = parser.parse_args()

    def _shutdown(sig, frame):
        print("\n\nStopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    run(
        url=f"{args.url}/{args.tenant_id}",
        rate=args.rate,
        batch_size=args.batch_size,
        single_ratio=args.single_ratio,
    )


if __name__ == "__main__":
    main()
