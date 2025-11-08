#!/usr/bin/env python3
"""
Producer client for YAK - Yet Another Kafka
Compatible with broker that uses:
- Topic-based routing via headers
- Raw byte payloads (not JSON)
- Simple /metadata/leader discovery (no Redis needed)
"""

import requests
import uuid
import json
import time
import os
import argparse
import random
from typing import List, Optional

# Config
PENDING_FILE = "pending.json"
DELIVERED_FILE = "delivered.json"
REQUEST_TIMEOUT = 5
MAX_RETRIES = 8
BACKOFF_BASE = 0.5


class Producer:
    def __init__(self, brokers: List[str] = None):
        """
        brokers: List of broker URLs like "http://172.24.224.84:8000"
        """
        if brokers is None or len(brokers) == 0:
            # Default brokers - matches your consumer's LEADER_DISCOVERY format
            brokers = ["http://172.24.248.219:8000", "http://172.24.224.84:8000"]
            print(f"[producer] Using default brokers: {brokers}")

        self.brokers = brokers[:]
        self.leader: Optional[str] = None
        self._load_state()

    def _load_state(self):
        self.pending = self._load_json(PENDING_FILE, default=[])
        self.delivered = set(self._load_json(DELIVERED_FILE, default=[]))

    def _save_state(self):
        self._atomic_write(PENDING_FILE, self.pending)
        self._atomic_write(DELIVERED_FILE, list(self.delivered))

    @staticmethod
    def _load_json(path, default):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return default
        except Exception as e:
            print(f"[producer] warning: {path} corrupted ({e}), backing up")
            try:
                os.rename(path, path + ".bak")
            except Exception:
                pass
            return default

    @staticmethod
    def _atomic_write(path, data):
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    def enqueue(self, payload: str, topic: str):
        """Enqueue a message with topic"""
        msg_id = str(uuid.uuid4())
        item = {
            "id": msg_id,
            "payload": payload,
            "topic": topic,
            "attempts": 0,
            "last_error": None,
        }
        self.pending.append(item)
        self._save_state()
        return msg_id

    def find_leader(self, prefer: Optional[str] = None) -> Optional[str]:
        """
        Discover current leader by querying /metadata/leader on known brokers.
        Same approach as consumer's find_leader().
        """
        candidates = self.brokers[:]
        if prefer and prefer in candidates:
            candidates.remove(prefer)
            candidates.insert(0, prefer)

        if self.leader and self.leader not in candidates and self.leader != prefer:
            candidates.insert(0, self.leader)

        for broker_url in candidates:
            try:
                url = broker_url.rstrip("/") + "/metadata/leader"
                print(f"[producer] querying {url}...")
                resp = requests.get(url, timeout=REQUEST_TIMEOUT)

                if resp.status_code == 200:
                    try:
                        j = resp.json()
                        leader = j.get("leader")

                        if leader:
                            # Ensure it's a full URL
                            if not leader.startswith("http"):
                                leader = f"http://{leader}"

                            leader = leader.rstrip("/")
                            print(f"[producer] ✓ discovered leader: {leader}")
                            self.leader = leader
                            return self.leader

                    except Exception as e:
                        print(
                            f"[producer] Failed to parse response from {broker_url}: {e}"
                        )
                else:
                    print(f"[producer] {broker_url} returned status {resp.status_code}")

            except requests.RequestException as e:
                print(f"[producer] Network error querying {broker_url}: {e}")
                continue

        print("[producer] ⚠ Could not discover leader from any broker")
        self.leader = None
        return None

    def _produce_once(self, leader_url: str, item: dict):
        """
        Send message to broker with topic header and raw bytes.
        Returns (success, offset_or_error)
        """
        url = leader_url.rstrip("/") + "/produce/"

        # Convert payload to bytes (broker expects raw bytes)
        if isinstance(item["payload"], str):
            data = item["payload"].encode("utf-8")
        else:
            data = str(item["payload"]).encode("utf-8")

        headers = {"Topic": item["topic"], "Idempotency-Key": item["id"]}

        try:
            resp = requests.post(
                url, headers=headers, data=data, timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException as e:
            return False, f"network:{e}"

        # Broker returns integer offset
        if 200 <= resp.status_code < 300:
            try:
                # Try JSON first
                offset = resp.json() if resp.text else None
                return True, offset
            except:
                # Try raw integer
                try:
                    offset = int(resp.text.strip())
                    return True, offset
                except:
                    return False, "invalid-response-format"
        else:
            # Error response
            try:
                j = resp.json()
                err = j.get("detail") or j.get("error") or resp.text
            except:
                err = resp.text
            return False, f"status:{resp.status_code}:{err}"

    def _is_leader_error(self, error_msg: str) -> bool:
        """Detect if error indicates we contacted the wrong broker"""
        error_lower = str(error_msg).lower()
        indicators = [
            "not the leader",
            "follower cannot accept",
            "not leader",
            "status:403",
            "status:503",
        ]
        return any(ind in error_lower for ind in indicators)

    def flush(self):
        """Send all pending messages with retry logic"""
        if not self.pending:
            print("[producer] no pending messages.")
            return

        i = 0
        while i < len(self.pending):
            item = self.pending[i]

            if item["id"] in self.delivered:
                print(f"[producer] message {item['id']} already delivered")
                self.pending.pop(i)
                self._save_state()
                continue

            # Discover leader if needed
            if not self.leader:
                print("[producer] discovering leader...")
                found = self.find_leader()
                if not found:
                    backoff = min(
                        BACKOFF_BASE * (1.5 ** item["attempts"])
                        + random.random() * 0.1,
                        10.0,
                    )
                    print(f"[producer] no leader found, backing off {backoff:.2f}s")
                    time.sleep(backoff)
                    item["attempts"] += 1
                    item["last_error"] = "no-leader"
                    self._save_state()

                    if item["attempts"] > MAX_RETRIES:
                        print(f"[producer] ✗ giving up on {item['id']}")
                        self.pending.pop(i)
                        self._save_state()
                        continue
                    continue

            # Try to produce
            topic = item.get("topic", "unknown")
            print(
                f"[producer] sending {item['id']} to '{topic}' via {self.leader} (attempt {item['attempts'] + 1})"
            )
            success, info = self._produce_once(self.leader, item)

            if success:
                print(
                    f"[producer] ✓ delivered {item['id']} to '{topic}' at offset={info}"
                )
                self.delivered.add(item["id"])
                self.pending.pop(i)
                self._save_state()
                continue
            else:
                print(f"[producer] ✗ failed {item['id']}: {info}")
                item["attempts"] += 1
                item["last_error"] = str(info)
                self._save_state()

                # Handle leader errors
                if self._is_leader_error(str(info)):
                    print("[producer] leader mismatch detected, rediscovering...")
                    old = self.leader
                    new = self.find_leader()
                    if new and new != old:
                        print(f"[producer] leader changed: {old} → {new}")
                    if not new:
                        self.leader = None
                else:
                    print("[producer] network/server error, rediscovering leader...")
                    self.find_leader()

                # Backoff
                backoff = min(
                    BACKOFF_BASE * (1.5 ** item["attempts"]) + random.random() * 0.1,
                    10.0,
                )
                print(f"[producer] backing off {backoff:.2f}s...")
                time.sleep(backoff)

                if item["attempts"] > MAX_RETRIES:
                    print(
                        f"[producer] ✗✗ dropping {item['id']} after {item['attempts']} attempts"
                    )
                    print(f"[producer] last error: {item['last_error']}")
                    self.pending.pop(i)
                    self._save_state()
                    continue

                continue

    def produce_and_wait(self, payload: str, topic: str):
        """Enqueue and flush until delivered"""
        msg_id = self.enqueue(payload, topic)
        print(f"[producer] enqueued {msg_id} for '{topic}'")

        max_iterations = 100
        iteration = 0
        while iteration < max_iterations:
            if msg_id in self.delivered:
                print(f"[producer] ✓✓✓ {msg_id} confirmed")
                return True
            if not any(p["id"] == msg_id for p in self.pending):
                print(f"[producer] ✗✗✗ {msg_id} dropped")
                return False

            self.flush()
            time.sleep(0.2)
            iteration += 1

        print(f"[producer] timeout waiting for {msg_id}")
        return False


def split_restaurant_into_topics(restaurant: dict):
    """
    Split restaurant into 3 topics:
    - topic1: grades + name
    - topic2: contact
    - topic3: stars + categories
    """
    topic1_data = {
        "grades": restaurant.get("grades", []),
        "name": restaurant.get("name", ""),
    }

    topic2_data = {"contact": restaurant.get("contact", {})}

    topic3_data = {
        "stars": restaurant.get("stars", 0),
        "categories": restaurant.get("categories", []),
    }

    return [
        ("topic1", json.dumps(topic1_data)),
        ("topic2", json.dumps(topic2_data)),
        ("topic3", json.dumps(topic3_data)),
    ]


def send_restaurants_from_file(producer: Producer, filepath: str, delay: float = 0.1):
    """Load restaurants.json and send each split across 3 topics"""
    try:
        with open(filepath, "r") as f:
            restaurants = json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return 0, 0

    if not isinstance(restaurants, list):
        print("Error: must be JSON array")
        return 0, 0

    print(f"\n{'=' * 60}")
    print(f"Loaded {len(restaurants)} restaurants from {filepath}")
    print(f"Will send {len(restaurants) * 3} messages across 3 topics")
    print(f"{'=' * 60}\n")

    success = 0
    failed = 0

    for idx, restaurant in enumerate(restaurants, 1):
        name = restaurant.get("name", "Unknown")
        print(f"\n[{idx}/{len(restaurants)}] Processing: {name}")

        # Split into 3 topics
        topic_messages = split_restaurant_into_topics(restaurant)

        # Send each topic
        for topic, payload in topic_messages:
            if producer.produce_and_wait(payload, topic):
                success += 1
            else:
                failed += 1
                print(f"  ⚠ Failed to send to {topic}")

        if delay > 0 and idx < len(restaurants):
            time.sleep(delay)

    print(f"\n{'=' * 60}")
    print(f"✅ Successfully sent: {success}/{len(restaurants) * 3}")
    print(f"❌ Failed: {failed}/{len(restaurants) * 3}")
    print(f"{'=' * 60}\n")

    return success, failed


def parse_args():
    p = argparse.ArgumentParser(description="YAK Producer - Simple Leader Discovery")
    p.add_argument(
        "--brokers",
        nargs="+",
        required=False,
        help="Broker URLs like http://172.24.224.84:8000",
    )
    p.add_argument(
        "--file", default="restaurants.json", help="Path to restaurants JSON file"
    )
    p.add_argument(
        "--delay", type=float, default=0.1, help="Delay between restaurants (seconds)"
    )
    p.add_argument(
        "--topic", default=None, help="Topic for manual message (topic1/topic2/topic3)"
    )
    p.add_argument(
        "message",
        nargs="?",
        help="Manual message (requires --topic). Omit for file mode.",
    )
    return p.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("YAK Producer - Simple Leader Discovery")
    print("=" * 60)

    producer = Producer(args.brokers)

    # Manual message mode
    if args.message:
        if not args.topic:
            print("Error: --topic required for manual messages")
            print("Use: --topic topic1 (or topic2, topic3)")
            exit(1)
        success = producer.produce_and_wait(args.message, args.topic)
        exit(0 if success else 1)

    # File mode (default)
    print(f"\nFile mode: reading from {args.file}")
    success, failed = send_restaurants_from_file(producer, args.file, args.delay)
    exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
