#!/usr/bin/env python3
"""
Producer client for YAK - Yet Another Kafka
Compatible with leader/follower broker setup:
- /metadata/leader returns {"leader": "node1"} or {"leader": "node2"}
- Brokers expect raw bytes with 'Topic' header at /produce/
"""

import requests
import uuid
import json
import time
import os
import argparse
import random
from typing import List, Optional

# ---------------- Config ----------------
NODE_IPS = {
    "node1": "ip1",   # leader node IP:PORT
    "node2": "ip2",  # follower node IP:PORT
}

PENDING_FILE = "pending.json"
DELIVERED_FILE = "delivered.json"
REQUEST_TIMEOUT = 5
MAX_RETRIES = 8
BACKOFF_BASE = 0.5
# ----------------------------------------


class Producer:
    def __init__(self, brokers: Optional[List[str]] = None):
        if not brokers:
            brokers = [f"http://{addr}" for addr in NODE_IPS.values()]
            print(f"[producer] Using default brokers: {brokers}")

        self.brokers = brokers
        self.leader = None
        self._load_state()

    # ---------- State ----------
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
            print(f"[producer] Warning: {path} corrupted ({e}) — resetting")
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

    # ---------- Queue ----------
    def enqueue(self, payload: str, topic: str):
        msg_id = str(uuid.uuid4())
        self.pending.append({
            "id": msg_id,
            "payload": payload,
            "topic": topic,
            "attempts": 0,
            "last_error": None,
        })
        self._save_state()
        return msg_id

    # ---------- Leader Discovery ----------
    def find_leader(self) -> Optional[str]:
        """Query /metadata/leader and map node name → IP"""
        for broker in self.brokers:
            try:
                url = broker.rstrip("/") + "/metadata/leader"
                print(f"[producer] Querying {url}...")
                resp = requests.get(url, timeout=REQUEST_TIMEOUT)

                if resp.status_code == 200:
                    j = resp.json()
                    node_name = j.get("leader")
                    if node_name:
                        node_name = node_name.strip().lower()
                        if node_name in NODE_IPS:
                            leader_url = f"http://{NODE_IPS[node_name]}"
                            print(f"[producer] ✓ Leader is {node_name} ({leader_url})")
                            self.leader = leader_url
                            return self.leader
                        else:
                            print(f"[producer] Unknown node '{node_name}', not in NODE_IPS map.")
                else:
                    print(f"[producer] {broker} returned status {resp.status_code}")

            except requests.RequestException as e:
                print(f"[producer] Network error contacting {broker}: {e}")
                continue

        print("[producer] ⚠ No leader found among brokers.")
        self.leader = None
        return None

    # ---------- Send ----------
    def _produce_once(self, leader_url: str, item: dict):
        """Send one message"""
        url = leader_url.rstrip("/") + "/produce/"
        data = item["payload"].encode("utf-8")
        headers = {"Topic": item["topic"], "Idempotency-Key": item["id"]}

        try:
            resp = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            return False, f"network:{e}"

        if 200 <= resp.status_code < 300:
            try:
                return True, int(resp.text.strip()) if resp.text.strip().isdigit() else resp.text
            except Exception:
                return True, resp.text
        else:
            try:
                j = resp.json()
                msg = j.get("detail") or j.get("error") or resp.text
            except Exception:
                msg = resp.text
            return False, f"status:{resp.status_code}:{msg}"

    def _is_leader_error(self, msg: str) -> bool:
        msg = msg.lower()
        return any(k in msg for k in ["not the leader", "follower cannot accept", "status:403"])

    # ---------- Flush ----------
    def flush(self):
        """Send all pending messages with retries."""
        if not self.pending:
            print("[producer] No pending messages.")
            return

        i = 0
        while i < len(self.pending):
            item = self.pending[i]
            if item["id"] in self.delivered:
                self.pending.pop(i)
                continue

            if not self.leader:
                self.find_leader()
                if not self.leader:
                    backoff = min(BACKOFF_BASE * (1.5 ** item["attempts"]) + random.random() * 0.1, 8.0)
                    print(f"[producer] No leader found, retrying in {backoff:.2f}s...")
                    time.sleep(backoff)
                    item["attempts"] += 1
                    continue

            print(f"[producer] Sending {item['id']} → {self.leader} (topic={item['topic']})")
            ok, info = self._produce_once(self.leader, item)

            if ok:
                print(f"[producer] ✓ Delivered {item['id']} @ offset {info}")
                self.delivered.add(item["id"])
                self.pending.pop(i)
                self._save_state()
                continue
            else:
                print(f"[producer] ✗ Failed {item['id']}: {info}")
                item["attempts"] += 1
                item["last_error"] = info

                if self._is_leader_error(info):
                    print("[producer] Leader mismatch, rediscovering...")
                    self.leader = None
                    self.find_leader()
                else:
                    print("[producer] Retrying after short delay...")
                    time.sleep(min(1.5 ** item["attempts"], 10))

                if item["attempts"] > MAX_RETRIES:
                    print(f"[producer] ✗ Dropping {item['id']} after {item['attempts']} attempts.")
                    self.pending.pop(i)
                    self._save_state()
                    continue
                i += 1

    def produce_and_wait(self, payload: str, topic: str):
        """Enqueue a single message and flush until delivered."""
        msg_id = self.enqueue(payload, topic)
        print(f"[producer] Enqueued {msg_id} for topic={topic}")

        for _ in range(10):
            self.flush()
            if msg_id in self.delivered:
                print(f"[producer] ✓✓✓ {msg_id} delivered successfully")
                return True
            time.sleep(0.3)
        print(f"[producer] ✗✗✗ Timeout waiting for {msg_id}")
        return False


# ---------- Restaurant Logic ----------
def split_restaurant_into_topics(restaurant: dict):
    """Split restaurant dict into 3 topic messages"""
    topic1_data = {"grades": restaurant.get("grades", []), "name": restaurant.get("name", "")}
    topic2_data = {"contact": restaurant.get("contact", {})}
    topic3_data = {"stars": restaurant.get("stars", 0), "categories": restaurant.get("categories", [])}

    return [
        ("topic1", json.dumps(topic1_data)),
        ("topic2", json.dumps(topic2_data)),
        ("topic3", json.dumps(topic3_data))
    ]


def send_restaurants_from_file(producer: Producer, filepath: str, delay: float = 0.1):
    """Load restaurants.json and send each record across 3 topics"""
    try:
        with open(filepath, 'r') as f:
            restaurants = json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return 0, 0

    if not isinstance(restaurants, list):
        print("Error: JSON must be an array")
        return 0, 0

    print(f"\n{'='*60}")
    print(f"Loaded {len(restaurants)} restaurants from {filepath}")
    print(f"Will send {len(restaurants)*3} messages across 3 topics")
    print(f"{'='*60}\n")

    success = failed = 0
    for idx, restaurant in enumerate(restaurants, 1):
        name = restaurant.get("name", "Unknown")
        print(f"\n[{idx}/{len(restaurants)}] Processing: {name}")
        topic_messages = split_restaurant_into_topics(restaurant)

        for topic, payload in topic_messages:
            if producer.produce_and_wait(payload, topic):
                success += 1
            else:
                failed += 1
                print(f"  ⚠ Failed to send to {topic}")

        if delay > 0 and idx < len(restaurants):
            time.sleep(delay)

    print(f"\n{'='*60}")
    print(f"✅ Successfully sent: {success}/{len(restaurants)*3}")
    print(f"❌ Failed: {failed}/{len(restaurants)*3}")
    print(f"{'='*60}\n")
    return success, failed


# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="YAK Producer - Leader Discovery + File mode")
    p.add_argument("--brokers", nargs="+", help="List of broker URLs (optional)")
    p.add_argument("--file", default="restaurants.json", help="Path to restaurants JSON file")
    p.add_argument("--delay", type=float, default=0.1, help="Delay between restaurants (s)")
    p.add_argument("--topic", help="Topic for manual message (topic1, topic2, topic3)")
    p.add_argument("message", nargs="?", help="Manual message text")
    return p.parse_args()


def main():
    args = parse_args()
    print("=" * 60)
    print("YAK Producer - Leader Discovery via /metadata/leader")
    print("=" * 60)

    producer = Producer(args.brokers)

    if args.message:
        if not args.topic:
            print("Error: --topic required when sending a manual message")
            return
        success = producer.produce_and_wait(args.message, args.topic)
        exit(0 if success else 1)
    else:
        print(f"\nFile mode: reading from {args.file}")
        success, failed = send_restaurants_from_file(producer, args.file, args.delay)
        exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()