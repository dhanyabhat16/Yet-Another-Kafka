#!/usr/bin/env python3
<<<<<<< HEAD
=======
"""
Producer client for YAK - Yet Another Kafka
Compatible with broker that uses:
- Topic-based routing via headers
- Raw byte payloads (not JSON)
- Simple /metadata/leader discovery (no Redis needed)
"""

>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
import requests
import uuid
import json
import time
import os
import argparse
import random
from typing import List, Optional

<<<<<<< HEAD
# Configurations finding leader and follower nodes
NODE_IPS = {"node1": "IP1","node2": "IP2",}

PENDING_FILE = "pending.json"
DELIVERED_FILE = "delivered.json"
REQUEST_TIMEOUT = 2 #failover detection
MAX_RETRIES = 8
BACKOFF_BASE = 0.3  #faster retry

# Producer class creation for functions
class Producer:
    def __init__(self, brokers: Optional[List[str]] = None):
        if not brokers:
            brokers = [f"http://{addr}" for addr in NODE_IPS.values()]
            print(f"[producer] Using default brokers: {brokers}")

        self.brokers = brokers
        self.leader = None
        self._load_state()

    # State
=======
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

>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
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
<<<<<<< HEAD
            print(f"[producer] Warning: {path} corrupted ({e}) — resetting")
=======
            print(f"[producer] warning: {path} corrupted ({e}), backing up")
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
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

<<<<<<< HEAD
    # Queueing mssgs for sending
    def enqueue(self, payload: str, topic: str):
        if self.already_pending(payload, topic):
            print(f"[producer] SKIP duplicate payload for topic={topic}")
            return None  # skip duplicate

        msg_id = str(uuid.uuid4())
        self.pending.append({
=======
    def enqueue(self, payload: str, topic: str):
        """Enqueue a message with topic"""
        msg_id = str(uuid.uuid4())
        item = {
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
            "id": msg_id,
            "payload": payload,
            "topic": topic,
            "attempts": 0,
            "last_error": None,
<<<<<<< HEAD
        })
        self._save_state()
        return msg_id
        
    def already_pending(self, payload: str, topic: str) -> bool:
        """Check if an identical payload-topic combo is already pending."""
        for item in self.pending:
            if item["topic"] == topic and item["payload"] == payload:
                return True
        return False

    def find_leader(self) -> Optional[str]:
        """
        Query all brokers for /metadata/leader and pick a candidate.
        Shuffle brokers to avoid fixed ordering bias. If a candidate is
        returned we set leader to the resolved URL but we do NOT treat this
        as authoritative if produce attempts to it fail — we will try other
        brokers immediately.
        """
        # randomize order so we don't always hit the same broker first
        brokers = list(self.brokers)
        random.shuffle(brokers)

        candidates = {}
        for broker in brokers:
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
                            # map to leader url
                            leader_url = f"http://{NODE_IPS[node_name]}"
                            candidates[leader_url] = candidates.get(leader_url, 0) + 1
                        else:
                            print(f"[producer] ERROR : Unknown node '{node_name}' from {broker}")
                else:
                    print(f"[producer] {broker} returned status {resp.status_code}")
            except requests.RequestException as e:
                print(f"[producer] ERROR : Network error contacting {broker}: {e}")
                continue

        if not candidates:
            print("[producer] ERROR : No leader info returned by any broker.")
            self.leader = None
            return None

        # pick the most commonly reported leader (simple majority heuristic)
        leader_url = max(candidates.items(), key=lambda kv: kv[1])[0]
        print(f"[producer] SUCCESS : Candidate leader chosen: {leader_url} (votes: {candidates[leader_url]})")
        self.leader = leader_url
        return leader_url


    def _produce_once(self, leader_url: str, item: dict):
        """Send one message to a given leader_url. Return (ok, info)"""
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


    def _produce_with_failover(self, current_leader: Optional[str], item: dict):
        """
        Try to produce to current_leader first (if set). If that fails with a network error
        or non-leader response, iterate over other brokers and try them directly.
        If any broker accepts the message, update self.leader and return success.
        Returns (ok, info).
        """
        attempted = []
        # build a randomized list of targets with current leader first (if provided)
        targets = []
        if current_leader:
            targets.append(current_leader)
        others = [b for b in self.brokers if b.rstrip("/") != (current_leader or "").rstrip("/")]
        random.shuffle(others)
        targets.extend(others)

        last_error = None
        for target in targets:
            attempted.append(target)
            ok, info = self._produce_once(target, item)
            if ok:
                # success: update leader if necessary and return
                if self.leader != target:
                    print(f"[producer] *NOTE* : FAILOVER: switching leader to {target}")
                    self.leader = target
                return True, info
            else:
                last_error = info
                # immediate retry to next broker if network or leader error
                # but if status indicates unknown topic or bad request, we may stop
                low = info.lower()
                if "network:" in low or "not the leader" in low or "follower cannot accept" in low or low.startswith("status:403"):
                    print(f"[producer] WARN: target {target} failed with '{info}', trying next broker...")
                    continue
                else:
                    # for non-leader, non-network errors (e.g. bad payload), stop trying other brokers
                    print(f"[producer] ERROR: non-recoverable error from {target}: {info}")
                    return False, info

        return False, last_error or "no-targets-tried"


    def flush(self):
        """Send all pending messages with retries."""
        if not self.pending:
            print("[producer] No pending messages.")
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
            return

        i = 0
        while i < len(self.pending):
            item = self.pending[i]
<<<<<<< HEAD
            if item["id"] in self.delivered:
=======

            if item["id"] in self.delivered:
                print(f"[producer] message {item['id']} already delivered")
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
                self.pending.pop(i)
                self._save_state()
                continue

<<<<<<< HEAD
            # ensure we have a candidate leader (try metadata)
            if not self.leader:
                self.find_leader()
                if not self.leader:
                    # nothing returned from metadata queries - try to send to any broker directly
                    print("[producer] No leader from metadata; will attempt direct produce on brokers.")
            
            print(f"[producer] Sending {item['id']} → {self.leader or 'any-broker'} (topic={item['topic']})")
            ok, info = self._produce_with_failover(self.leader, item)

            if ok:
                print(f"[producer] SUCCESS :✓ Delivered {item['id']} @ offset {info}")
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
                self.delivered.add(item["id"])
                self.pending.pop(i)
                self._save_state()
                continue
            else:
<<<<<<< HEAD
                print(f"[producer] FAILED : ✗ Failed {item['id']}: {info}")
                item["attempts"] += 1
                item["last_error"] = info
                self._save_state()

                # If it's a leader mismatch or network error, we've already attempted failover above.
                # Now apply backoff and retry later.
                backoff = min(BACKOFF_BASE * (1.3 ** item["attempts"]), 3.0)
                print(f"[producer] Backing off {backoff:.2f}s...")
                time.sleep(backoff)

                if item["attempts"] > MAX_RETRIES:
                    print(f"[producer] ERROR : ✗ Dropping {item['id']} after {item['attempts']} attempts.")
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
                    self.pending.pop(i)
                    self._save_state()
                    continue

<<<<<<< HEAD
                # Try next pending item (or retry same item on next loop)
                continue


    def produce_and_wait(self, payload: str, topic: str):
        """Enqueue a single message and flush until delivered."""
        msg_id = self.enqueue(payload, topic)
        print(f"[producer] Enqueued {msg_id} for topic={topic}")

        # OPTIMIZED: Faster polling (0.2s instead of 0.3s)
        for _ in range(15):  # More attempts for reliability
            self.flush()
            if msg_id in self.delivered:
                print(f"[producer] SUCCESS : ✓✓✓ {msg_id} delivered successfully")
                return True
            time.sleep(0.2)
        print(f"[producer] ERROR : ✗✗✗ Timeout waiting for {msg_id}")
        return False


# ---------- Restaurant Logic (UPDATED) ----------

def extract_unique_key(restaurant: dict) -> str:
    """Extracts a stable unique key for each restaurant"""
    _id = restaurant.get("_id")
    if isinstance(_id, dict) and "$oid" in _id:
        return _id["$oid"]
    elif isinstance(_id, str):
        return _id
    else:
        # fallback if _id missing
        return str(uuid.uuid4())


def split_restaurant_into_topics(restaurant: dict):
    """
    Split restaurant dict into 3 topic messages, 
    all sharing a unique key for consumer correlation.
    """
    unique_key = extract_unique_key(restaurant)

    topic1_data = {
        "key": unique_key,
        "grades": restaurant.get("grades", []),
        "name": restaurant.get("name", "")
    }
    topic2_data = {
        "key": unique_key,
        "contact": restaurant.get("contact", {})
    }
    topic3_data = {
        "key": unique_key,
        "stars": restaurant.get("stars", 0),
        "categories": restaurant.get("categories", [])
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
    }

    return [
        ("topic1", json.dumps(topic1_data)),
        ("topic2", json.dumps(topic2_data)),
<<<<<<< HEAD
        ("topic3", json.dumps(topic3_data))
=======
        ("topic3", json.dumps(topic3_data)),
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
    ]


def send_restaurants_from_file(producer: Producer, filepath: str, delay: float = 0.1):
<<<<<<< HEAD
    """Load restaurants.json and send each record across 3 topics (with shared unique keys)."""
    try:
        with open(filepath, 'r') as f:
=======
    """Load restaurants.json and send each split across 3 topics"""
    try:
        with open(filepath, "r") as f:
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
            restaurants = json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return 0, 0

    if not isinstance(restaurants, list):
<<<<<<< HEAD
        print("Error: JSON must be an array")
        return 0, 0

    print(f"\n{'='*60}")
    print(f"Loaded {len(restaurants)} restaurants from {filepath}")
    print(f"Will send {len(restaurants)*3} messages (3 topics per restaurant)")
    print(f"{'='*60}\n")

    success = failed = 0
    for idx, restaurant in enumerate(restaurants, 1):
        name = restaurant.get("name", "Unknown")
        print(f"\n[{idx}/{len(restaurants)}] Processing: {name}")
        topic_messages = split_restaurant_into_topics(restaurant)

=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
        for topic, payload in topic_messages:
            if producer.produce_and_wait(payload, topic):
                success += 1
            else:
                failed += 1
<<<<<<< HEAD
                print(f" ERROR : ⚠ Failed to send to {topic}")
=======
                print(f"  ⚠ Failed to send to {topic}")
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc

        if delay > 0 and idx < len(restaurants):
            time.sleep(delay)

<<<<<<< HEAD
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
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
    return p.parse_args()


def main():
    args = parse_args()
<<<<<<< HEAD
    print("=" * 60)
    print("YAK Producer - Optimized for Fast Failover")
=======

    print("=" * 60)
    print("YAK Producer - Simple Leader Discovery")
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
    print("=" * 60)

    producer = Producer(args.brokers)

<<<<<<< HEAD
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
=======
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
>>>>>>> 7d629b5fb9cbbac0e307e3353ee74c5c62c7eadc
