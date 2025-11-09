# consumer.py
import os
import json
import csv
from datetime import datetime
import asyncio
import httpx
from fastapi import FastAPI
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
TOPICS = [
    t.strip() for t in os.environ.get("KNOWN_TOPICS", "topic1,topic2,topic3").split(",")
]
BROKERS = [
    f"http://{b}" if not b.startswith("http") else b
    for b in os.environ.get("BROKERS", "172.24.248.219:8000,172.24.224.84:8000").split(
        ","
    )
]
OFFSET_DIR = os.path.join(os.getcwd(), "consumer_offsets")
os.makedirs(OFFSET_DIR, exist_ok=True)
DATA_DIR = os.path.join(os.getcwd(), "consumer_data")
os.makedirs(DATA_DIR, exist_ok=True)
JOIN_DIR = os.path.join(DATA_DIR, "joined")
os.makedirs(JOIN_DIR, exist_ok=True)
CONSUMER_PORT = int(os.environ.get("CONSUMER_PORT", 7000))


def offset_file(topic):
    return os.path.join(OFFSET_DIR, f"offset_{topic}.json")


def load_offsets() -> Dict[str, int]:
    """
    Load offsets for all topics from files.
    Returns a dict of topic -> offset.
    ## Args:
    - Topics: List of topics to load offsets for.
    - Offset Directory: Directory where offset files are stored.
    ## Returns:
    - A dictionary mapping topic names to their offsets.

    """
    out = {}
    for t in TOPICS:
        fn = offset_file(t)
        try:
            with open(fn, "r") as f:
                data = json.load(f)
            # Accept multiple possible JSON shapes: an int, {"offset": n}, {topic: n}, or a single-value dict
            if isinstance(data, int):
                out[t] = data
            elif isinstance(data, dict):
                if "offset" in data and isinstance(data["offset"], int):
                    out[t] = data["offset"]
                elif t in data and isinstance(data[t], int):
                    out[t] = data[t]
                else:
                    ints = [v for v in data.values() if isinstance(v, int)]
                    out[t] = ints[0] if ints else -1
            else:
                out[t] = -1
        except FileNotFoundError:
            # If the offset file doesn't exist yet, default to -1
            out[t] = -1
    return out


def save_offset(topic: str, offset: int) -> None:
    """Persist offset as JSON so load_offsets can parse numeric values reliably"""
    with open(offset_file(topic), "w") as f:
        json.dump(offset, f)


def map_ip_to_host(ip: str) -> str:
    """Map an IP address to a hostname using a predefined mapping."""
    ip_host_map = {"172.24.224.84": "node1", "172.24.248.219": "node2"}
    return ip_host_map.get(ip, ip)


def safe_key_for_filename(key: str) -> str:
    """Return a filesystem-safe filename for a group key."""
    if not key:
        return "unknown"
    # Replace path separators and unsafe chars
    out = str(key).strip().replace(os.sep, "_")
    out = out.replace(" ", "_")
    # limit length to reasonable size
    return out[:200]


def get_group_key(topic: str, payload_obj, offset: int) -> str:
    """Derive a grouping key for a message payload.

    Heuristics used:
    - If payload has 'name' field, use that.
    - If payload has nested 'contact' with 'email', use the email.
    - If payload has an 'id' field, use it.
    - If payload has 'categories' (topic3), use the first category.
    - Fallback to offset-based key.
    """
    try:
        if isinstance(payload_obj, dict):
            if "name" in payload_obj and payload_obj["name"]:
                return str(payload_obj["name"])
            if "contact" in payload_obj and isinstance(payload_obj["contact"], dict):
                email = payload_obj["contact"].get("email")
                if email:
                    return str(email)
            if "id" in payload_obj:
                return str(payload_obj["id"])
            if (
                "categories" in payload_obj
                and isinstance(payload_obj["categories"], list)
                and payload_obj["categories"]
            ):
                return str(payload_obj["categories"][0])
    except Exception:
        pass
    return f"offset_{offset}"





def merge_into_joined(
    key: str, topic: str, payload_raw: str, payload_obj, offset: int
) -> None:
    """Merge the incoming message into a joined JSON record for `key`.

    Stores per-key JSON in JOIN_DIR/<key>.json and updates an index CSV.
    """
    fn = os.path.join(JOIN_DIR, f"{safe_key_for_filename(key)}.json")
    record = {}
    if os.path.exists(fn):
        try:
            with open(fn, "r", encoding="utf-8") as f:
                record = json.load(f)
        except Exception:
            record = {}

    # ensure fields
    record.setdefault("key", key)
    record.setdefault("first_seen", None)
    record.setdefault("last_seen", None)
    record.setdefault("topics", {})

    now = datetime.utcnow().isoformat() + "Z"
    if not record.get("first_seen"):
        record["first_seen"] = now
    record["last_seen"] = now

    # store topic payload (raw and parsed) and offset
    topic_entry = {
        "offset": offset,
        "raw": payload_raw,
        "parsed": payload_obj,
        "updated": now,
    }
    record["topics"][topic] = topic_entry

    try:
        with open(fn, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[consumer] Error saving joined record {fn}: {e}")

    # update index CSV
    index_fn = os.path.join(JOIN_DIR, "index.csv")
    # load existing index into memory
    index = {}
    if os.path.exists(index_fn):
        try:
            with open(index_fn, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    index[r["key"]] = r
        except Exception:
            index = {}

    index[key] = {
        "key": key,
        "last_seen": record["last_seen"],
        "topics": ",".join(sorted(record["topics"].keys())),
        "json_file": os.path.basename(fn),
    }

    # write back index
    try:
        with open(index_fn, "w", encoding="utf-8", newline="") as f:
            fieldnames = ["key", "last_seen", "topics", "json_file"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for v in index.values():
                writer.writerow(v)
    except Exception as e:
        print(f"[consumer] Error updating joined index {index_fn}: {e}")


def write_message_csv(
    topic: str, group_key: str, offset: int, payload_raw: str
) -> None:
    """Append a message to a CSV file grouped by topic and group_key.

    CSV columns: offset, timestamp_iso, payload_json
    """
    topic_dir = os.path.join(DATA_DIR, topic)
    os.makedirs(topic_dir, exist_ok=True)
    safe_key = safe_key_for_filename(group_key)
    fn = os.path.join(topic_dir, f"{safe_key}.csv")
    write_header = not os.path.exists(fn)
    ts = datetime.utcnow().isoformat() + "Z"
    try:
        with open(fn, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["offset", "timestamp", "payload"])
            writer.writerow([offset, ts, payload_raw])
    except Exception as e:
        print(f"[consumer] Error writing message to {fn}: {e}")


def extract_id(payload_obj) -> str:
    """Extract the document _id (prefer $oid) from payload object if present."""
    if not isinstance(payload_obj, dict):
        return None
    _id = payload_obj.get("_id")
    if isinstance(_id, dict):
        # common shape: {"$oid": "..."}
        oid = _id.get("$oid")
        if oid:
            return str(oid)
            return None


def write_topic_row(topic: str, payload_obj, offset: int) -> None:
    """Write a structured row into a per-topic CSV using attribute names as columns.

    topic1 -> columns: _id, name, grades
    topic2 -> columns: _id, contact_phone, contact_email, contact_location
    topic3 -> columns: _id, stars, categories
    """
    if not isinstance(payload_obj, dict):
        return

    tid = extract_id(payload_obj) or ""
    topic_dir = os.path.join(DATA_DIR, topic)
    os.makedirs(topic_dir, exist_ok=True)
    fn = os.path.join(topic_dir, "data.csv")
    write_header = not os.path.exists(fn)

    try:
        with open(fn, "a", newline="", encoding="utf-8") as f:
            if topic == "topic1":
                fieldnames = ["_id", "name", "grades"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerow({
                    "_id": tid,
                    "name": payload_obj.get("name", ""),
                    "grades": json.dumps(payload_obj.get("grades", []), ensure_ascii=False),
                })
            elif topic == "topic2":
                fieldnames = ["_id", "contact_phone", "contact_email", "contact_location"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                contact = payload_obj.get("contact") or {}
                writer.writerow({
                    "_id": tid,
                    "contact_phone": contact.get("phone", ""),
                    "contact_email": contact.get("email", ""),
                    "contact_location": json.dumps(contact.get("location", []), ensure_ascii=False),
                })
            elif topic == "topic3":
                fieldnames = ["_id", "stars", "categories"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerow({
                    "_id": tid,
                    "stars": payload_obj.get("stars", ""),
                    "categories": json.dumps(payload_obj.get("categories", []), ensure_ascii=False),
                })
            else:
                # generic: store entire JSON
                fieldnames = ["_id", "payload"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerow({"_id": tid, "payload": json.dumps(payload_obj, ensure_ascii=False)})
    except Exception as e:
        print(f"[consumer] Error writing structured topic row to {fn}: {e}")


@app.get("/collate")
def collate(joined_csv: str = None) -> Dict[str, str]:
    """Collate per-topic CSVs into one CSV keyed by _id.

    The output CSV will contain columns:
    _id, name, grades, contact_phone, contact_email, contact_location, stars, categories
    """
    topic1_fn = os.path.join(DATA_DIR, "topic1", "data.csv")
    topic2_fn = os.path.join(DATA_DIR, "topic2", "data.csv")
    topic3_fn = os.path.join(DATA_DIR, "topic3", "data.csv")

    records = {}

    def ensure_rec(_id):
        if _id not in records:
            records[_id] = {
                "_id": _id,
                "name": "",
                "grades": "[]",
                "contact_phone": "",
                "contact_email": "",
                "contact_location": "[]",
                "stars": "",
                "categories": "[]",
            }

    # read topic1
    if os.path.exists(topic1_fn):
        with open(topic1_fn, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                _id = r.get("_id") or ""
                ensure_rec(_id)
                records[_id]["name"] = r.get("name", "")
                records[_id]["grades"] = r.get("grades", "[]")

    # topic2
    if os.path.exists(topic2_fn):
        with open(topic2_fn, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                _id = r.get("_id") or ""
                ensure_rec(_id)
                records[_id]["contact_phone"] = r.get("contact_phone", "")
                records[_id]["contact_email"] = r.get("contact_email", "")
                records[_id]["contact_location"] = r.get("contact_location", "[]")

    # topic3
    if os.path.exists(topic3_fn):
        with open(topic3_fn, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                _id = r.get("_id") or ""
                ensure_rec(_id)
                records[_id]["stars"] = r.get("stars", "")
                records[_id]["categories"] = r.get("categories", "[]")

    out_fn = joined_csv or os.path.join(JOIN_DIR, "collated.csv")
    fieldnames = [
        "_id",
        "name",
        "grades",
        "contact_phone",
        "contact_email",
        "contact_location",
        "stars",
        "categories",
    ]
    try:
        with open(out_fn, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for _id, rec in records.items():
                writer.writerow({k: rec.get(k, "") for k in fieldnames})
    except Exception as e:
        return {"error": str(e)}

    return {"collated": out_fn, "rows": len(records)}


def find_leader() -> str:
    """Discover the current leader broker from the list of brokers."""
    for broker_url in BROKERS:
        try:
            url = broker_url.rstrip("/") + "/metadata/leader"
            resp = httpx.get(url, timeout=2.0)
            j = resp.json()
            leader = j.get("leader")

            if leader:
                leader = leader.strip().lower()
                # Map node name to IP:port
                if leader == "node1":
                    return "172.24.224.84:8000"
                elif leader == "node2":
                    return "172.24.248.219:8000"
                else:
                    # If it's already in IP:port format
                    return leader
        except Exception as e:
            print(f"[consumer] Error finding leader in broker {broker_url}: {e}")
            continue
    print("[consumer] ⚠ No leader found")
    return None


@app.get("/offsets")
def offsets() -> Dict[str, int]:
    """Calling load_offsets to retrieve current offsets on the /offsets endpoint"""
    return load_offsets()


@app.get("/pull")
def pull(topic: str, from_offset: int, to_offset: int = None) -> Dict[str, int]:
    """Pull messages for a topic from the leader broker's /consume endpoint.

    The brokers expose a byte-oriented /consume endpoint which returns raw messages
    appended as lines (message + "\n"). Offsets are byte offsets. This endpoint
    will request the leader's /consume?topic=<>&offset=<from_offset> and parse the
    returned bytes, splitting on newlines and advancing byte offsets accordingly.
    """
    leader = find_leader()
    if not leader:
        return {"error": "no leader"}

    # ensure we have a non-negative start offset
    if from_offset is None or from_offset < 0:
        from_offset = 0

    # leader should be host:port
    url = f"http://{leader}/consume/"
    try:
        r = httpx.get(url, params={"topic": topic, "offset": from_offset}, timeout=10.0)
    except Exception as e:
        return {"error": str(e)}

    if r.status_code != 200:
        return {"error": f"leader returned {r.status_code}"}

    raw = r.content
    if not raw:
        return {"pulled": 0}

    parts = raw.split(b"\n")
    pulled = 0
    cur_offset = from_offset
    for p in parts:
        if not p:
            continue
        try:
            payload = p.decode("utf-8", errors="replace")
        except Exception:
            payload = str(p)
        print(f"CONSUME {topic} {cur_offset} -> {payload}")
        # attempt to parse JSON payload to extract grouping key
        msg_offset = cur_offset
        try:
            payload_obj = json.loads(payload)
        except Exception:
            payload_obj = None

        group_key = get_group_key(topic, payload_obj, msg_offset)
        # persist the message grouped by the derived key
        write_message_csv(topic, group_key, msg_offset, payload)
        # also write structured per-topic row (columns) when available
        try:
            write_topic_row(topic, payload_obj, msg_offset)
        except Exception as e:
            print(f"[consumer] Warning: write_topic_row failed: {e}")

        # use document _id as the canonical join key when available
        try:
            ckey = extract_id(payload_obj) or group_key
        except Exception:
            ckey = group_key
        merge_into_joined(ckey, topic, payload, payload_obj, msg_offset)

        pulled += 1
        cur_offset += len(p) + 1  # account for terminating newline byte
        save_offset(topic, cur_offset)  # Save AFTER incrementing to next message

    return {"pulled": pulled}


@app.get("/health")
def health() -> Dict[str, bool]:
    """Health check endpoint."""
    try:
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


async def consume_task():
    """Background task to continuously consume messages from all topics."""
    print("[consumer] Starting background consumption task...")
    while True:
        try:
            offsets = load_offsets()
            for topic in TOPICS:
                # Start from 0 if no offset exists yet (not -1, as brokers reject negative offsets)
                current_offset = offsets.get(topic, 0)
                if current_offset < 0:
                    current_offset = 0
                result = pull(topic, from_offset=current_offset)
                if result.get("pulled", 0) > 0:
                    print(
                        f"[consumer] ✓ Pulled {result['pulled']} messages from {topic}"
                    )
            await asyncio.sleep(2)  # Poll every 2 seconds
        except Exception as e:
            print(f"[consumer] Error in consume_task: {e}")
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    """Start the background consumption task on app startup."""
    asyncio.create_task(consume_task())
    print("[consumer] Startup complete - listening for messages...")


if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("YAK Consumer - Auto-Consumption Mode")
    print("=" * 60)
    print(f"Topics: {TOPICS}")
    print(f"Brokers: {BROKERS}")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=7000)