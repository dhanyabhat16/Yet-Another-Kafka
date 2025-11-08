# consumer.py
import os
import json
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
