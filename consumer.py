# consumer.py
import os
import time
import json
import httpx
from fastapi import FastAPI, Request
from typing import Dict
import threading
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
TOPICS = os.environ.get("KNOWN_TOPICS", "sports,finance").split(",")
LEADER_DISCOVERY = os.environ.get(
    "BROKERS", "http://127.0.0.1:8000,http://127.0.0.1:8001"
).split(",")
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


def find_leader() -> str:
    """Discover the current leader broker from the list of brokers."""
    for b in LEADER_DISCOVERY:
        try:
            r = httpx.get(f"{b}/metadata/leader", timeout=2.0).json()
            leader = r.get("leader")
            if leader:
                return leader
        except Exception as e:
            print(f"Error finding leader in broker {b}: {e}")
            continue
    return None


@app.get("/offsets")
def offsets() -> Dict[str, int]:
    """Calling load_offsets to retrieve current offsets on the /offsets endpoint"""
    return load_offsets()


@app.get("/pull")
def pull(topic: str, from_offset: int, to_offset: int) -> Dict[str, int]:
    """Pull messages for a topic from from_offset to to_offset from the leader broker on the /pull endpoint."""
    # on broker instruction, pull from leader then update local offset
    leader = find_leader()
    if not leader:
        return {"error": "no leader"}
    try:
        r = httpx.get(
            f"http://{leader}/consume",
            params={"topic": topic, "from_offset": from_offset, "to_offset": to_offset},
            timeout=10.0,
        )
    except Exception as e:
        return {"error": str(e)}
    if r.status_code != 200:
        return {"error": f"leader returned {r.status_code}"}
    j = r.json()
    msgs = j.get("messages", [])
    if not msgs:
        return {"pulled": 0}
    for m in msgs:
        # process message locally - here we just print and update offset
        print(f"CONSUME {topic} {m['offset']} -> {m['payload']}")
        save_offset(topic, m["offset"])
    return {"pulled": len(msgs)}


@app.get("/health")
def health() -> Dict[str, bool]:
    """Health check endpoint."""
    try:
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    print("Starting consumer, topics:", TOPICS)
    uvicorn.run(app, host="0.0.0.0", port=CONSUMER_PORT)
