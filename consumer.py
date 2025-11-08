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
TOPICS = os.environ.get("KNOWN_TOPICS", "topic1, topic2, topic3").split(",")
LEADER_DISCOVERY = os.environ.get(
    "BROKERS", "172.24.248.219:8000,172.24.224.84:8000"
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


def map_ip_to_host(ip: str) -> str:
    """Map an IP address to a hostname using a predefined mapping."""
    ip_host_map = {"172.24.224.84": "node1", "172.24.248.219": "node2"}
    return ip_host_map.get(ip, ip)


def find_leader() -> str:
    """Discover the current leader broker from the list of brokers."""
    # LEADER_DISCOVERY contains broker base URLs like http://host:port
    for b in LEADER_DISCOVERY:
        try:
            resp = httpx.get(f"{b}/metadata/leader", timeout=2.0)
            j = resp.json()
            leader = j.get("leader")
            self_addr = j.get("self")
            # If the leader value already encodes host:port, return it.
            if leader and ":" in str(leader):
                # strip any http scheme if present
                return str(leader).replace("http://", "").replace("https://", "")
            # If this broker reports that it is the leader, return its self address
            if leader and self_addr and str(leader) == str(self_addr):
                return str(self_addr)
            # As a fallback, if the metadata returns 'self' and it equals this broker
            if self_addr and resp.url.host:
                # return the broker address we just queried (host:port)
                host = resp.url.host
                port = resp.url.port
                if host and port:
                    return f"{map_ip_to_host(host)}:{host}:{port}"
        except Exception as e:
            print(f"Error finding leader in broker {b}: {e}")
            continue
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
        save_offset(topic, cur_offset)
        pulled += 1
        cur_offset += len(p) + 1  # account for terminating newline byte

    return {"pulled": pulled}


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
    uvicorn.run(app, host="172.24.165.219", port=CONSUMER_PORT)
