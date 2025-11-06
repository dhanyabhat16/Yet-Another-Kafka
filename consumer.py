# consumer.py
import os
import time
import json
import httpx
from fastapi import FastAPI, Request
from typing import Dict
import threading
from python_dotenv import load_dotenv

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
    return os.path.join(OFFSET_DIR, f"offset_{topic}.txt")


def load_offsets() -> Dict[str, int]:
    out = {}
    for t in TOPICS:
        fn = offset_file(t)
        try:
            with open(fn, "r") as f:
                out[t] = int(f.read().strip())
        except Exception as e:
            print(f"Error loading offset for topic {t}: {e}")
            out[t] = -1
    return out


def save_offset(topic, offset):
    with open(offset_file(topic), "w") as f:
        f.write(str(offset))


def find_leader():
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
def offsets():
    return load_offsets()


@app.get("/pull")
def pull(topic: str, from_offset: int, to_offset: int):
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
def health():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    print("Starting consumer, topics:", TOPICS)
    uvicorn.run(app, host="0.0.0.0", port=CONSUMER_PORT)
