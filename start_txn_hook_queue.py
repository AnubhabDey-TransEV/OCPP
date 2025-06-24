import asyncio
import httpx
import json
import os
from datetime import datetime, timedelta
from typing import Dict
from decouple import config

# === FILE PATHS ===
LIVE_FILE = "start_txn_live_queue.jsonl"
FAILED_FILE = "start_txn_failed.jsonl"
FATAL_FILE = "start_txn_fatal_queue.jsonl"

# === CONFIG ===
RETRY_BASE_INTERVAL = 30  # seconds
MAX_RETRIES = 6
HOOK_URL = "https://be.ocpp.cms.transev.site/users/checkstartresponse"
apiauthkey = config("APIAUTHKEY")

# === STATE ===
_worker_task = None
_shutdown_event = asyncio.Event()

# === IN-MEMORY STORAGE (this is your real-time energy limit store) ===
# Maps transaction_id → max_kwh (float)
max_energy_limits = {}  # <- This is what you read from ocpprequests.py

# === FILE I/O ===
def append_jsonl(path: str, obj: Dict):
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")

def read_jsonl(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]

def write_jsonl(path: str, objects: list):
    with open(path, "w") as f:
        for obj in objects:
            f.write(json.dumps(obj) + "\n")

# === INTERFACE ===
def add_to_start_hook_queue(payload: Dict):
    task = {
        "payload": payload,
        "retries": 0,
        "next_retry": datetime.now().isoformat()
    }
    current = read_jsonl(LIVE_FILE)
    if any(t["payload"]["transactionid"] == payload["transactionid"] for t in current):
        return
    append_jsonl(LIVE_FILE, task)
    print(f"[HOOK QUEUE 📥] Queued StartTransaction hook for TX {payload['transactionid']}")

def log_fatal(task: Dict, reason: str):
    task["fatal_reason"] = reason
    task["logged_at"] = datetime.now().isoformat()
    append_jsonl(FATAL_FILE, task)
    print(f"[☠️] Fatal hook task TX {task.get('payload', {}).get('transactionid', 'unknown')} – {reason}")

# === RETRY POST ===
async def try_post_hook(payload: Dict):
    try:
        headers = {"apiauthkey": apiauthkey}
        async with httpx.AsyncClient() as client:
            resp = await client.post(HOOK_URL, json=payload, headers=headers, timeout=10)

        if resp.status_code >= 500:
            raise Exception(f"Server error: {resp.status_code}")
        elif resp.status_code >= 400:
            log_fatal({"payload": payload}, f"HTTP {resp.status_code}: {resp.text}")
            return "fatal"

        data = resp.json()
        if "max_kwh" not in data:
            log_fatal({"payload": payload}, f"No max_kwh in response: {data}")
            return "fatal"

        max_energy_limits[payload["transactionid"]] = float(data["max_kwh"])
        print(f"[✅] Hook acknowledged for TX {payload['transactionid']} — max_kwh={data['max_kwh']}")
        return True

    except Exception as e:
        print(f"[❌] Retryable failure for TX {payload['transactionid']}: {e}")
        return False

# === MAIN LOOP ===
async def process_live_queue():
    while not _shutdown_event.is_set():
        live = read_jsonl(LIVE_FILE)
        now = datetime.now()
        next_queue = []

        for entry in live:
            retry_time = datetime.fromisoformat(entry["next_retry"])
            if now >= retry_time:
                result = await try_post_hook(entry["payload"])
                if result == True:
                    continue
                elif result == "fatal":
                    continue
                else:
                    entry["retries"] += 1
                    if entry["retries"] >= MAX_RETRIES:
                        append_jsonl(FAILED_FILE, entry)
                        print(f"[💀] Max retries exceeded for TX {entry['payload']['transactionid']}")
                        continue
                    backoff = RETRY_BASE_INTERVAL * (2 ** (entry["retries"] - 1))
                    entry["next_retry"] = (now + timedelta(seconds=backoff)).isoformat()
            next_queue.append(entry)

        write_jsonl(LIVE_FILE, next_queue)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass

async def retry_failed_queue():
    while not _shutdown_event.is_set():
        failed = read_jsonl(FAILED_FILE)
        still_failed = []

        for entry in failed:
            result = await try_post_hook(entry["payload"])
            if result != True:
                still_failed.append(entry)

        write_jsonl(FAILED_FILE, still_failed)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass

# === BOOTSTRAP ===
async def _worker_loop():
    print("[HOOK QUEUE 🔁] Background hook worker running...")
    await asyncio.gather(
        process_live_queue(),
        retry_failed_queue()
    )

def start_hook_worker():
    global _worker_task
    if _worker_task is None:
        _worker_task = asyncio.create_task(_worker_loop())
        print("[HOOK QUEUE 🚀] Hook worker launched.")

async def shutdown_hook_worker():
    print("[HOOK QUEUE 🛑] Shutting down hook worker...")
    _shutdown_event.set()
    if _worker_task:
        await _worker_task
    print("[HOOK QUEUE ✅] Clean shutdown complete.")
