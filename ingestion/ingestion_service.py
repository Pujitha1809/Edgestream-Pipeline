"""
ingestion_service.py
---------------------
Real-time ingestion layer for the sensor pipeline.

Responsibilities (mirrors the architecture of the NSF IoT project, but
this time owns the full stack down to the device layer):

  1. Spawns the compiled C++ sensor_simulator binary as a subprocess -
     this stands in for a serial port reader or MQTT subscriber that
     would sit in front of real ESP32 devices.
  2. Reads its stdout line-by-line (one JSON reading per line) and
     validates each reading against expected bounds/schema before
     accepting it - the same "SQL-based quality checks" pattern used
     in the original NSF air-quality pipeline, adapted to a streaming
     source instead of batch CSVs.
  3. Persists valid readings to SQLite for history/replay.
  4. Broadcasts every accepted reading to connected WebSocket clients
     in real time, and exposes a REST endpoint for historical queries.

Run:
    pip install fastapi uvicorn --break-system-packages
    python3 ingestion_service.py
Then open dashboard/index.html in a browser (or serve it) - it connects
to ws://localhost:8000/ws for the live feed.
"""

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent

DASHBOARD_DIR = BASE_DIR.parent / "dashboard"
DB_PATH = BASE_DIR / "readings.db"

VALID_TEMP_RANGE = (-40.0, 85.0)   # plausible DHT22-class sensor range
VALID_HUMIDITY_RANGE = (0.0, 100.0)

# ---------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------

def init_db():
    # check_same_thread=False: FastAPI runs sync endpoint functions in a
    # threadpool, so the connection must be safe to use across threads.
    # Writes from the async ingestion loop and reads from REST endpoints
    # are never concurrent in this single-worker setup, so this is safe.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            temperature_c REAL,
            humidity_pct REAL,
            ingested_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rejected_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_payload TEXT NOT NULL,
            reason TEXT NOT NULL,
            rejected_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def store_reading(conn, reading: dict):
    conn.execute(
        "INSERT INTO readings (sensor_id, timestamp, temperature_c, humidity_pct, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            reading["sensor_id"],
            reading["timestamp"],
            reading["temperature_c"],
            reading["humidity_pct"],
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def store_rejection(conn, raw_payload: str, reason: str):
    conn.execute(
        "INSERT INTO rejected_readings (raw_payload, reason, rejected_at) VALUES (?, ?, ?)",
        (raw_payload, reason, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


# ---------------------------------------------------------------------
# Validation layer - same spirit as the NSF pipeline's SQL-based
# quality checks: reject anything physically implausible or malformed
# before it ever reaches storage or downstream consumers.
# ---------------------------------------------------------------------

def validate_reading(payload: dict) -> tuple[bool, str]:
    required_fields = {"sensor_id", "timestamp", "temperature_c", "humidity_pct", "fault"}
    if not required_fields.issubset(payload.keys()):
        return False, "missing_required_fields"

    if payload.get("fault") is True:
        return False, "sensor_fault_flagged"

    t = payload["temperature_c"]
    h = payload["humidity_pct"]

    if not (VALID_TEMP_RANGE[0] <= t <= VALID_TEMP_RANGE[1]):
        return False, f"temperature_out_of_range:{t}"
    if not (VALID_HUMIDITY_RANGE[0] <= h <= VALID_HUMIDITY_RANGE[1]):
        return False, f"humidity_out_of_range:{h}"

    return True, "ok"


# ---------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
db_conn = None
simulator_process = None
reader_task = None


async def read_simulator_stream(app_state):
    """Background task: reads the simulator's stdout line by line and
    pushes valid readings through the pipeline. This is the async
    equivalent of a serial-port read loop on real hardware."""
    global simulator_process
    loop = asyncio.get_event_loop()

    while True:
        line = await loop.run_in_executor(None, simulator_process.stdout.readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            store_rejection(db_conn, line, "invalid_json")
            continue

        is_valid, reason = validate_reading(payload)
        if not is_valid:
            store_rejection(db_conn, line, reason)
            await manager.broadcast({"type": "rejected", "reason": reason, "raw": payload})
            continue

        store_reading(db_conn, payload)
        await manager.broadcast({"type": "reading", "data": payload})


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_conn, simulator_process, reader_task
    db_conn = init_db()

    if not SIMULATOR_PATH.exists():
        print(f"ERROR: simulator python script not found at {SIMULATOR_PATH}", file=sys.stderr)
    else:
        simulator_process = subprocess.Popen(
            [sys.executable, str(SIMULATOR_PATH), "3", "800"],  # 3 sensors, 800ms interval
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        reader_task = asyncio.create_task(read_simulator_stream(app))

    yield

    if simulator_process:
        simulator_process.terminate()
    if reader_task:
        reader_task.cancel()
    if db_conn:
        db_conn.close()


app = FastAPI(title="Real-Time Sensor Ingestion Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def serve_dashboard():
    index_path = DASHBOARD_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Dashboard not found; API is running at /health, /stats, /history, /ws"}


@app.get("/health")
def health():
    return {"status": "ok", "simulator_running": simulator_process is not None and simulator_process.poll() is None}


@app.get("/history")
def history(limit: int = 100):
    cur = db_conn.execute(
        "SELECT sensor_id, timestamp, temperature_c, humidity_pct, ingested_at "
        "FROM readings ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    return [
        {
            "sensor_id": r[0],
            "timestamp": r[1],
            "temperature_c": r[2],
            "humidity_pct": r[3],
            "ingested_at": r[4],
        }
        for r in rows
    ]


@app.get("/stats")
def stats():
    total = db_conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    rejected = db_conn.execute("SELECT COUNT(*) FROM rejected_readings").fetchone()[0]
    return {"accepted": total, "rejected": rejected}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
