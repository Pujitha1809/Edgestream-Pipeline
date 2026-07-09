# EdgeStream Pipeline

A real-time sensor data platform spanning embedded device simulation, stream
ingestion with validation, and live visualization — architected as three
independently runnable layers, the way a production IoT system would be
split across firmware, backend, and frontend teams.

```
┌─────────────────────┐      stdout / JSON lines      ┌──────────────────────┐      WebSocket      ┌────────────────────┐
│   Embedded Layer     │ ─────────────────────────────▶│   Ingestion Layer     │ ────────────────────▶│  Visualization      │
│   (C++)              │      (serial/UART stand-in)    │   (Python / FastAPI)  │      REST + WS        │  Layer (HTML/JS)    │
│   sensor_simulator    │                                │   validate → SQLite   │                        │  live dashboard     │
└─────────────────────┘                                └──────────────────────┘                        └────────────────────┘
```

## What this is (and isn't)

This project **simulates** the embedded/device layer in C++ rather than
running on physical hardware — that's a deliberate, disclosed choice, not
a shortcut being hidden. The simulator is written to mirror how firmware
on an ESP32 + DHT22 sensor would actually behave: fixed-interval polling,
bounded random-walk drift (not just noise), and injected transient sensor
faults. The comments in `sensor_simulator.cpp` show exactly how each part
maps onto real Arduino-framework code.

Everything downstream of the device layer — the ingestion service,
validation logic, SQLite persistence, REST API, and WebSocket live feed —
is real, fully working code, not a mockup. It was built and tested
end-to-end while developing this project.

## Why this exists

Built to extend prior real-time data pipeline work (validated, distributed
sensor ingestion from a multi-node IoT deployment) down into the device
layer, closing the gap between "consumes sensor data" and "owns the full
pipeline from sensor to dashboard."

## Architecture

### 1. Embedded layer — `embedded/sensor_simulator.cpp`
- Simulates N sensor nodes producing temperature/humidity readings
- Realistic drift model (each reading is a bounded random walk from the
  last, not independent noise) — mirrors how physical sensors behave
- ~0.5% random fault injection per reading, to exercise the validation
  layer downstream (real sensors fail intermittently; a robust pipeline
  has to handle that, not assume clean data)
- Outputs newline-delimited JSON to stdout at a fixed interval — this is
  the same shape of interface a serial port reader or MQTT subscriber
  would consume from real hardware

### 2. Ingestion layer — `ingestion/ingestion_service.py`
- FastAPI service that spawns the simulator as a subprocess and reads its
  stdout stream line by line
- **Validation**: rejects malformed JSON, out-of-range physical values,
  and fault-flagged readings before they reach storage — the same
  "SQL-based quality checks" pattern used for validating real-time
  sensor data, applied to a live stream instead of a batch file
- **Persistence**: valid readings and rejected readings are both logged to
  SQLite (`readings` and `rejected_readings` tables) for history and
  auditability
- **Live distribution**: every accepted/rejected reading is broadcast
  over WebSocket to connected clients in real time
- REST endpoints: `/health`, `/stats`, `/history?limit=N`

### 3. Visualization layer — `dashboard/index.html`
- Connects to the ingestion service's WebSocket feed
- Live-updating per-sensor cards and time-series charts (Chart.js) for
  temperature and humidity
- Rolling event log showing accepted vs. rejected readings in real time,
  so the validation logic is visibly doing its job, not just running
  silently

## Running it

**Requirements:** `g++` (C++17), Python 3.10+

```bash
# 1. Build the embedded-layer simulator
cd embedded
g++ -O2 -std=c++17 sensor_simulator.cpp -o sensor_simulator
cd ..

# 2. Install and run the ingestion service (spawns the simulator itself)
cd ingestion
pip install fastapi uvicorn --break-system-packages   # omit the flag if not needed on your system
python3 ingestion_service.py
```

The service starts on `http://localhost:8000` and immediately begins
consuming the simulated sensor stream.

```bash
# 3. Open the dashboard
# In a separate terminal/browser tab:
open ../dashboard/index.html      # macOS
# or just double-click the file / serve it with any static file server
```

You should see live-updating charts within a second or two, plus an event
log showing individual readings being accepted or occasionally rejected
(watch for the ~0.5% simulated fault rate).

### Quick API checks
```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
curl "http://localhost:8000/history?limit=10"
```

## Deploying (Render)

The service is packaged as a single Docker container (`Dockerfile`) — it
builds the C++ simulator, installs the Python dependencies, and serves
both the API and the dashboard from one FastAPI app on one port. This is
what makes it deployable on a platform like Render's free tier, which
needs a single long-running web service (not a static site — the
simulator subprocess and WebSocket connections need a persistent
process).

**Steps:**
1. Push this repo to GitHub (public or private).
2. Go to [render.com](https://render.com) → New → Blueprint, and point it
   at the repo. Render will detect `render.yaml` and configure everything
   automatically (Docker build, free plan, health check on `/health`).
   - Alternatively: New → Web Service → Docker → point at the repo, no
     blueprint needed.
3. Wait for the build (a couple of minutes — it's compiling C++ and
   installing Python deps in the container).
4. Once live, open the assigned `*.onrender.com` URL — the dashboard
   loads directly at `/`, no separate steps needed.

**Note on the free tier:** Render's free web services spin down after
15 minutes of inactivity and take 30-60 seconds to wake up on the next
request. If sharing this as a live demo link, mention that upfront so a
recruiter/interviewer doesn't think it's broken while it wakes up.

## Design notes / what I'd do differently at larger scale

- Swap the subprocess-stdout interface for a real serial port (`pyserial`)
  or MQTT broker when moving to physical hardware — the ingestion layer's
  interface to the device layer was deliberately kept simple (line-
  delimited JSON) so that swap is a small, isolated change
- SQLite is fine for a single-node demo; a multi-sensor production
  deployment would move to a time-series store (InfluxDB/TimescaleDB) for
  the write throughput and retention/downsampling that raw SQLite doesn't
  handle well
- The current setup runs one ingestion process per simulator instance;
  scaling to many physical devices would need a proper message broker
  (MQTT/Kafka) in front of the ingestion layer instead of a single
  subprocess pipe

## Stack

C++17 · Python · FastAPI · SQLite · WebSockets · Chart.js
