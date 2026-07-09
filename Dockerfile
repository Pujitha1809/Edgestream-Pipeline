# EdgeStream Pipeline — single-container deployment
# Builds the C++ embedded-layer simulator, then runs the FastAPI
# ingestion service (which spawns the simulator and also serves the
# dashboard), all as one deployable unit.

FROM python:3.12-slim

# Build tools for the C++ simulator
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Embedded layer: compile the simulator ---
COPY embedded/sensor_simulator.cpp embedded/sensor_simulator.cpp
RUN g++ -O2 -std=c++17 embedded/sensor_simulator.cpp -o embedded/sensor_simulator

# --- Ingestion layer: install Python deps ---
COPY ingestion/requirements.txt ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt

COPY ingestion/ingestion_service.py ingestion/ingestion_service.py

# --- Dashboard: served statically by the same FastAPI app ---
COPY dashboard/index.html dashboard/index.html

EXPOSE 8000

CMD ["python3", "ingestion/ingestion_service.py"]
