# piTemp2026 — Project Overview & Recommended Upgrades

## What This Application Does

piTemp2026 is a distributed IoT system for automated greenhouse climate control. It runs across multiple Raspberry Pis and a central server, using Apache Kafka as a message bus to coordinate sensor readings, automation decisions, and hardware actuation.

### System Architecture

```
Sensor Pi (DHT22 + OpenWeather)
    └─► [greenhouse-readings] Kafka topic
            ├─► Decision Engine  → [greenhouse-commands] → Worker Pi (servos + TP-Link fan switch)
            │                                                    └─► [greenhouse-status]
            └─► Database Consumer → TimescaleDB
                                        └─► FastAPI server → Web Dashboard (Chart.js)
```

### Components

| Component | Location | Role |
|-----------|----------|------|
| **Sensor** (`sensor/kafka_producer.py`) | Pi Zero W (center of greenhouse) | Reads DHT22 every 5 min, fetches OpenWeather data, publishes to Kafka |
| **Worker** (`worker/greenhouse_worker.py`) | Pi near windows | Consumes commands, drives 4 servo motors (louver windows) + TP-Link Kasa smart switch (exhaust fan) |
| **Decision Engine** (`web/decision_engine.py`) | Server / offsite Pi | Consumes sensor readings, calculates optimal window position and fan state, publishes commands |
| **Broker / DB** (`greenhouse/`) | Server | Kafka 3.7.1 (arm64), TimescaleDB (PostgreSQL 16), FastAPI REST server |
| **Web Dashboard** (`web/`) | Browser | Dark-theme dashboard with Chart.js charts for temperature, humidity, and automation activity log |

### Decision Logic

The engine targets **75°F** with a ±3°F tolerance, using proportional window control and fan hysteresis to avoid rapid cycling:

- `temp ≥ 95°F` → windows 100% open (emergency)
- `temp ≤ 40°F` → windows 0% (emergency close)
- `temp > 78°F` → windows open proportionally; fan ON at 80°F, OFF at 76°F
- `humidity ≥ 80%` → windows forced to minimum 75%
- Night mode (10 PM – 6 AM) → window position reduced by 50%
- All thresholds are configurable via environment variables

---

## Top 3 Recommended Upgrades

### 1. Fix the Async Bug and Re-Enable Disabled Features

**Impact: High — core automation is currently non-functional**

Several critical pieces of automation are either broken or intentionally disabled and were never re-enabled:

**a) Worker async bug** (`worker/greenhouse_worker.py:146`):
```python
# BUG — asyncio.sleep in a synchronous context will fail at runtime
asyncio.sleep(0.5)

# Fix:
import time
time.sleep(0.5)
```

**b) Window servo control is commented out** (`web/decision_engine.py:190-197`):
The decision engine calculates a window position correctly but never sends the servo command. Uncomment the block that publishes to `greenhouse-commands` for window position.

**c) Humidity-based fan/window trigger (smarter logic required):**
The `HIGH_HUMIDITY` threshold is defined but the fan activation line is disabled. Rather than simply turning on the fan whenever inside humidity is high, the correct approach is to compare inside humidity against outside humidity (from the OpenWeather data already collected) and only vent if outside air is drier by a meaningful margin (default: 15%, configurable via `HUMIDITY_MARGIN`). Venting into equally or more humid outside air would make the problem worse.

This requires two changes:
1. `sensor/kafka_producer.py` — extract `data["main"]["humidity"]` from the OpenWeather response and add `web_humidity` to the `SensorReading` dataclass
2. `web/decision_engine.py` — replace the simple `humidity >= HIGH_HUMIDITY` check with `humidity >= HIGH_HUMIDITY and (humidity - web_humidity) >= HUMIDITY_MARGIN`

Both files contain `#V2` comments marking exactly where to make these changes.

These three fixes restore the intended behavior of the system with minimal code change.

---

### 2. Add the Missing Database Schema and State Persistence

**Impact: High — the database will not initialize without this, and automation state is lost on every restart**

**a) Create `greenhouse/init.sql`:**
The `docker-compose.yml` mounts this file at `/docker-entrypoint-initdb.d/init.sql` to initialize TimescaleDB on first run, but the file does not exist in the repo. Without it, none of the required tables (`sensor_readings`, `actuator_commands`, `actuator_status`, `alerts`) are created and the database consumer will crash immediately.

The schema should include:
- TimescaleDB hypertables for `sensor_readings` (time-series optimized)
- A continuous aggregate view for `hourly_stats` (already called by the API)
- A data retention policy (e.g., drop readings older than 1 year)

**b) Persist fan state and window position to the database:**
The decision engine tracks `current_fan_state` and `current_window_position` as in-memory variables only. On restart these reset to their defaults, which can cause the system to re-send unnecessary commands or miss the correct initial state. Persisting these to the database (or reading the latest `actuator_status` row on startup) would make the engine restart-safe.

---

### 3. Add API Authentication and an Alerting System

**Impact: Medium-High — the infrastructure is already in place but both features are incomplete**

**a) Secure the API:**
The FastAPI server currently has `allow_origins=["*"]` and no authentication. For any deployment accessible beyond localhost, add:
- An API key header check (simple and sufficient for a personal project)
- Restrict CORS origins to the specific dashboard hostname
- Use environment variables for secrets rather than `.env.example` defaults

**b) Implement the alerting pipeline:**
The `alerts` table and `/api/alerts` endpoint already exist in the database and API server. The missing piece is a publisher: add alert detection logic to the decision engine that publishes to the `greenhouse-alerts` Kafka topic when conditions exceed safe thresholds (e.g., temp > 95°F for more than 10 minutes, fan command acknowledged but temp still rising). 

Pair this with a notification step — a simple email via `smtplib` or a push notification via a free service like Pushover — and the system becomes actionable rather than just observable.
