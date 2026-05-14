# MTA Subway Reliability Tracker

Real-time streaming pipeline that computes per-line and per-station subway reliability metrics from MTA GTFS-RT feeds — giving journalists and commuters flight-delay-style stats for the NYC subway.

---

## The Problem

MTA publishes live feeds every 30 seconds, but there is no easy way to query *historical* reliability. There is no equivalent of flight-delay statistics for the subway.

## The Solution

A streaming pipeline that ingests GTFS-RT every 30s, computes windowed reliability metrics in Spark Structured Streaming, writes hot aggregates to Postgres for live serving, and archives raw events to S3 Parquet for analysis.

---

## How It Works

MTA publishes GTFS-Realtime feeds every 30 seconds — protobuf snapshots of every active train. The producer polls all 8 feed groups and publishes each stop time update as a JSON message to Redpanda (Kafka). Spark consumes those messages continuously and writes to two places at the same time from the same stream — the hot path and the cold path.

### Hot path vs Cold path

They are not a sequence. They are two simultaneous outputs of the same Spark job, reading from the same parsed events before and after aggregation:

```
Kafka message
     │
     ▼
parse_events()
     │
     ├──→ build_aggregates() → Postgres    (hot path)
     │
     └──→ raw events → S3 Parquet          (cold path)
```

**Hot path** — Spark computes a summary row every 5 minutes per route and writes it to Postgres. Detail is traded for speed. FastAPI and Grafana read from here.

```
window_start         route_id   on_time_pct   delay_p95
2026-05-13 09:00     4          0.87          162s
2026-05-13 09:00     A          0.73          310s
```

Answers: *Is the 4 train running on time right now? Which line has the worst delay today?*

**Cold path** — Spark writes every individual event to S3 Parquet, partitioned by date, with no aggregation. Nothing is thrown away. Airflow loads these files into Snowflake daily.

```
event_time            route_id   stop_id   delay_seconds
2026-05-13 09:00:14   4          640N      120
2026-05-13 09:00:15   4          631N      85
2026-05-13 09:00:18   A          A27N      400
```

Answers: *What is the average delay at Times Square on weekday mornings? Does the A train get worse after rain? Which stop causes the most cascading delays?*

The hot path gives you fast pre-computed answers for monitoring. The cold path keeps everything so you can ask questions you didn't think of when you built the pipeline.

### Why Airflow reads S3 and not Postgres

Airflow loads raw events from S3 into Snowflake — not the aggregated metrics from Postgres. Snowflake gets the original unaggregated data so you can recompute any metric, at any granularity, using SQL on months of history. If the 5-minute window in Spark turns out to be the wrong resolution, the raw data in Snowflake lets you recompute at 1-minute or 1-hour without re-running the pipeline.

---

## Architecture

```
MTA GTFS-RT feeds (HTTP/protobuf, every 30s)
        │
        ▼
gtfs_producer.py   →   JSON messages
        │
        ▼
Kafka (Redpanda)
  mta.trip_updates | mta.vehicle_positions | mta.alerts
        │
        ▼
Spark Structured Streaming
  watermark: 2 min  |  window: 5 min tumbling
  agg: on_time_pct, delay_p50, delay_p95 per line + station
        │
   ┌────┴────────────┐
   ▼ HOT             ▼ COLD
Postgres           S3 Parquet
reliability_metrics   raw/dt=YYYY-MM-DD/
   │
   ├── FastAPI  GET /reliability?line=4&window=1d
   └── Grafana  live dashboard

S3 Parquet  →  (Airflow, daily 3 AM)  →  Snowflake RAW.MTA_EVENTS
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Ingest | MTA GTFS-Realtime API, Python `gtfs-realtime-bindings` |
| Broker | Redpanda (Kafka-compatible, no ZooKeeper) |
| Stream processing | PySpark 3.5 Structured Streaming (`apache/spark:3.5.0`) |
| Hot store | Postgres 15 |
| Cold store | S3 Parquet (partitioned by `dt=YYYY-MM-DD`) |
| Long-term store | Snowflake |
| Serving | FastAPI + asyncpg |
| Dashboard | Grafana 10 (auto-provisioned over Postgres) |
| Orchestration | Airflow 2.8 (daily S3 → Snowflake compaction) |
| Container | Docker Compose |

---

## Project Layout

```
├── config/settings.py          # @dataclass config — all env-driven
├── src/
│   ├── ingestion/
│   │   ├── gtfs_producer.py    # Polls MTA feeds → Kafka JSON every 30s
│   │   └── static_schedule.py  # Downloads gtfs_static.zip → S3 CSV
│   ├── transform/
│   │   ├── schemas.py          # PySpark StructType per Kafka topic
│   │   ├── spark_streaming.py  # Main streaming job (hot + cold paths)
│   │   └── metrics.py          # Pure functions: on_time_pct, percentile
│   └── load/
│       ├── postgres_writer.py  # foreachBatch upsert → reliability_metrics
│       └── snowflake_loader.py # COPY INTO RAW.MTA_EVENTS from S3
├── api/
│   ├── main.py                 # FastAPI app + GET /reliability
│   ├── db.py                   # asyncpg pool + window param parsing
│   └── models.py               # Pydantic response models
├── dags/
│   └── dag_mta_s3_to_snowflake.py  # Daily compaction DAG
├── infra/
│   ├── postgres/init.sql       # DDL for reliability_metrics table
│   ├── grafana/provisioning/   # Auto-provisioned datasource + dashboard
│   └── docker/                 # Dockerfiles for producer + API
└── tests/                      # pytest suite (metrics, schemas, API, writer)
```

---

## Deployment Strategy

Running every container on a single laptop is hardware-intensive and requires the machine to stay on 24/7 in order to capture all the data from the API. The recommended approach splits the workload across a free cloud VM and your laptop.

### Recommended: Hetzner VM + Laptop split

A Hetzner CPX11 runs the broker and producer 24/7 at ~$7/month. Your laptop connects to it when you are ready to process the accumulated data.

```
Hetzner VM (~$7/month, always on)   Laptop (run weekly or on demand)
─────────────────────────────────   ──────────────────────────────────
redpanda                            spark-master + spark-worker
gtfs-producer                       postgres
redpanda-console                    fastapi + grafana
                                    airflow (occasional)
```

**How it works:**
- The VM runs continuously, polling MTA feeds every 30s and accumulating messages in Redpanda (up to 10GB / ~7 days retention)
- Redpanda acts as a durable buffer — messages are not lost when Spark is off
- When you're ready to process, start Spark on your laptop pointed at the VM's Redpanda
- Spark reads the entire backlog at ~1000× the producer rate — a week of data clears in ~10 minutes
- Results land in Postgres (hot) and S3 (cold) as normal

**Cost:** ~$7/month for a Hetzner CPX11 (2 vCPU, 2GB RAM).

#### Hetzner VM resource allocation

| Resource | Spec | Notes |
|---|---|---|
| Plan | CPX11 | 2 vCPU, 2GB RAM, 40GB disk |
| Image | Ubuntu 22.04 | Standard — no minimal variant needed |
| Location | US-East (Ashburn) | Closest to MTA API servers in NYC |

**Memory layout on 2GB RAM:**

```
Ubuntu OS:           ~250MB
Docker daemon:       ~100MB
gtfs-producer:       ~150MB
Redpanda:            ~500MB
─────────────────────────────
Total:               ~1000MB  (~1GB headroom)
```

See **Approach 1** in the Quick Start below for the full step-by-step Hetzner VM setup.

### Alternative: All local (optional)

Run everything on one machine. Requires 8GB+ RAM, machine must stay on 24/7. Steps are the same — just run all `docker compose up` commands on the same host.

---

## Quick Start

### Prerequisites (both approaches)
- Docker + Docker Compose
- Python 3.11+
- AWS credentials with S3 read/write access
- Snowflake account (for cold-path compaction)

---

## Approach 1 — Hetzner VM + Laptop (recommended)

The Hetzner VM runs the broker and producer 24/7. Your laptop connects to it when you are ready to process the accumulated data.

---

### VM — Step 1: Generate an SSH key on your laptop

Skip if you already have `~/.ssh/id_ed25519.pub`.

```bash
# Mac / Linux / Windows Git Bash
ssh-keygen -t ed25519 -C "your-email"
# Press Enter to accept all defaults

cat ~/.ssh/id_ed25519.pub   # copy this — paste it into Hetzner
```

---

### VM — Step 2: Create the Hetzner server

1. Go to [hetzner.com/cloud](https://www.hetzner.com/cloud) → **Sign up** → create a new project
2. Click **Add Server** and configure:
   - **Location:** US-East (Ashburn)
   - **Image:** Ubuntu 22.04
   - **Type:** Shared vCPU → **CPX11** (2 vCPU, 2GB RAM)
   - **Networking:** leave defaults (public IPv4 enabled)
   - **Firewall:** skip — Hetzner has no default port blocking
   - **SSH Keys:** click **Add SSH key** → paste output of `cat ~/.ssh/id_ed25519.pub` → name it `mta-tracker`
   - **Volumes:** skip
   - **Backups:** skip
   - **Name:** `mta-tracker`
3. Click **Create & Buy Now** — server is ready in ~30 seconds

The public IP is shown immediately in the Hetzner dashboard.

---

### VM — Step 3: SSH into the server

```bash
ssh root@<hetzner-ip>
# Type yes to confirm host fingerprint on first connection
```

Hetzner uses `root` by default — no `ubuntu` prefix needed.

---

### VM — Step 4: Install Docker

```bash
sudo apt remove docker docker.io containerd runc -y
sudo apt update
sudo apt install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Verify
docker --version
docker compose version
```

No `usermod` needed — you are already root on Hetzner.

---

### VM — Step 5: Add VM SSH key to GitHub

This lets the VM clone your repo.

```bash
cat ~/.ssh/id_ed25519.pub   # copy the full output
```

Go to **GitHub → Settings → SSH and GPG keys → New SSH key**:
- Title: `hetzner-vm`
- Paste the output → **Add SSH key**

Test:
```bash
ssh -T git@github.com
# Expect: Hi your-username! You've successfully authenticated...
```

---

### VM — Step 6: Clone the repo

```bash
git clone git@github.com:your-username/MTA-subway-reliability-tracker.git
cd MTA-subway-reliability-tracker
cp .env.example .env
# No edits needed — MTA_FEED_URLS and KAFKA_BOOTSTRAP_SERVERS are pre-filled correctly
```

---

### VM — Step 7: Start the broker and producer

```bash
docker compose up -d redpanda redpanda-console gtfs-producer
```

---

### VM — Step 8: Create topics and set retention

```bash
# Create topics
docker exec -it redpanda rpk topic create \
  mta.trip_updates mta.vehicle_positions mta.alerts

# Set retention per topic — allocated by actual data volume (10GB total, 7 days each)
# trip_updates produces ~95% of all data, so it gets the largest allocation
docker exec -it redpanda rpk topic alter-config mta.trip_updates \
  --set retention.bytes=8589934592 \
  --set retention.ms=604800000

docker exec -it redpanda rpk topic alter-config mta.vehicle_positions \
  --set retention.bytes=1610612736 \
  --set retention.ms=604800000

docker exec -it redpanda rpk topic alter-config mta.alerts \
  --set retention.bytes=536870912 \
  --set retention.ms=604800000

# Verify retention applied
docker exec -it redpanda rpk topic describe mta.trip_updates
# Expect: retention.bytes=8589934592, retention.ms=604800000
```

Retention is set at the topic level via `rpk` — not as a Redpanda server flag.

---

### VM — Step 9: Verify producer is running

```bash
docker logs -f gtfs-producer
# Expect: "feed=gtfs trip_updates=142 vehicle_positions=89" every 30s
# Ctrl+C to stop following logs — producer keeps running in background
```

The VM is now collecting data continuously. Messages accumulate for up to 7 days before the oldest are rolled off. Leave it running.

**Health check — verify everything is working:**

```bash
# All 3 containers should show "Up"
docker compose ps

# Redpanda cluster is healthy
docker exec -it redpanda rpk cluster info

# Topics exist and have messages
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic describe mta.trip_updates

# Producer is polling feeds
docker logs --tail 20 gtfs-producer
```

**Redpanda Console — monitor topics remotely:**

Try opening `http://<hetzner-ip>:8082` in your browser. Hetzner has no default port blocking so this should work directly.

If your ISP blocks port 8082, use SSH port forwarding — open a new terminal on your laptop and keep it running:

```bash
ssh -L 8082:localhost:8082 root@<hetzner-ip> -N
```

Then open `http://localhost:8082` → Topics tab. Close the terminal when done.

---

### Laptop — Step 10: Set up Python environment

```bash
python -m venv .venv

# Mac / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

---

### Laptop — Step 11: Configure environment

```bash
cp .env.example .env
```

Open `.env` and fill in each section. Most values are pre-filled — only the ones marked **fill in** require your input.

#### MTA feeds

```
MTA_FEED_URLS=https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs,...
MTA_STATIC_GTFS_URL=http://web.mta.info/developers/data/nyct/subway/google_transit.zip
```

**`MTA_FEED_URLS`** — pre-filled. Leave as-is.

**`MTA_STATIC_GTFS_URL`** — pre-filled. Only change if MTA moves the file.

#### Kafka (Redpanda) — point to Hetzner VM

```
KAFKA_BOOTSTRAP_SERVERS=<hetzner-ip>:19092   ← fill in with your Hetzner server IP
```

Find your server IP in the Hetzner dashboard → your project → server list. Port `19092` is Redpanda's external listener. Port `9092` is internal (Docker-only) — do not use it here.

```
KAFKA_TOPIC_TRIP_UPDATES=mta.trip_updates
KAFKA_TOPIC_VEHICLE_POSITIONS=mta.vehicle_positions
KAFKA_TOPIC_ALERTS=mta.alerts
```

**`KAFKA_TOPIC_*`** — pre-filled. Leave as-is.

#### Postgres

```
POSTGRES_MTA_DSN=postgresql://mta:mta@localhost:5432/mta
```

**`POSTGRES_MTA_DSN`** — pre-filled. Created automatically by `infra/postgres/init.sql` on first container start.

#### AWS / S3

```
AWS_ACCESS_KEY_ID=        ← fill in
AWS_SECRET_ACCESS_KEY=    ← fill in
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=                ← fill in
SPARK_CHECKPOINT_BASE=    ← fill in (derived from S3_BUCKET)
```

**`AWS_ACCESS_KEY_ID`** and **`AWS_SECRET_ACCESS_KEY`**:
1. Log into [AWS Console](https://console.aws.amazon.com) → **IAM → Users → Add users**
2. Attach policy `AmazonS3FullAccess`
3. Go to user → **Security credentials → Create access key**
4. Choose **Application running outside AWS**, copy key and secret here

**`S3_BUCKET`** — create a bucket in **AWS Console → S3 → Create bucket**. Name must be globally unique (e.g. `mta-tracker-yourname-2026`).

**`SPARK_CHECKPOINT_BASE`** — must use `s3a://` (not `s3://`):
```
S3_BUCKET=mta-reliability-tracker
SPARK_CHECKPOINT_BASE=s3a://mta-reliability-tracker/checkpoints/mta
```

#### Snowflake

```
SNOWFLAKE_ACCOUNT=     ← fill in
SNOWFLAKE_USER=        ← fill in
SNOWFLAKE_PASSWORD=    ← fill in
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MTA
SNOWFLAKE_ROLE=MTA_TRANSFORMER
```

Only needed for the Airflow DAG. If skipping Snowflake, leave blank.

**`SNOWFLAKE_ACCOUNT`** — account identifier from the bottom-left of the Snowflake UI. Looks like `xy12345.us-east-1`.

**`SNOWFLAKE_USER`** / **`SNOWFLAKE_PASSWORD`** — service account created by the setup queries below.

##### Snowflake setup queries

Run once in a Snowflake worksheet. Replace `your-bucket-name`, `YOUR_AWS_ACCESS_KEY_ID`, `YOUR_AWS_SECRET_ACCESS_KEY`, and `your-password` with actual values.

```sql
CREATE DATABASE IF NOT EXISTS MTA;
CREATE SCHEMA IF NOT EXISTS MTA.RAW;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS MTA_TRANSFORMER;
GRANT USAGE  ON WAREHOUSE COMPUTE_WH TO ROLE MTA_TRANSFORMER;
GRANT USAGE  ON DATABASE  MTA        TO ROLE MTA_TRANSFORMER;
GRANT USAGE  ON SCHEMA    MTA.RAW    TO ROLE MTA_TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA MTA.RAW TO ROLE MTA_TRANSFORMER;
GRANT INSERT, SELECT ON ALL TABLES    IN SCHEMA MTA.RAW TO ROLE MTA_TRANSFORMER;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA MTA.RAW TO ROLE MTA_TRANSFORMER;

CREATE STAGE IF NOT EXISTS MTA.RAW.S3_STAGE
    URL = 's3://your-bucket-name/raw/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID' AWS_SECRET_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY')
    FILE_FORMAT = (TYPE = PARQUET);

CREATE USER IF NOT EXISTS mta_svc
    PASSWORD = 'your-password' DEFAULT_ROLE = MTA_TRANSFORMER DEFAULT_WAREHOUSE = COMPUTE_WH MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE MTA_TRANSFORMER TO USER mta_svc;

SHOW GRANTS TO ROLE MTA_TRANSFORMER;
SHOW STAGES IN SCHEMA MTA.RAW;
```

#### Airflow internals

```
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__FERNET_KEY=    ← generate this
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
```

**`AIRFLOW_UID`** — on Linux change to your UID (`id -u`) to avoid volume permission issues.

**`AIRFLOW__CORE__FERNET_KEY`** ⚠️ — generate with venv active:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

### Laptop — Step 12: Load static schedule (one-time)

Required before Spark can compute `on_time_pct`. Re-run weekly.

```bash
python -m src.ingestion.static_schedule
# Expected:
  2026-05-13 03:53:02,222 INFO __main__ Downloading static GTFS ...
  2026-05-13 03:53:03,376 INFO __main__ Flat schedule: 562755 rows, 29 unique routes
  2026-05-13 03:53:21,829 INFO __main__ Wrote 562755 rows to s3://...
```

---

### Laptop — Step 13: Run the Spark streaming job

Build the Spark image once (bakes in all JARs and Python packages):

```bash
docker compose build spark-master spark-worker
docker compose up -d spark-master postgres
```

Run the streaming job (stays attached — dedicate a terminal tab):

```bash
# Windows (Git Bash)
MSYS_NO_PATHCONV=1 docker exec spark-master //opt/spark/bin/spark-submit \
  //opt/spark/work/src/transform/spark_streaming.py

# Mac / Linux
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/spark/work/src/transform/spark_streaming.py
```

Spark drains the entire VM backlog immediately — a week of data clears in ~10 minutes. Expected output:

```
INFO src.load.postgres_writer batch_id=11 upserted 1328 rows to reliability_metrics
```

**Verify:**

1. **Spark Master UI** — `http://localhost:8081` → Running Applications → `mta-reliability-streaming`
2. **Redpanda Console** — `http://<hetzner-ip>:8082` → Topics → `mta.trip_updates` → Consumers tab — lag decreasing rapidly
3. **Postgres** — after ~10 minutes:

```bash
docker exec -it postgres psql -U mta -d mta -c "SELECT COUNT(*) FROM reliability_metrics;"
# Expect: count > 0
```

---

### Laptop — Step 14: Start the API and dashboard

Open a new terminal tab — spark-submit stays attached in its own tab.

```bash
docker compose up -d fastapi grafana

# API
curl "http://localhost:8000/reliability?line=4&window=1d"
curl "http://localhost:8000/reliability?line=A&station=A27N&window=6h"

# Grafana — open http://localhost:3000  (admin / admin)
# Dashboards → MTA Subway Reliability
```

---

### Laptop — Step 15: Airflow cold-path compaction (occasional)

Only needed to load S3 Parquet into Snowflake. Start it, run the DAG, then stop it.

```bash
docker compose up -d airflow-init airflow-webserver airflow-scheduler
# Open http://localhost:8080  (admin / admin)
# Unpause dag_mta_s3_to_snowflake
```

**Backfill missed days** — one trigger per day. `COPY INTO` is idempotent so re-running the same date is safe:

```bash
docker exec airflow-scheduler airflow dags trigger dag_mta_s3_to_snowflake \
  --exec-date 2026-05-10T03:00:00+00:00

docker exec airflow-scheduler airflow dags trigger dag_mta_s3_to_snowflake \
  --exec-date 2026-05-11T03:00:00+00:00
```

When done:

```bash
docker compose stop airflow-webserver airflow-scheduler
```

---

## Approach 2 — All local (optional)

Run every container on one machine. Requires 8GB+ RAM and the machine must stay on 24/7 to collect data continuously.

---

### Step 1: Set up Python environment

```bash
python -m venv .venv

# Mac / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

---

### Step 2: Configure environment

```bash
cp .env.example .env
```

Fill in all sections using the same variable descriptions from Approach 1 above. The only difference: `KAFKA_BOOTSTRAP_SERVERS` stays as `localhost:9092` — no VM IP needed.

---

### Step 3: Start core infrastructure

```bash
docker compose up -d redpanda postgres

# Create Kafka topics
docker exec -it redpanda rpk topic create \
  mta.trip_updates mta.vehicle_positions mta.alerts

# Verify topics — open http://localhost:8082 → Topics tab (3 topics)
```

---

### Step 4: Load static schedule (one-time)

```bash
python -m src.ingestion.static_schedule
```

---

### Step 5: Start the producer

```bash
docker compose up -d gtfs-producer

# Tab 1 — watch producer logs
docker logs -f gtfs-producer
# Expect: "feed=gtfs trip_updates=142 vehicle_positions=89" every 30s
```

---

### Step 6: Run the Spark streaming job

```bash
docker compose build spark-master spark-worker
docker compose up -d spark-master

# Tab 2 — stays attached
# Windows (Git Bash)
MSYS_NO_PATHCONV=1 docker exec spark-master //opt/spark/bin/spark-submit \
  //opt/spark/work/src/transform/spark_streaming.py

# Mac / Linux
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/spark/work/src/transform/spark_streaming.py
```

After ~10 minutes verify:

```bash
docker exec -it postgres psql -U mta -d mta -c "SELECT COUNT(*) FROM reliability_metrics;"
# Expect: count > 0
```

---

### Step 7: Start the API and dashboard

```bash
# Tab 3
docker compose up -d fastapi grafana

curl "http://localhost:8000/reliability?line=4&window=1d"
# Grafana — http://localhost:3000  (admin / admin) → Dashboards → MTA Subway Reliability
```

---

### Step 8: Airflow cold-path compaction (occasional)

```bash
docker compose up -d airflow-init airflow-webserver airflow-scheduler
# http://localhost:8080  (admin / admin)
# Unpause dag_mta_s3_to_snowflake

docker compose stop airflow-webserver airflow-scheduler  # when done
```

---

## API Reference

### `GET /reliability`

Query windowed reliability metrics from Postgres.

| Param | Type | Required | Description |
|---|---|---|---|
| `line` | string | ✅ | Route ID — e.g. `4`, `A`, `L` |
| `window` | string | ✅ | Lookback: `1h`, `6h`, `12h`, `1d`, `7d` |
| `station` | string | ❌ | Optional `stop_id` filter |

**Example response:**

```json
{
  "line": "4",
  "window": "1d",
  "records": [
    {
      "window_start": "2026-04-28T12:00:00Z",
      "window_end": "2026-04-28T12:05:00Z",
      "route_id": "4",
      "stop_id": null,
      "on_time_pct": 0.87,
      "delay_p50_seconds": 28.0,
      "delay_p95_seconds": 162.5,
      "sample_count": 214
    }
  ]
}
```

---

## Service Ports

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8082 |
| Spark Master UI | http://localhost:8081 |
| FastAPI | http://localhost:8000 |
| FastAPI docs | http://localhost:8000/docs |
| Airflow | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Postgres | localhost:5432 |

---

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Kafka serialization | JSON | No schema registry needed; native `from_json()` in Spark |
| Kafka broker | Redpanda | Kafka-compatible, no ZooKeeper, single Docker image |
| Static schedule join | Broadcast CSV at Spark job start | Simple; ~500K rows fits in memory |
| Two streaming queries | Separate `checkpointLocation` each | Prevents checkpoint corruption on restart |
| Postgres upsert | `ON CONFLICT DO UPDATE` | Replay-safe — reprocessing a window never duplicates |
| Cold-path writer | Spark native `writeStream.format("parquet")` | pandas BytesIO pattern doesn't work in streaming |

---

## Running Tests

```bash
pytest tests/ -v
```

Tests cover: metric calculations, Spark schema validation, producer protobuf parsing, Postgres writer idempotency, FastAPI endpoint contracts.

---
