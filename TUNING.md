# Streaming Resource Tuning — Rationale

The consumer runs on a single machine with **16 GB RAM**, consuming a GTFS-RT
feed that a separate **2 GB VM** polls into Kafka (Redpanda) every 15–30 seconds
at **a few hundred records per poll**. The job is **stateless** — it parses,
classifies (valid vs. quarantine), and writes Parquet to S3 with S3-backed
checkpointing. There is no aggregation in Spark; all analysis is SQL-at-query-time
in Snowflake (Kappa pattern).

The single most important sizing fact: **at a few hundred records/poll, the data
volume is tiny.** With one-day Kafka retention, the worst-case *full* backlog is
~1.7M records (≈300 × ≈5,760 polls/day), which is small relative to 16 GB. So the
job is **I/O-bound, not memory-bound**, and tuning targets file count and recovery
behavior — not memory pressure or throughput.

**1. Small-file overhead (the steady-state cost).**
A 15–30s poll into a per-poll micro-batch produces thousands of sub-megabyte
Parquet files per day. The cost is per-file, not per-byte: each file is an S3
PUT and a unit of work for the downstream `COPY INTO`. A **2-minute trigger**
(`processingTime="2 minutes"`) bundles 4–8 polls per write, and **`coalesce(1)`**
in each `foreachBatch` forces one file per topic per trigger. Net: ~4–5× fewer
files with zero analytical loss, since no metric needs sub-2-minute resolution.

**2. Recovery throttle (the rare cost).**
`maxOffsetsPerTrigger=50000` bounds records per micro-batch. Steady state is
~2.4–4.8k records/batch, so 50k is ~10–20× headroom. After downtime the trigger
fires continuously while lag exists, so a full-day backlog drains in minutes over
back-to-back batches. **Drain speed comes from batch repetition, not batch size** —
raising the cap further buys nothing because at this volume the binding constraint
is S3 write + COPY cadence, not intake. Hence 50k, not 200k+.

**3. Box-fit settings (headroom, not horsepower).**
`local[4]` (not `local[*]`) gives catch-up batches some write parallelism while
leaving cores for the OS and Kafka client. `driver-memory 4g` is comfortable
headroom on 16 GB — **not a throughput gain**; a stateless job processing a few
hundred rows/batch never approaches that heap, and a larger allocation would sit
idle. `shuffle.partitions=4` / `default.parallelism=4` avoid spawning 200 tasks
over a few thousand rows on the rare incidental shuffle. `failOnDataLoss=false`
tolerates Redpanda retention trimming offsets the consumer hasn't reached.

**Why Spark at all for stateless ingestion?** A plain Kafka consumer would use
fewer resources. Spark earns its place via exactly-once checkpointing, the S3
commit protocol's partial-write recovery, and schema enforcement on the
protobuf-derived rows — and it is the same engine I'd keep if the transforms
ever become stateful, so the architecture scales without a rewrite.
