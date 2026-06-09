#!/usr/bin/env bash
# submit_stream.sh
# Spark Structured Streaming consumer for the MTA GTFS-RT pipeline.
# Target: local machine, 16 GB RAM, consuming a feed polled every 15-30s.
# Producer is a separate 2 GB VM (irrelevant to consumer memory sizing).
# Job profile: STATELESS ingestion (no groupBy/window) -> Parquet on S3, checkpointed.
#
# Tuning philosophy: at a few hundred records/poll the data volume is tiny and the
# job is I/O-bound, not memory-bound. The enemy is therefore (a) small files, not
# throughput, and (b) per-batch overhead. Memory is NOT the constraint at this
# scale on 16GB; values below favor file-count control and safe recovery over a
# memory headroom we don't need.

set -euo pipefail

spark-submit \
  `# --- Master: local[4], NOT local[*]. Leave cores for the OS + Kafka client.` \
  `# A stateless writer is I/O-bound; 4 threads give catch-up batches some write` \
  `# parallelism without the contention local[*] causes on a shared desktop.` \
  --master "local[*]" \
  \
  `# --- Driver memory: in local mode the driver IS the executor.` \
  `# 4g on a 16GB box is comfortable HEADROOM, not a throughput gain -- a stateless` \
  `# job processing a few hundred rows/batch is I/O-bound and never memory-bound.` \
  `# Larger heaps (6-8g) would sit idle; 4g covers catch-up batches + S3 buffers.` \
  --driver-memory 6g \
  \
  `# --- Shuffle partitions: default is 200. We have no shuffle (stateless), but` \
  `# if any incidental shuffle appears, 200 tasks over a few thousand rows is pure` \
  `# scheduling overhead. Match to local thread count (4).` \
  --conf spark.sql.shuffle.partitions=16 \
  \
  `# --- Default parallelism likewise matched to the 4 local threads.` \
  --conf spark.default.parallelism=16 \
  \
  `# --- Adaptive Query Execution on: lets Spark coalesce post-shuffle partitions` \
  `# automatically if a shuffle ever does occur. Cheap insurance.` \
  --conf spark.sql.adaptive.enabled=true \
  \
  `# --- Kafka package (match to your Spark version).` \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  \
  mta_stream.py
