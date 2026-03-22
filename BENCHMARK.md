# Linear Road Benchmark Results

Local benchmark results on a single-node Kafka (KRaft) with Docker.
All results use **realtime feeding** — data is paced to match encoded event timestamps.

## Test Environment

| Component | Spec |
|---|---|
| Machine | Apple Silicon, 12 CPUs, 9 GB max heap |
| Kafka | Confluent CP 7.6.0 (single-node KRaft, Docker) |
| Java | OpenJDK 23.0.2 |
| Kafka Streams | 3.7.0 |
| Partitions | 12 (all source and sink topics) |
| Stream threads | 8 |
| Feeding mode | Realtime (paced to event timestamps) |
| Vehicle density | ~200 cars/lane/segment (3600 vehicles/xway, spec-compliant) |

## Results

### L=10 — Standard Mode (10 expressways, 3600 vehicles/xway, 10 min simulated)

| Stream | Records | p50 | p95 | p99 | max | Violations | Result |
|---|---|---|---|---|---|---|---|
| Toll notification | 255,604 | 91s | 151s | 153s | 155s | 255,604 | **FAIL** |
| Account balance | 90 | 4s | 5s | 8s | 8s | 3 | **FAIL** |
| Daily expenditure | 90 | 3s | 5s | 6s | 6s | 1 | **FAIL** |
| Accident notification | 245 | 1ms | 1ms | 1ms | 1ms | 0 | **PASS** |

| Metric | Value |
|---|---|
| Wall-clock duration | 616s (for 600s simulated) |
| Total records processed | 256,029 |
| Process throughput | 416 records/sec |
| Peak CPU | 46.9% |
| Peak heap | 2,569 MB |

### L=10 — Speculative Emit Mode (10 expressways, 3600 vehicles/xway, 10 min simulated)

With `linearroad.streaming.speculative_emit=true`, windowed aggregation results are flushed
every 2 seconds via wall-clock punctuation instead of waiting for minute boundaries.

| Stream | Records | p50 | p95 | p99 | max | Violations | Result |
|---|---|---|---|---|---|---|---|
| Toll notification | 2,763,232 | 92s | 152s | 154s | 156s | 2,763,232 | **FAIL** |
| Account balance | 90 | 4s | 7s | 10s | 10s | 23 | **FAIL** |
| Daily expenditure | 90 | 2s | 5s | 7s | 7s | 4 | **FAIL** |
| Accident notification | 10,952 | 1ms | 1ms | 1ms | 1ms | 0 | **PASS** |

| Metric | Value |
|---|---|
| Wall-clock duration | 621s (for 600s simulated) |
| Total records processed | 2,774,364 |
| Process throughput | 4,468 records/sec |
| Peak CPU | 58.2% |
| Peak heap | 3,713 MB |

### L=50 — Standard Mode (50 expressways, 3600 vehicles/xway, 10 min simulated)

| Stream | Records | p50 | p95 | p99 | max | Violations | Result |
|---|---|---|---|---|---|---|---|
| Toll notification | 589,997 | 222s | 315s | 315s | 350s | 589,997 | **FAIL** |
| Account balance | 308 | 28s | 39s | 39s | 42s | 308 | **FAIL** |
| Daily expenditure | 315 | 28s | 40s | 40s | 43s | 315 | **FAIL** |
| Accident notification | 0 | — | — | — | — | — | — |

| Metric | Value |
|---|---|
| Wall-clock duration | ~490s (for 600s simulated — could not keep up) |
| Total records processed | 590,620 |
| Process throughput | 655 records/sec |
| Peak CPU | 50.9% |
| Peak heap | 4,300 MB |

## Analysis

### Toll Notification Latency: Architectural Limitation

Even at L=10 with speculative emit enabled, toll notification latency remains at **p50=92 seconds** —
far above the 5-second requirement. This is not a scaling or configuration issue. It is caused by
the chained windowed joins in the toll notification pipeline:

```
Position Reports
  → 60s tumbling window: Number of Vehicles (NOV)
  → 300s sliding window / 60s advance: Latest Average Velocity (LAV)
  → 120s sliding window / 30s advance: Accident Detection
  → Windowed join (60s): NOV + LAV + Accidents → Current Toll
  → Windowed join (60s): Segment Crossing + Current Toll → Toll Notification
```

The Kafka Streams DSL `JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60))` buffers
records from both sides for up to 60 seconds waiting for matching keys. This creates a minimum
~60-second pipeline delay that compounds across the two join stages.

The speculative emit optimization (bypassing the `OnMinuteChangeEmitter`) successfully reduced
upstream aggregation latency, increasing toll notification throughput from 255K to 2.7M records.
However, it did not reduce the join latency because the bottleneck is in the DSL join operators
themselves, not in the aggregation emission.

### What Passes

- **Accident notifications** consistently pass at 1ms latency across all configurations.
- **Daily expenditure queries** are close to passing with speculative emit (p50=2s).
- **Account balance queries** are close to passing with speculative emit (p50=4s).

### What Would Be Needed for Toll Notifications to Pass

Replacing the DSL windowed joins with Processor API joins would allow immediate emission on
first match — but this would trade correctness guarantees (out-of-order handling, late arrivals)
for latency. Since the benchmark requires *correct* toll calculations, this is not a valid
optimization.

Alternative approaches:
- **Apache Flink** — supports event-time triggers that can emit on first match while maintaining
  correctness via watermarks and allowed lateness
- **Fundamental topology redesign** — replace chained windowed joins with continuously-updated
  KTables and table-table joins, avoiding windowed buffering entirely
- **Multi-broker Kafka cluster** — would not reduce join latency but would support higher
  L-ratings by increasing partition throughput

## Running the Benchmark

```bash
# Build
just build

# Start Kafka and create topics
just kafka-up 12
just topics-create 12

# Generate data (spec-compliant density: 3600 vehicles/xway ≈ 200 cars/lane/segment)
just generate <xways> <duration_min> 3600

# Create properties file
cat > benchmark-data/benchmark.properties <<EOF
linearroad.data.path=benchmark-data/benchmark.dat
linearroad.hisotical.data.path=benchmark-data/benchmark.dat.tolls.dat
linearroad.kafka.bootstrapservers=localhost:9092
linearroad.mode=all
linearroad.mode.debug=
linearroad.kafka.num_stream_threads=8
linearroad.feeding.realtime=true
linearroad.streaming.speculative_emit=false
EOF

# Run
java -jar target/kafka-linearroad-1.0-SNAPSHOT.jar benchmark-data/benchmark.properties
```

Live metrics are logged every 5 seconds during the run.

### Key Properties

| Property | Default | Description |
|---|---|---|
| `linearroad.feeding.realtime` | `false` | Pace feeding to event timestamps for proper benchmarking |
| `linearroad.streaming.speculative_emit` | `false` | Flush windowed aggregations every 2s instead of waiting for minute boundaries |
| `linearroad.kafka.num_stream_threads` | `0` (auto) | Number of Kafka Streams threads |

## Notes

- Latency is measured as `(wall_clock_elapsed - event_time)` — the delay between when an event
  should have occurred in realtime and when the response was produced.
- Vehicle density is ~200 cars/lane/segment per the LR specification.
- 12 partitions on a single broker outperforms 100 partitions due to reduced overhead.
- The `Records ingested` counter in the report is not yet wired to the feeder — only output
  stream records are counted.
