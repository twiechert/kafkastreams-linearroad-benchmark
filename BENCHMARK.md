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

### L=10 (10 expressways, 3600 vehicles/xway, 10 min simulated) — FAIL

| Stream | p50 | p99 | max | Violations | Result |
|---|---|---|---|---|---|
| Toll notification (n=255,604) | 91s | 153s | 155s | 255,604 | FAIL |
| Account balance (n=90) | 4s | 8s | 8s | 3 | FAIL |
| Daily expenditure (n=90) | 3s | 6s | 6s | 1 | FAIL |
| Accident notification (n=245) | 1ms | 1ms | 1ms | 0 | PASS |

| Metric | Value |
|---|---|
| Wall-clock duration | 616s for 600s simulated |
| Total records processed | 256,029 |
| Peak CPU | 46.9% |
| Peak heap | 2,569 MB |

### L=50 (50 expressways, 3600 vehicles/xway, 10 min simulated) — FAIL

| Stream | p50 | p99 | max | Violations | Result |
|---|---|---|---|---|---|
| Toll notification (n=589,997) | 222s | 315s | 350s | 589,997 | FAIL |
| Account balance (n=308) | 28s | 39s | 42s | 308 | FAIL |
| Daily expenditure (n=315) | 28s | 40s | 43s | 315 | FAIL |
| Accident notification (n=0) | — | — | — | — | — |

| Metric | Value |
|---|---|
| Wall-clock duration | ~490s for 600s simulated |
| Total records processed | 590,620 |
| Peak CPU | 50.9% |
| Peak heap | 4,300 MB |

## Analysis

Even at L=10 with spec-compliant density, the system cannot meet the 5-second latency
requirement for toll notifications. The toll notification p50 latency is **91 seconds** —
far above the 5-second threshold.

### Root Cause: Architectural Latency from Windowed Joins

The toll notification path involves multiple chained windowed operations:

```
Position Reports
  → 60s window: Number of Vehicles (NOV)
  → 300s window: Latest Average Velocity (LAV)
  → 120s window: Accident Detection
  → Windowed join: NOV + LAV + Accidents → Current Toll
  → Windowed join: Segment Crossing + Current Toll → Toll Notification
```

Each windowed aggregation in Kafka Streams emits results only when the window closes
(or when new events advance stream time past the window boundary). With 60-second tumbling
windows, there is a **minimum ~60-second inherent delay** before toll values are available
for downstream joins. The multi-stage pipeline compounds this.

This is not a scaling issue — adding more brokers or threads will not reduce it.
It is a fundamental limitation of the Kafka Streams DSL windowed-join approach for
this specific benchmark topology.

### What Would Be Needed to Pass

- **Suppress-based windowing** or **custom processors** that emit early/speculative results
  before window closure, rather than waiting for final aggregates
- **Punctuation-based emit** using wall-clock time to force output at regular intervals
- Alternatively, a **Flink-style approach** with event-time triggers and allowed lateness

### What Works Well

- Account balance and daily expenditure queries are close to passing at L=10
  (p50=3-4s, only a few violations at p99)
- Accident notifications pass with 1ms latency
- The topology correctly produces toll notifications, accident detections, and all
  query responses — the logic is sound, the latency is the bottleneck
- The system handles 256K+ records at L=10 and 590K+ at L=50

## Running the Benchmark

```bash
# Generate data: just generate <xways> <duration_min> <vehicles_per_xway>
just generate 10 10 3600

# Create properties file with realtime=true, then run:
java -jar target/kafka-linearroad-1.0-SNAPSHOT.jar benchmark-data/benchmark.properties
```

Live metrics are logged every 5 seconds during the run.

## Notes

- Latency is measured as `(wall_clock_elapsed - event_time)` — the delay between when an event
  should have occurred in realtime and when the response was produced.
- Vehicle density is ~200 cars/lane/segment per the LR specification.
- 12 partitions on a single broker outperforms 100 partitions due to reduced overhead.
