# Linear Road Benchmark Results

Local benchmark results on a single-node Kafka (KRaft) with Docker.
All results use **realtime feeding** — data is paced to match encoded event timestamps,
so N minutes of simulated data takes N minutes of wall-clock time.

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

### L=50 (50 expressways, 3600 vehicles/xway, 10 min simulated) — FAIL

| Metric | Value |
|---|---|
| Wall-clock duration | ~490s (8.2 min) for 10 min simulated |
| Toll notifications | 589,997 |
| Account balance responses | 308 |
| Daily expenditure responses | 315 |
| **Total records processed** | **590,620** |
| Toll notification latency | p50=222s, p99=315s, max=350s |
| Account balance latency | p50=28s, p99=39s, max=42s |
| Daily expenditure latency | p50=28s, p99=40s, max=43s |
| Peak CPU | ~50% |
| Peak heap | ~4.3 GB |
| **Result** | **FAIL — all streams exceed 5s latency requirement** |

### Analysis

At L=50 with spec-compliant density (200 cars/lane/segment), the system cannot meet the 5-second
latency requirement on a single local Kafka broker. The toll notification stream accumulates
~3-5 minutes of processing lag behind realtime.

Key bottlenecks:
- Single Kafka broker limits partition throughput
- Windowed joins and repartitioning create significant overhead
- 180K records per 30-second tick overwhelms the local setup

### Next Steps

- Binary search for max achievable L-rating on this hardware
- Test with multi-broker Kafka (AWS MSK) for proper scaling
- Investigate JMX-based metrics for more accurate latency measurement

## Benchmark Configuration

Data is generated using `LinearRoadDataGenerator` with vehicles clustered into 3 segments
per expressway to ensure toll conditions (>50 vehicles per segment) and low average velocities
(<40 mph) are met. Density is ~200 cars/lane/segment per the LR specification.

### Running the Benchmark

```bash
# Generate data: just generate <xways> <duration_min> <vehicles_per_xway>
just generate 50 10 3600

# Run with realtime feeding
# Edit benchmark.properties to set linearroad.feeding.realtime=true
java -jar target/kafka-linearroad-1.0-SNAPSHOT.jar benchmark-data/benchmark.properties
```

### Key Properties

| Property | Description |
|---|---|
| `linearroad.feeding.realtime` | `true` to pace feeding to event timestamps, `false` for max throughput |
| `linearroad.kafka.num_stream_threads` | Number of Kafka Streams threads (0 = auto-detect CPUs) |

## Notes

- Latency is measured as `(wall_clock_elapsed - event_time)` — the delay between when an event
  should have occurred in realtime and when the response was produced.
- Live metrics are logged every 5 seconds during the run.
- Partition count significantly impacts local performance: 12 partitions on a single broker
  outperforms 100 partitions due to reduced overhead.
