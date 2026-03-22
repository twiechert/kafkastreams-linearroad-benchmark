# Linear Road Benchmark Results

Local benchmark results on a single-node Kafka (KRaft) with Docker.

## Test Environment

| Component | Spec |
|---|---|
| Machine | Apple Silicon, 12 CPUs, 9 GB max heap |
| Kafka | Confluent CP 7.6.0 (single-node KRaft, Docker) |
| Java | OpenJDK 23.0.2 |
| Kafka Streams | 3.7.0 |
| Partitions | 12 (all source and sink topics) |
| Stream threads | 8 |
| Feeding mode | Max throughput (non-realtime) |

## Results

### L=50 (50 expressways, 1000 vehicles/xway, 10 min simulated)

| Metric | Value |
|---|---|
| Duration | 108 seconds |
| Toll notifications | 100,806 |
| Account balance responses | 450 |
| Daily expenditure responses | 450 |
| **Total records processed** | **101,706** |
| **Process throughput** | **939 records/sec** |
| Kafka internal process rate | 1,747 records/sec |
| Toll notification latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Account balance latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Daily expenditure latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Peak CPU | 44.4% |
| Peak heap | 2,452 MB |
| **Result** | **PASS — all latency requirements met** |

### L=100 (100 expressways, 1000 vehicles/xway, 10 min simulated)

| Metric | Value |
|---|---|
| Duration | 159 seconds |
| Toll notifications | 16,003 |
| Account balance responses | 900 |
| Daily expenditure responses | 900 |
| **Total records processed** | **17,803** |
| **Process throughput** | **112 records/sec** |
| Kafka internal process rate | 2,952 records/sec |
| Toll notification latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Account balance latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Daily expenditure latency | p50=1ms, p95=1ms, p99=1ms, max=1ms |
| Peak CPU | 41.7% |
| Peak heap | 2,461 MB |
| **Result** | **PASS — all latency requirements met** |

## Benchmark Configuration

Data is generated using `LinearRoadDataGenerator` with vehicles clustered into 3 segments per expressway to ensure toll conditions (>50 vehicles per segment) and low average velocities (<40 mph) are met.

### Running the Benchmark

```bash
# Generate data and run
just generate <xways> <duration_min> <vehicles_per_xway>

# Configure and run
# See justfile for full local benchmark: just local
```

### Key Properties

| Property | Description |
|---|---|
| `linearroad.feeding.realtime` | `true` to pace feeding to event timestamps, `false` for max throughput |
| `linearroad.kafka.num_stream_threads` | Number of Kafka Streams threads (0 = auto-detect CPUs) |

## Notes

- Accident notifications are not yet triggered by the data generator — the accident detection conditions (2+ vehicles stopped at the exact same position for 4 consecutive 30s reports) require more precise data generation.
- Partition count significantly impacts local performance: 12 partitions on a single broker outperforms 100 partitions due to reduced overhead.
- The `Records ingested` counter in the report is not yet wired to the feeder — only output stream records are counted.
