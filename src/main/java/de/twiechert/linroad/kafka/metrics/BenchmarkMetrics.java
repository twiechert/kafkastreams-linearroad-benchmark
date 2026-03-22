package de.twiechert.linroad.kafka.metrics;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.sun.management.OperatingSystemMXBean;

/**
 * Collects benchmark metrics during a Linear Road benchmark run.
 *
 * Tracks per-stream-component latencies, throughput, response latencies
 * (with percentiles), and integrates with Kafka Streams' built-in metrics.
 */
public class BenchmarkMetrics {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkMetrics.class);

    private final long startTimeMs = System.currentTimeMillis();

    // --- CPU & Memory ---
    private final OperatingSystemMXBean osMxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
    private final Runtime runtime = Runtime.getRuntime();

    // Peak values tracked across all snapshots
    private volatile double peakCpuLoad = 0;
    private volatile long peakHeapUsedBytes = 0;
    private volatile long peakRssBytes = 0;

    // --- Throughput ---
    private final LongAdder recordsIngested = new LongAdder();
    private final LongAdder recordsProcessed = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> recordsPerStream = new ConcurrentHashMap<>();

    // --- Latency per response type (toll notification, accident notification, etc.) ---
    private final ConcurrentHashMap<String, LatencyTracker> responseLatencies = new ConcurrentHashMap<>();

    // --- Partition-level metrics ---
    private final ConcurrentHashMap<Integer, LongAdder> recordsPerPartition = new ConcurrentHashMap<>();

    // --- Periodic snapshots for time-series ---
    private final ConcurrentLinkedQueue<Snapshot> snapshots = new ConcurrentLinkedQueue<>();

    public void recordIngested() {
        recordsIngested.increment();
    }

    public void recordIngested(int partition) {
        recordsIngested.increment();
        recordsPerPartition.computeIfAbsent(partition, k -> new LongAdder()).increment();
    }

    public void recordProcessed(String streamName) {
        recordsProcessed.increment();
        recordsPerStream.computeIfAbsent(streamName, k -> new LongAdder()).increment();
    }

    public long getProcessedCount() {
        return recordsProcessed.sum();
    }

    /**
     * Returns a compact live summary of latencies per response type.
     */
    public String getLiveLatencySummary() {
        StringBuilder sb = new StringBuilder();
        responseLatencies.forEach((name, tracker) -> {
            LatencyStats stats = tracker.getStats();
            if (stats.count() > 0) {
                sb.append(String.format("%s: n=%d p50=%dms p99=%dms max=%dms | ",
                        name, stats.count(), stats.p50Ms(), stats.p99Ms(), stats.maxMs()));
            }
        });
        return sb.length() > 0 ? sb.toString() : "no responses yet";
    }

    /**
     * Records a response latency (wall-clock time from trigger to response).
     *
     * @param responseType e.g. "toll_notification", "accident_notification"
     * @param latencyMs    wall-clock latency in milliseconds
     */
    public void recordResponseLatency(String responseType, long latencyMs) {
        responseLatencies.computeIfAbsent(responseType, k -> new LatencyTracker()).record(latencyMs);
    }

    /**
     * Takes a point-in-time snapshot for time-series analysis, including CPU and memory.
     */
    public void takeSnapshot() {
        double processCpuLoad = osMxBean.getProcessCpuLoad();
        if (processCpuLoad < 0) processCpuLoad = 0; // -1 means not available

        MemoryUsage heapUsage = memoryMxBean.getHeapMemoryUsage();
        long heapUsedBytes = heapUsage.getUsed();
        long heapMaxBytes = heapUsage.getMax();

        // Committed memory (resident-ish for JVM)
        long committedHeapBytes = heapUsage.getCommitted();
        MemoryUsage nonHeapUsage = memoryMxBean.getNonHeapMemoryUsage();
        long totalCommittedBytes = committedHeapBytes + nonHeapUsage.getCommitted();

        // Track peaks
        if (processCpuLoad > peakCpuLoad) peakCpuLoad = processCpuLoad;
        if (heapUsedBytes > peakHeapUsedBytes) peakHeapUsedBytes = heapUsedBytes;
        if (totalCommittedBytes > peakRssBytes) peakRssBytes = totalCommittedBytes;

        snapshots.add(new Snapshot(
                System.currentTimeMillis() - startTimeMs,
                recordsIngested.sum(),
                recordsProcessed.sum(),
                processCpuLoad,
                heapUsedBytes,
                heapMaxBytes,
                totalCommittedBytes
        ));
    }

    /**
     * Extracts Kafka Streams built-in metrics.
     */
    public Map<String, Double> extractKafkaStreamsMetrics(KafkaStreams streams) {
        Map<String, Double> result = new TreeMap<>();
        Map<MetricName, ? extends Metric> metrics = streams.metrics();

        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName name = entry.getKey();
            Object value = entry.getValue().metricValue();

            // Extract the most useful metrics
            String group = name.group();
            String metricName = name.name();

            if (value instanceof Double && !((Double) value).isNaN()) {
                if (group.contains("stream-thread-metrics")) {
                    if (metricName.contains("process-rate") ||
                            metricName.contains("process-latency") ||
                            metricName.contains("commit-rate") ||
                            metricName.contains("poll-records")) {
                        result.put(group + "." + metricName, (Double) value);
                    }
                } else if (group.contains("stream-task-metrics")) {
                    if (metricName.contains("process-latency") ||
                            metricName.contains("record-lateness")) {
                        String taskId = name.tags().getOrDefault("task-id", "unknown");
                        result.put("task." + taskId + "." + metricName, (Double) value);
                    }
                } else if (group.contains("stream-state-metrics")) {
                    if (metricName.contains("put-rate") ||
                            metricName.contains("get-rate") ||
                            metricName.contains("all-rate")) {
                        String storeId = name.tags().getOrDefault("store-id", "unknown");
                        result.put("state." + storeId + "." + metricName, (Double) value);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Generates the full benchmark report.
     */
    public BenchmarkReport generateReport(KafkaStreams streams) {
        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        return new BenchmarkReport(
                elapsedMs,
                recordsIngested.sum(),
                recordsProcessed.sum(),
                new TreeMap<>(getStreamCounts()),
                new TreeMap<>(getResponseLatencyStats()),
                getPartitionSkew(),
                new ResourceStats(peakCpuLoad, peakHeapUsedBytes, peakRssBytes,
                        runtime.availableProcessors(),
                        runtime.maxMemory()),
                streams != null ? extractKafkaStreamsMetrics(streams) : Collections.emptyMap(),
                new ArrayList<>(snapshots)
        );
    }

    private Map<String, Long> getStreamCounts() {
        Map<String, Long> counts = new HashMap<>();
        recordsPerStream.forEach((k, v) -> counts.put(k, v.sum()));
        return counts;
    }

    private Map<String, LatencyStats> getResponseLatencyStats() {
        Map<String, LatencyStats> stats = new HashMap<>();
        responseLatencies.forEach((k, v) -> stats.put(k, v.getStats()));
        return stats;
    }

    private PartitionSkew getPartitionSkew() {
        if (recordsPerPartition.isEmpty()) return new PartitionSkew(0, 0, 0, 0);

        long[] counts = recordsPerPartition.values().stream()
                .mapToLong(LongAdder::sum).sorted().toArray();
        long min = counts[0];
        long max = counts[counts.length - 1];
        double avg = Arrays.stream(counts).average().orElse(0);
        double skew = avg > 0 ? (max - min) / avg : 0;

        return new PartitionSkew(min, max, avg, skew);
    }

    // --- Inner classes ---

    public static class LatencyTracker {
        private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        private final LongAdder count = new LongAdder();
        private final AtomicLong maxLatency = new AtomicLong(0);

        public void record(long latencyMs) {
            latencies.add(latencyMs);
            count.increment();
            maxLatency.updateAndGet(current -> Math.max(current, latencyMs));
        }

        public LatencyStats getStats() {
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            if (sorted.length == 0) return new LatencyStats(0, 0, 0, 0, 0, 0, 0);

            return new LatencyStats(
                    sorted.length,
                    sorted[(int) (sorted.length * 0.5)],
                    sorted[(int) (sorted.length * 0.95)],
                    sorted[(int) (sorted.length * 0.99)],
                    sorted[sorted.length - 1],
                    Arrays.stream(sorted).average().orElse(0),
                    Arrays.stream(sorted).filter(l -> l > 5000).count()
            );
        }
    }

    public record LatencyStats(long count, long p50Ms, long p95Ms, long p99Ms, long maxMs, double avgMs,
                               long violationCount) {
        public boolean meetsLatencyRequirement() {
            return violationCount == 0;
        }

        @Override
        public String toString() {
            return String.format("n=%d p50=%dms p95=%dms p99=%dms max=%dms avg=%.1fms violations=%d %s",
                    count, p50Ms, p95Ms, p99Ms, maxMs, avgMs, violationCount,
                    meetsLatencyRequirement() ? "PASS" : "FAIL");
        }
    }

    public record PartitionSkew(long min, long max, double avg, double skewRatio) {
        @Override
        public String toString() {
            return String.format("min=%d max=%d avg=%.0f skew=%.2f", min, max, avg, skewRatio);
        }
    }

    public record ResourceStats(double peakCpuLoad, long peakHeapUsedBytes, long peakCommittedBytes,
                                int availableProcessors, long maxHeapBytes) {
        @Override
        public String toString() {
            return String.format("cpuPeak=%.1f%% heapPeak=%dMB committed=%dMB maxHeap=%dMB cores=%d",
                    peakCpuLoad * 100,
                    peakHeapUsedBytes / (1024 * 1024),
                    peakCommittedBytes / (1024 * 1024),
                    maxHeapBytes / (1024 * 1024),
                    availableProcessors);
        }
    }

    public record Snapshot(long elapsedMs, long ingested, long processed,
                           double cpuLoad, long heapUsedBytes, long heapMaxBytes, long committedBytes) {
    }

    public record BenchmarkReport(
            long elapsedMs,
            long recordsIngested,
            long recordsProcessed,
            Map<String, Long> recordsPerStream,
            Map<String, LatencyStats> responseLatencies,
            PartitionSkew partitionSkew,
            ResourceStats resourceStats,
            Map<String, Double> kafkaStreamsMetrics,
            List<Snapshot> throughputTimeline
    ) {
        public void printToConsole() {
            System.out.println();
            System.out.println("╔══════════════════════════════════════════════════╗");
            System.out.println("║        LINEAR ROAD BENCHMARK REPORT             ║");
            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.printf("║ Duration:           %,.1f seconds%n", elapsedMs / 1000.0);
            System.out.printf("║ Records ingested:   %,d%n", recordsIngested);
            System.out.printf("║ Records processed:  %,d%n", recordsProcessed);
            System.out.printf("║ Ingest throughput:  %,.0f records/sec%n",
                    recordsIngested * 1000.0 / Math.max(elapsedMs, 1));
            System.out.printf("║ Process throughput: %,.0f records/sec%n",
                    recordsProcessed * 1000.0 / Math.max(elapsedMs, 1));
            System.out.println("╠══════════════════════════════════════════════════╣");

            System.out.println("║ PER-STREAM RECORD COUNTS:");
            recordsPerStream.forEach((stream, count) ->
                    System.out.printf("║   %-35s %,d%n", stream, count));

            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.println("║ RESPONSE LATENCIES (5s requirement):");
            boolean allPass = true;
            for (var entry : responseLatencies.entrySet()) {
                System.out.printf("║   %-20s %s%n", entry.getKey(), entry.getValue());
                if (!entry.getValue().meetsLatencyRequirement()) allPass = false;
            }

            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.printf("║ PARTITION SKEW:     %s%n", partitionSkew);

            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.println("║ RESOURCE USAGE:");
            System.out.printf("║   Peak CPU load:      %.1f%%%n", resourceStats.peakCpuLoad() * 100);
            System.out.printf("║   Peak heap used:     %,d MB%n", resourceStats.peakHeapUsedBytes() / (1024 * 1024));
            System.out.printf("║   Peak committed mem: %,d MB%n", resourceStats.peakCommittedBytes() / (1024 * 1024));
            System.out.printf("║   Max heap:           %,d MB%n", resourceStats.maxHeapBytes() / (1024 * 1024));
            System.out.printf("║   Available CPUs:     %d%n", resourceStats.availableProcessors());

            if (!kafkaStreamsMetrics.isEmpty()) {
                System.out.println("╠══════════════════════════════════════════════════╣");
                System.out.println("║ KAFKA STREAMS INTERNAL METRICS:");
                kafkaStreamsMetrics.entrySet().stream()
                        .limit(20) // top 20
                        .forEach(e -> System.out.printf("║   %-40s %.4f%n", e.getKey(), e.getValue()));
            }

            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.printf("║ OVERALL: %s%n", allPass ? "✓ ALL LATENCY REQUIREMENTS MET" : "✗ LATENCY VIOLATIONS DETECTED");
            System.out.println("╚══════════════════════════════════════════════════╝");
        }

        public void writeCsv(File outputFile) throws IOException {
            try (PrintWriter w = new PrintWriter(new FileWriter(outputFile))) {
                w.println("elapsed_ms,ingested,processed,cpu_load,heap_used_mb,heap_max_mb,committed_mb");
                for (Snapshot s : throughputTimeline) {
                    w.printf("%d,%d,%d,%.4f,%d,%d,%d%n",
                            s.elapsedMs(), s.ingested(), s.processed(),
                            s.cpuLoad(),
                            s.heapUsedBytes() / (1024 * 1024),
                            s.heapMaxBytes() / (1024 * 1024),
                            s.committedBytes() / (1024 * 1024));
                }
            }
        }
    }
}
