package de.twiechert.linroad.kafka.benchmark;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validates that all benchmark responses meet the 5-second latency requirement.
 * Collects response timestamps and compares them against their trigger timestamps.
 *
 * The Linear Road specification requires all responses (toll notifications, accident
 * notifications, account balance responses, daily expenditure responses) to be emitted
 * within 5 seconds of the triggering event.
 */
public class BenchmarkLatencyValidator {

    private static final long MAX_LATENCY_SECONDS = 5;

    private final AtomicLong totalResponses = new AtomicLong(0);
    private final AtomicLong lateResponses = new AtomicLong(0);
    private final AtomicLong maxLatencyMs = new AtomicLong(0);
    private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();

    /**
     * Records a response with its latency.
     *
     * @param triggerTimeMs  wall-clock time when the triggering event was sent
     * @param responseTimeMs wall-clock time when the response was received
     */
    public void recordResponse(long triggerTimeMs, long responseTimeMs) {
        long latencyMs = responseTimeMs - triggerTimeMs;
        totalResponses.incrementAndGet();
        latencies.add(latencyMs);

        if (latencyMs > MAX_LATENCY_SECONDS * 1000) {
            lateResponses.incrementAndGet();
        }

        maxLatencyMs.updateAndGet(current -> Math.max(current, latencyMs));
    }

    /**
     * @return true if all responses met the 5-second latency requirement
     */
    public boolean allResponsesOnTime() {
        return lateResponses.get() == 0;
    }

    public long getTotalResponses() {
        return totalResponses.get();
    }

    public long getLateResponses() {
        return lateResponses.get();
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs.get();
    }

    public double getAverageLatencyMs() {
        if (latencies.isEmpty()) return 0;
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    @Override
    public String toString() {
        return String.format("Responses: %d, Late: %d, Max latency: %dms, Avg latency: %.1fms, Pass: %s",
                totalResponses.get(), lateResponses.get(), maxLatencyMs.get(),
                getAverageLatencyMs(), allResponsesOnTime() ? "YES" : "NO");
    }
}
