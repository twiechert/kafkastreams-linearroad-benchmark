package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.NumberOfVehicles;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the Number of Vehicles stream topology using TopologyTestDriver.
 * Verifies that distinct vehicles are counted correctly per (xway, segment, direction) within 1-minute windows.
 */
class NumberOfVehiclesTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<XwaySegmentDirection, PositionReport> inputTopic;
    private TestOutputTopic<XwaySegmentDirection, NumberOfVehicles> outputTopic;

    private static final String INPUT_TOPIC = "position-reports";
    private static final String OUTPUT_TOPIC = "nov-output";

    @BeforeEach
    void setup() {
        StreamsBuilder builder = new StreamsBuilder();

        DefaultSerde<XwaySegmentDirection> keySerde = new DefaultSerde<>();
        DefaultSerde<PositionReport> valueSerde = new DefaultSerde<>();

        KStream<XwaySegmentDirection, PositionReport> positionReportStream =
                builder.stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde));

        // Build the NOV stream inline (simplified version without OnMinuteChangeEmitter)
        positionReportStream.mapValues(v -> new org.javatuples.Pair<>(v.getVehicleId(), v.getTime()))
                .groupByKey(Grouped.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
                .aggregate(() -> new NumberOfVehiclesTopologyTest.VehicleIdTimeIntermediate(0L, new HashSet<>()),
                        (key, value, agg) -> {
                            agg.vehicleIds.add(value.getValue0());
                            return new VehicleIdTimeIntermediate(value.getValue1(), agg.vehicleIds);
                        },
                        Materialized.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), new NumberOfVehicles(Util.minuteOfWindowEnd(k.window().end()), v.vehicleIds.size())))
                .to(OUTPUT_TOPIC, Produced.with(new DefaultSerde<>(), new DefaultSerde<>()));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nov-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, keySerde.serializer(), valueSerde.serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, new DefaultSerde<XwaySegmentDirection>().deserializer(), new DefaultSerde<NumberOfVehicles>().deserializer());
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void singleVehicleInSegment_countsOne() {
        XwaySegmentDirection key = new XwaySegmentDirection(1, 5, true);
        PositionReport report = new PositionReport(10L, 100, 60, 1, 500);

        inputTopic.pipeInput(key, report);

        assertFalse(outputTopic.isEmpty(), "Should produce at least one output");
        KeyValue<XwaySegmentDirection, NumberOfVehicles> result = outputTopic.readKeyValue();
        assertEquals(1, result.key.getXway());
        assertEquals(5, result.key.getSeg());
        assertEquals(1, result.value.getNumberOfVehicles());
    }

    @Test
    void multipleDistinctVehicles_countedCorrectly() {
        XwaySegmentDirection key = new XwaySegmentDirection(1, 5, true);

        // Three different vehicles in the same segment within the same minute
        inputTopic.pipeInput(key, new PositionReport(10L, 100, 60, 1, 500));
        inputTopic.pipeInput(key, new PositionReport(20L, 200, 55, 1, 510));
        inputTopic.pipeInput(key, new PositionReport(30L, 300, 70, 2, 520));

        // Read all outputs and check the last one has count 3
        KeyValue<XwaySegmentDirection, NumberOfVehicles> lastResult = null;
        while (!outputTopic.isEmpty()) {
            lastResult = outputTopic.readKeyValue();
        }

        assertNotNull(lastResult);
        assertEquals(3, lastResult.value.getNumberOfVehicles(),
                "Should count 3 distinct vehicles");
    }

    @Test
    void sameVehicleDuplicateReports_countedOnce() {
        XwaySegmentDirection key = new XwaySegmentDirection(1, 5, true);

        // Same vehicle ID (100) reports twice
        inputTopic.pipeInput(key, new PositionReport(10L, 100, 60, 1, 500));
        inputTopic.pipeInput(key, new PositionReport(20L, 100, 55, 1, 510));

        KeyValue<XwaySegmentDirection, NumberOfVehicles> lastResult = null;
        while (!outputTopic.isEmpty()) {
            lastResult = outputTopic.readKeyValue();
        }

        assertNotNull(lastResult);
        assertEquals(1, lastResult.value.getNumberOfVehicles(),
                "Same vehicle should only be counted once");
    }

    @Test
    void differentSegments_countedSeparately() {
        XwaySegmentDirection key1 = new XwaySegmentDirection(1, 5, true);
        XwaySegmentDirection key2 = new XwaySegmentDirection(1, 6, true);

        inputTopic.pipeInput(key1, new PositionReport(10L, 100, 60, 1, 500));
        inputTopic.pipeInput(key2, new PositionReport(10L, 200, 55, 1, 510));

        // Both segments should produce outputs with count 1 each
        int count = 0;
        while (!outputTopic.isEmpty()) {
            KeyValue<XwaySegmentDirection, NumberOfVehicles> result = outputTopic.readKeyValue();
            assertEquals(1, result.value.getNumberOfVehicles());
            count++;
        }
        assertTrue(count >= 2, "Should have outputs for both segments");
    }

    // Simplified intermediate class for the test
    static class VehicleIdTimeIntermediate implements java.io.Serializable {
        Long time;
        HashSet<Integer> vehicleIds;

        VehicleIdTimeIntermediate(Long time, HashSet<Integer> vehicleIds) {
            this.time = time;
            this.vehicleIds = vehicleIds;
        }
    }
}
