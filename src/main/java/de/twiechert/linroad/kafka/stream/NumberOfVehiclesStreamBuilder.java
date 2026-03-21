package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.NumberOfVehicles;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.stream.processor.OnMinuteChangeEmitter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashSet;


/**
 * This class builds the number of vehicles that per (expressway, segment, direction) tuple.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class NumberOfVehiclesStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(NumberOfVehiclesStreamBuilder.class);

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public KStream<XwaySegmentDirection, NumberOfVehicles> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify number of vehicles at expressway, segment and direction per minute.");

        KStream<XwaySegmentDirection, NumberOfVehicles> novAgg = positionReportStream.mapValues(v -> new Pair<>(v.getVehicleId(), v.getTime()))
                // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .groupByKey(Grouped.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
                .aggregate(() -> new VehicleIdTimeIntermediate(0L, new HashSet<>()), (key, value, agg) -> {
                    agg.getValue1().add(value.getValue0());
                    return new VehicleIdTimeIntermediate(value.getValue1(), agg.getValue1());
                }, Materialized.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .toStream().map((k, v) -> new KeyValue<>(k.key(), new NumberOfVehicles(Util.minuteOfReport(k.window().end()), v.getValue1().size())));

        return OnMinuteChangeEmitter.get(novAgg, new DefaultSerde<>(), new DefaultSerde<>(), "latest-nov");

    }


    public static class VehicleIdTimeIntermediate extends Pair<Long, HashSet<Integer>> {
        public VehicleIdTimeIntermediate(Long value0, HashSet<Integer> value1) {
            super(value0, value1);
        }

    }


}
