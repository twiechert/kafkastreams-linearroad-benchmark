package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.TimedOnMinute;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.stream.processor.OnMinuteChangeEmitter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This stream program is able to recognize accidents.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccidentDetectionStreamBuilder {


    private static final Logger logger = LoggerFactory
            .getLogger(AccidentDetectionStreamBuilder.class);

    private LinearRoadKafkaBenchmarkApplication.Context context;

    public AccidentDetectionStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
    }

    public KStream<XwaySegmentDirection, Long> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify accidents");
        TimeWindows accDetectionWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(4 * 30)).advanceBy(Duration.ofSeconds(30));

        // an accident at minute m expressway x, segment s, direction d will be mapped to all segments downstream 0..4
        //detect an accident on a given segment whenever two or more vehicles are stopped in that segment at the same lane and position
        // therefore flat-mapping to all affected segments
        KStream<XwayLaneDirSegPosIntermediate, AccidentDetectionValIntermediate> accDetection = positionReportStream
                .filter((k, v) -> v.getSpeed() == 0)
                .map((key, value) -> new KeyValue<>(
                new XwayLaneDirSegPosIntermediate(key.getXway(), value.getLane(), key.getDir(), key.getSeg(), value.getPos()),
                new Pair<>(value.getTime(), value.getVehicleId())))
                // current time to use | if more than one vehicle in window | current count of position reports in window

                .groupByKey(Grouped.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .windowedBy(accDetectionWindow)
                .aggregate(() -> new AccidentDetectionValIntermediate(0L, new HashMap<>()),
                        (key, value, agg) -> {
                            if (!agg.getVehicleMap().containsKey(value.getValue1())) {
                                agg.getVehicleMap().put(value.getValue1(), 1);
                            } else {
                                agg.getVehicleMap().put(value.getValue1(), agg.getVehicleMap().get(value.getValue1()) + 1);
                            }
                            // the latest timestamp is updated again, which is required by the OnMinuteChangeEmitter to work properly
                            return new AccidentDetectionValIntermediate(Util.minuteOfReport(value.getValue0()), agg.getVehicleMap());
                        }
                        , Materialized.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v.setWindowEndMinute(Util.minuteOfReport(k.window().end()))))
                  /*
                  There must be at least two cars emitting four consecutive position reports.
                 */
                .filter((k, v) -> v.getVehicleMap().entrySet().stream().filter(p -> p.getValue() >= 4).count() >= 2);

        return OnMinuteChangeEmitter.getForWindowed(accDetection, new DefaultSerde<>(), new DefaultSerde<>(), "latest-acc")



                // key -> xway, segment, direction | value -> minute in which accident has been detected
                .flatMap((key, value0) ->
                        // creates range 0,1,2,3,4
                        IntStream.range(0, 5).mapToObj(in -> new KeyValue<>(new XwaySegmentDirection(key.getValue0(), ((key.getValue3() - in) < 0) ? 0 : key.getValue3() - in, key.getValue2()),
                                value0.getWindowEndMinute())).collect(Collectors.toList()))
                .repartition(Repartitioned.<XwaySegmentDirection, Long>with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("ACC_DETECTION")));


    }


    public static class AccidentDetectionValIntermediate extends Triplet<Long, HashMap<Integer, Integer>, Long> implements TimedOnMinute.TimedOnMinuteWithWindowEnd {

        public AccidentDetectionValIntermediate(Long time, HashMap<Integer, Integer> vehicles) {
            super(time, vehicles, 0L);
        }

        public AccidentDetectionValIntermediate(Long time, HashMap<Integer, Integer> vehicles, Long windowEndTime) {
            super(time, vehicles, windowEndTime);
        }

        public HashMap<Integer, Integer> getVehicleMap() {
            return this.getValue1();
        }

        public AccidentDetectionValIntermediate setWindowEndMinute(long windowEndMinute) {
            return new AccidentDetectionValIntermediate(this.getValue0(), this.getValue1(), windowEndMinute);
        }

        @Override
        public long getWindowEndMinute() {
            return getValue2();
        }

        @Override
        public long getMinute() {
            return getValue0();
        }
    }

    public static class XwayLaneDirSegPosIntermediate extends Quintet<Integer, Integer, Boolean, Integer, Integer> {
        public XwayLaneDirSegPosIntermediate(Integer xway, Integer lane, Boolean dir, Integer segment, Integer pos) {
            super(xway, lane, dir, segment, pos);
        }


    }

}
