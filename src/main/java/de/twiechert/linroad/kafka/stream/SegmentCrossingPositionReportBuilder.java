package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.SegmentCrossing;
import de.twiechert.linroad.kafka.model.VehicleIdXwayDirection;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

/**
 * This stream is required by the toll notification stream {@link TollNotificationStreamBuilder} and the toll assessment {@link de.twiechert.linroad.kafka.stream.historical.table.CurrentExpenditurePerVehicleTableBuilder}.
 * It emits position reports only, if the segment has changed since the last position report, or if it is the very first position report.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class SegmentCrossingPositionReportBuilder {

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(SegmentCrossingPositionReportBuilder.class);

    private LinearRoadKafkaBenchmarkApplication.Context context;

    public SegmentCrossingPositionReportBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
    }

    public KStream<VehicleIdXwayDirection, SegmentCrossing> getStream(KStream<XwaySegmentDirection, PositionReport> positionReports) {
        logger.debug("Building stream to notify segment crossing position reports");


        KStream<VehicleIdXwayDirection, SegmentCrossing> posReportByVehicleXwayDir = positionReports
                .map((k, v) -> new KeyValue<>(new VehicleIdXwayDirection(v.getVehicleId(), k), new SegmentCrossing(v.getTime(), k.getSeg(), v.getLane())))
                .repartition(Repartitioned.with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("POS_BY_VEHICLE_XWAY_DIR")));

        KStream<VehicleIdXwayDirection, SegmentCrossing> posReportByVehicleXwayDirShifted = positionReports
                /*
                  + 30 is necessary, because second stream must be before
                  THIS IS NOT EQUAL TO USING BEFORE(), AFTER()
                 */
                .map((k, v) -> new KeyValue<>(new VehicleIdXwayDirection(v.getVehicleId(), k), new SegmentCrossing(v.getTime() + 30, k.getSeg(), v.getLane())))
                .repartition(Repartitioned.with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("POS_BY_VEHICLE_XWAY_DIR_SHIFTED")));

        /*
          ... must calculate a toll every time a vehicle reports a position in a new segment, and notify the driver of this toll.
          --> we must check if the segment has changed since the last position report of that vehicle.
          Because Kafka streams does not support data-driven windows, we self-join the position report with a slide of 30 seconds.
          We must use a left join, because the first element does not have a predecessor.
         */
        return posReportByVehicleXwayDir.leftJoin(posReportByVehicleXwayDirShifted,
                // We analyze if the positions reports at t_0=x and t_1=x+30 have not been issued from the same segment
                (report1, report2) -> new Quartet<>(report1.getTime(), report1.getSegment(), report2 == null || !report1.getSegment().equals(report2.getSegment()), report1.getLane()),
                // self-join x1 on x2 such that x2 is 30 seconds before...
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                StreamJoined.with(new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>()))
                // we consider only those where the segment has changed....
                .filter((k, v) -> v.getValue2())
                .mapValues(v -> new SegmentCrossing(v.getValue0(), v.getValue1(), v.getValue3()))
                // with that hack, we can save the time of the predecessor (timestamp of vehicle emitting position in segment before) which is required to the toll notification
                // however, this assumes that events do not arrive out of order (per-key)
                .groupByKey(Grouped.with(new DefaultSerde<>(), new DefaultSerde<>()))
                .aggregate(SegmentCrossing::new, (key, value, agg) -> value.setPredecessorTime(agg.getTime()),
                        Materialized.as(context.topic("SEG_CROSSISNGS_WITH_PREDECESSOR")))
                .toStream();
    }
}
