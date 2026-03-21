package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

/**
 * This stream realizes the toll notification.
 * It is not the same as the current toll stream {@link CurrentTollStreamBuilder}. A couple of things have to be considered here:
 * <p>
 * (I) tolls are triggered by the position report of a vehicle
 * (II) tolls are only calculated, if a vehicle has changed the segment since the last position report (which is not guaranteed)
 * (III) if the new segment is an exit lane, neither a toll is notified/assessed
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TollNotificationStreamBuilder extends StreamBuilder<Void, TollNotification> {

    public static String TOPIC = "TOLL_NOT";

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(TollNotificationStreamBuilder.class);


    public TollNotificationStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    public KStream<Void, TollNotification> getStream(KStream<VehicleIdXwayDirection, SegmentCrossing> segmentCrossingPositionReports,
                                                     KStream<XwaySegmentDirection, CurrentToll> currentTollStream) {
        logger.debug("Building stream to notify drivers about accidents");


        /*
         * If the vehicle exits at the exit ramp of a segment, the toll for that segment is not charged. -> thus position reports on exits can be ignored.
         *
         * Before joining, times have to be remapped, because: "the toll reported for the segment being exited is assessed to the vehicle's account.
         * Thus, a toll calculation for one segment often is concurrent with an account being debited for the previous segment."
         * in order to join, we map the time to minutes (tolls are based on the current minute)
         * but we must preserve the actual timestamp, because it has to be emitted in the response stream
         */
        KStream<XwaySegmentDirection, ConsecutivePosReportIntermediate> segmentCrossingPerXwaySegmentDir = segmentCrossingPositionReports
                .filter((k, v) -> v.getLane() != 4)
                /*
                  When position report for change to segment s, we need to assses segment s-1 and for that we need
                  to subtract 1 in the segment and change the event time accordingly
                 */
                .map((k, v) -> new KeyValue<>(new XwaySegmentDirection(k.getXway(), v.getSegment(), k.getDir()),
                        // for joining purpose we need the minute of the preceding position report, but we need to keep the exact timestamp for emitting
                        new ConsecutivePosReportIntermediate(Util.minuteOfReport(v.getTime()), v.getTime(), k.getVehicleId())))
                // join with current toll stream, create VID, time, current time, speed , toll
                .repartition(Repartitioned.with(new DefaultSerde<>(), new DefaultSerde<>()).withName("SEG_CROSSINGS_FOR_TOLL_NOT"));

        return segmentCrossingPerXwaySegmentDir
                .join(currentTollStream, (psRep, currToll) -> new TollNotification(psRep.getVehicleId(), psRep.getTime(), LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(), currToll.getVelocity(), currToll.getToll()),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
                        StreamJoined.with(new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>()))
                .selectKey((k, v) -> new Void());
    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


    public static class ConsecutivePosReportIntermediate extends Triplet<Long, Long, Integer> {

        public ConsecutivePosReportIntermediate(Long timeMinute, Long time, Integer vehicleId) {
            super(timeMinute, time, vehicleId);
        }

        public Long getTime() {
            return this.getValue1();
        }

        public Integer getVehicleId() {
            return this.getValue2();
        }

    }


}
