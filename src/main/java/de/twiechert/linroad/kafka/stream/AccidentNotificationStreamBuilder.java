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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * This class notifies drivers about occurred accidents if they are close-by according to the LR requirements.
 * For that purpose, the segment crossing position report is used, because position reports can only trigger
 * accident notifications, if the preceding position report of the same vehicle has not been issued from the same segment.
 *
 * Thus, it is convenient to use the reduced segment crossing position report stream, that only contains the first position report within a segment per vehicle.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccidentNotificationStreamBuilder extends StreamBuilder<Void, AccidentNotification> {

    private static final String TOPIC = "ACC_NOT";

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(AccidentNotificationStreamBuilder.class);


    @Autowired
    public AccidentNotificationStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    public KStream<Void, AccidentNotification> getStream(KStream<VehicleIdXwayDirection, SegmentCrossing> segmentCrossingPositionReports,
                                                         KStream<XwaySegmentDirection, Long> accidentReports) {
        logger.debug("Building stream to notify drivers about accidents");


        /*
          We do not use the normal position report stream, because two consecutive position reports from the same segment must not cause two accident notifications.
          We use the consecutive position report stream, that only emits the first position report in a segment per vehicle.
         */
        KStream<XwaySegmentDirection, AccidentNotificationIntermediate> segmentCrossingPositionReportsForAccNotification = segmentCrossingPositionReports.map((k, v) -> new KeyValue<>(new XwaySegmentDirection(k.getXway(), v.getSegment(), k.getDir()), AccidentNotificationIntermediate.fromPosReport(v)))
                .repartition(Repartitioned.with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("ACC_DET_POS")));
        /*
          The trigger for an accident notification is a position report
          that identifies a vehicle entering a segment 0 to 4 segments upstream of some accident location,
          but only if q was emitted no earlier than theminute following theminutewhen the accident occurred, and no later than the minute the accident is
         */

        /*
         *Additionally the authors require that the notification must happen only if the respective position report q was emitted no earlier
         * than the minute following the minute when the accident occurred, and no later than the minute the accident is cleared.
         * Thus, a position report at minute $m$ will not trigger an accident notification of an accident occurred at the same minute.
         * Technically this is approached by modifying the event time of the position report source stream.
         */
        return  accidentReports.join(segmentCrossingPositionReportsForAccNotification, (value1, value2) -> value2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
                        StreamJoined.with(new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>()))
                // no notification required if exit-lane
                .filter((k,v) -> v.getLane() != 4)
                .map((k, v) -> new KeyValue<>(new Void(), new AccidentNotification(v.getPositionRepRequestTimeInSec(), LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(), k.getSeg())));

    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


    public static class AccidentNotificationIntermediate extends Triplet<Long, Long, Integer> {

        public AccidentNotificationIntermediate(Long minute, Long second, Integer lane) {
            super(minute, second, lane);
        }

        public int getLane() {
            return getValue2();
        }

        public long getPositionRepRequestTimeInSec() {
            return getValue1();
        }

        public static AccidentNotificationIntermediate fromPosReport(SegmentCrossing segmentCrossingPositionReport) {
            long timeToUse = Util.minuteOfReport(segmentCrossingPositionReport.getTime())-1;
            return new AccidentNotificationIntermediate((timeToUse >= 0)? timeToUse: 0, segmentCrossingPositionReport.getTime(), segmentCrossingPositionReport.getLane());

        }

    }
}
