package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

/**
 * This stream generates a pair of the current toll and LAV per XwaySegmentDirection tuple
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 *
 */
public class CurrentTollStreamBuilder extends StreamBuilder<XwaySegmentDirection, CurrentToll> {

    public static final String TOPIC = "CURRENT_TOLL";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(CurrentTollStreamBuilder.class);

    public CurrentTollStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }


    public KStream<XwaySegmentDirection, CurrentToll> getStream(KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream,
                                                                KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream,
                                                                KStream<XwaySegmentDirection, Long> accidentDetectionStream) {
        logger.debug("Building stream to calculate the current toll on expressway, segent, direction..");
        return numberOfVehiclesStream
                .repartition(Repartitioned.<XwaySegmentDirection, NumberOfVehicles>with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("LAV_TOLL")))
                .join(latestAverageVelocityStream
                                .repartition(Repartitioned.<XwaySegmentDirection, AverageVelocity>with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("NOV_TOLL"))),
                        (value1, value2) -> new CurrentTollIntermediate(value2.getMinute(), value2.getAverageSpeed(), value1.getNumberOfVehicles()),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
                        StreamJoined.with(new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>()))
                .leftJoin(accidentDetectionStream,
                        (value1, value2) -> value1.setNoAccident(value2 == null),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
                        StreamJoined.with(new DefaultSerde<>(), new DefaultSerde<>(), new Serdes.LongSerde()))
                .filter((k, v) -> v.isTollApplicable())
                // otherwise calculate toll
                .mapValues(CurrentTollIntermediate::calculateCurrentToll);
        // no need to use the OnMinuteChangeEmitter, because source streams have already been reduced...
    }


    @Override
    public String getOutputTopic() {
        return TOPIC;
    }

    static class CurrentTollIntermediate extends Quartet<Long, Double, Integer, Boolean> implements TimedOnMinute {


        CurrentTollIntermediate(Long minute, Double averageVelocity, Integer numberOfVehicles) {
            super(minute, averageVelocity, numberOfVehicles, true);
        }

        CurrentTollIntermediate(Long minute, Double averageVelocity, Integer numberOfVehicles, Boolean noAccident) {
            super(minute, averageVelocity, numberOfVehicles, noAccident);
        }

        CurrentTollIntermediate setNoAccident(boolean noAccident) {
            return new CurrentTollIntermediate(this.getValue0(), this.getValue1(), this.getValue2(), noAccident);
        }

        /**
         * Calculates the current toll based on the information given in the object.
         *
         * @return the current toll.
         */
        CurrentToll calculateCurrentToll() {
            return new CurrentToll(this.getMinute() + 1, 2 * Math.pow(this.getNumberOfVehicles() - 50, 2), this.getAverageVelocity());
        }

        boolean isTollApplicable() {
            return this.hasNoAccident() && this.getAverageVelocity() < 40 && this.getNumberOfVehicles() > 50;
        }

        public long getMinute() {
            return getValue0();
        }

        double getAverageVelocity() {
            return getValue1();
        }

        int getNumberOfVehicles() {
            return getValue2();
        }

        boolean hasNoAccident() {
            return getValue3();
        }

    }

}
