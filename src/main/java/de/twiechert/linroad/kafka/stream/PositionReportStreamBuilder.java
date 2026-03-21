package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class provides the Kafka position report topic as stream.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 *
 */
@Component
public class PositionReportStreamBuilder {

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public KStream<XwaySegmentDirection, PositionReport> getStream(StreamsBuilder builder) {
        return builder.stream(context.topic(PositionReportHandler.TOPIC), Consumed.with(new DefaultSerde<>(), new DefaultSerde<>()));
    }
}
