package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.DailyExpenditureRequestHandler;
import de.twiechert.linroad.kafka.model.historical.DailyExpenditureRequest;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class reads the tuples from the daily expenditure request stream and returns the respective stream object for further usage.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class DailyExpenditureRequestStreamBuilder {

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public DailyExpenditureRequestStreamBuilder() {
    }

    public KStream<DailyExpenditureRequest, Void> getStream(StreamsBuilder builder) {
        return builder.stream(context.topic(DailyExpenditureRequestHandler.TOPIC),
                Consumed.with(new DefaultSerde<>(), new DefaultSerde<>()));
    }
}
