package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.AccountBalanceRequestHandler;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


/**
 * This class reads the tuples from the account balance stream and returns the respective stream object for further usage.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceRequestStreamBuilder {

    private LinearRoadKafkaBenchmarkApplication.Context context;

    public AccountBalanceRequestStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
    }

    public KStream<AccountBalanceRequest, Void> getStream(StreamsBuilder builder) {
        return builder.stream(context.topic(AccountBalanceRequestHandler.TOPIC),
                Consumed.with(new AccountBalanceRequest.Serde(), new DefaultSerde<>()));
    }
}
