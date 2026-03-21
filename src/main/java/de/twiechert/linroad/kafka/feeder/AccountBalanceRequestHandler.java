package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import org.apache.kafka.common.serialization.Serializer;
import static de.twiechert.linroad.kafka.stream.Util.pInt;
import static de.twiechert.linroad.kafka.stream.Util.pLng;

/**
 * This class creates a Kafka topics for the account balance request.
 *
 * Key corresponds to (Time: t, VID: v, QID: q).
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceRequestHandler extends TupleHandler<AccountBalanceRequest, Void> {

    public static final String TOPIC = "BALANCE";

    public AccountBalanceRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context, 2);
    }

    @Override
    protected AccountBalanceRequest transformKey(String[] tuple) {
        // consider strange field mapping in csv file --> there are a lot of dummy fields....
        return new AccountBalanceRequest(pLng(tuple[1]), pInt(tuple[3]), pInt(tuple[9]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<AccountBalanceRequest>> getKeySerializerClass() {
        return AccountBalanceRequest.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<Void>> getValueSerializerClass() {
        return Void.Serializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }


}
