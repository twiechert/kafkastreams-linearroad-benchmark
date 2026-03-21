package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.model.historical.DailyExpenditureRequest;
import org.apache.kafka.common.serialization.Serializer;
import static de.twiechert.linroad.kafka.stream.Util.pInt;
import static de.twiechert.linroad.kafka.stream.Util.pLng;

/**
 * This class creates a Kafka topic for the daily expenditure requests.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 *
 */
public class DailyExpenditureRequestHandler extends TupleHandler<DailyExpenditureRequest, Void> {

    public static final String TOPIC = "DAILYEXP";

    public DailyExpenditureRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context, 3);
    }

    @Override
    protected DailyExpenditureRequest transformKey(String[] tuple) {
        // consider strange field mapping in csv file --> there are a lot of dummy fields....
        return new DailyExpenditureRequest(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[9]), pInt(tuple[4]), pInt(tuple[14]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<DailyExpenditureRequest>> getKeySerializerClass() {
        return DailyExpenditureRequest.Serializer.class;
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
