package de.twiechert.linroad.kafka.feeder.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.feeder.TupleHandler;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleIdDay;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serializer;
import static de.twiechert.linroad.kafka.stream.Util.pDob;
import static de.twiechert.linroad.kafka.stream.Util.pInt;

/**
 * This class handles a toll history tuple by sending it to the respective Kafka topic.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TollHistoryRequestHandler extends TupleHandler<XwayVehicleIdDay, Double> {

    public static final String TOPIC = "TOLL_HIST_TABLE";

    public TollHistoryRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    @Override
    protected XwayVehicleIdDay transformKey(String[] tuple) {
        return new XwayVehicleIdDay(pInt(tuple[2]), pInt(tuple[0]), pInt(tuple[1]));
    }

    @Override
    protected Double transformValue(String[] tuple) {
        return pDob(tuple[3]);
    }

    @Override
    protected Class<? extends Serializer<XwayVehicleIdDay>> getKeySerializerClass() {
        return XwayVehicleIdDay.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<Double>> getValueSerializerClass() {
        return DoubleSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }
}
