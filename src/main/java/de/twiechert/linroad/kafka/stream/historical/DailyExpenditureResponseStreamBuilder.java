package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.*;
import de.twiechert.linroad.kafka.stream.StreamBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;


/**
 * This stream represents the response of daily expenditure requests. In order to respond to these requests,
 * the actual request stream is joined with a table of expenditures per vehicle, xway and day.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class DailyExpenditureResponseStreamBuilder extends StreamBuilder<Void, DailyExpenditureResponse> {

    public final static String TOPIC = "DAILY_EXP_RESP";


    public DailyExpenditureResponseStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    public KStream<Void, DailyExpenditureResponse> getStream(KStream<DailyExpenditureRequest, Void> dailyExpenditureRequestStream,
                                                             KTable<XwayVehicleIdDay, Double> tollHistory) {

        /*
          "...such that Type identifies this tuple as an daily expenditure request, Time is the time of the request, VID is the vehicle making the request, QID is the query identifier,
          and XWay and Day (1 . . .69) identify the expressway and the day (1 is yesterday, 69 is 10 weeks ago) for which an expenditure total is desired. ."
         */
        KStream<XwayVehicleIdDay, DailyExpenditureRequest> accountBalanceRequestsPerVehicleXwayAndDay =
                dailyExpenditureRequestStream.map((k, v) -> new KeyValue<>(new XwayVehicleIdDay(k.getXWay(), k.getVehicleId(), k.getDay()), k))
                        .repartition(Repartitioned.with(new DefaultSerde<>(), new DefaultSerde<>()).withName(context.topic("ACC_BALANCE_PER_XWAY_VEH_DAY")));


        return accountBalanceRequestsPerVehicleXwayAndDay.leftJoin(tollHistory,
                (dayRequest, currToll) -> new DailyExpenditureResponse(dayRequest.getRequestTime(), LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(), dayRequest.getQueryId(), (currToll == null) ? 0 : currToll)
        )
                .selectKey((k, v) -> new Void());
    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }
}
