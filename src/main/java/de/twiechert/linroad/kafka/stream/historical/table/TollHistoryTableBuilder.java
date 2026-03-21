package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.historical.TollHistoryRequestHandler;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleIdDay;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle, xway and day.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TollHistoryTableBuilder {


    public static final String TOPIC = "TOLL_HISTORY";
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public TollHistoryTableBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
    }

    public KTable<XwayVehicleIdDay, Double> getTable(StreamsBuilder builder) {
        // Read from the source topic as a table, then materialize to the fixed topic
        KTable<XwayVehicleIdDay, Double> sourceTable = builder.table(
                context.topic(TollHistoryRequestHandler.TOPIC),
                Consumed.with(new DefaultSerde<>(), new Serdes.DoubleSerde()));
        // Write to the fixed topic for later re-reading
        sourceTable.toStream().to(TOPIC, Produced.with(new DefaultSerde<>(), new Serdes.DoubleSerde()));
        // Return the table from the fixed topic
        return builder.table(TOPIC,
                Consumed.with(new DefaultSerde<>(), new Serdes.DoubleSerde()),
                Materialized.with(new DefaultSerde<>(), new Serdes.DoubleSerde()));
    }


    public KTable<XwayVehicleIdDay, Double> getExistingTable(StreamsBuilder builder) {
        return builder.table(TOPIC,
                Consumed.with(new DefaultSerde<>(), new Serdes.DoubleSerde()));
    }
}
