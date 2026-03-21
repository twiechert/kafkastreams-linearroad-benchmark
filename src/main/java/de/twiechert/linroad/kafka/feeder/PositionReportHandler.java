package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.time.TupleTimestampExtractor;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TimestampExtractor;
import static de.twiechert.linroad.kafka.stream.Util.pInt;
import static de.twiechert.linroad.kafka.stream.Util.pLng;

/**
 * This class creates a Kafka topics for the position reports
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com> *
 */
public class PositionReportHandler extends TupleHandler<XwaySegmentDirection, PositionReport> {

    public static final String TOPIC = "POS";

    public PositionReportHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context, 0);
    }

    @Override
    protected XwaySegmentDirection transformKey(String[] tuple) {
        return new XwaySegmentDirection(pInt(tuple[4]), pInt(tuple[7]), tuple[6].equals("0") );
    }

    @Override
    protected PositionReport transformValue(String[] tuple) {
        return new PositionReport(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]), pInt(tuple[5]), pInt(tuple[8]));
    }

    @Override
    protected Class<? extends Serializer<XwaySegmentDirection>> getKeySerializerClass() {
        return XwaySegmentDirection.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<PositionReport>> getValueSerializerClass() {
        return PositionReport.Serializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }


    public static class TimeStampExtractor implements TimestampExtractor {

        private final TupleTimestampExtractor tupleTimestampExtractor = new TupleTimestampExtractor(TupleTimestampExtractor.KeyValue.Value, 0);

        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            return tupleTimestampExtractor.extract(record, partitionTime);
        }
    }
}
