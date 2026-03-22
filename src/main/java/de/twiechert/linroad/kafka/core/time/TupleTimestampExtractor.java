package de.twiechert.linroad.kafka.core.time;

import de.twiechert.linroad.kafka.model.TimedOnMinute;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Tuple;

/**
 * This class is able to extract timestamps from tuples.
 * For that purpose you specify, if the timestamp is read from the key or value and the position within the tuple (beginning with 0)
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TupleTimestampExtractor extends FallbackTimestampExtractor implements TimestampExtractor {


    private final KeyValue keyValue;

    private final int pos;

    /**
     *
     * @param keyValue the part of the message to extract the timestamp from
     * @param pos the position to extract the timestamp from
     */
    public TupleTimestampExtractor(KeyValue keyValue, int pos) {
        this.keyValue = keyValue;
        this.pos = pos;
    }

    public enum KeyValue {
        Key, Value
    }


    @Override
    public long extractTimestamp(ConsumerRecord<Object, Object> record) {
        if(keyValue.equals(KeyValue.Key)) {
            return this.extract(record.key());

        } else {
            return this.extract(record.value());

        }
    }

    private long extract(Object record) {
        /*
          DO NOT CHANGE ORDER!! TUPLE EXTRACTION HAS PRESEDENCE
          Linear Road timestamps are in seconds — Kafka requires milliseconds.
         */
        if (record instanceof Tuple)
            return (long) ((Tuple) record).getValue(pos) * 1000L;

        else if (record instanceof TimedOnMinute) {
            // Minutes are already logical units, convert to ms epoch
            return ((TimedOnMinute) record).getMinute() * 60_000L;
        } else return (long) record * 1000L;
    }
}
