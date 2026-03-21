package de.twiechert.linroad.kafka.core.time;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This class represents is used as a base class of the project's common timestamp extractor.
 * It will fallback to wallclock time, if the realization will throw an exception.
 *
 */
public abstract class FallbackTimestampExtractor implements TimestampExtractor {


    private final WallclockTimestampExtractor wallclockTimestampExtractor = new WallclockTimestampExtractor();


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            return this.extractTimestamp(record);
        } catch (Exception e) {

            return wallclockTimestampExtractor.extract(record, partitionTime);
        }
    }

    public abstract long extractTimestamp(ConsumerRecord<Object, Object> record);

}
