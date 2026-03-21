package de.twiechert.linroad.kafka.stream.processor;

import de.twiechert.linroad.kafka.model.TimedOnMinute;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Kafka Streams does not support windowing as known in Flink. In addition to the final value of a window-based aggregation, Kafka will emmit
 * also all intermediate aggregate values. This behaviour in a lot cases undesired.
 *
 * This processor filters a stream, such that the latest value of a certain key seen at minute m is emitted, if an element from minute m+1 (same key) is observed.
 * This is used for the stream of current tolls {@link de.twiechert.linroad.kafka.stream.CurrentTollStreamBuilder}, to get only one toll, the latest aggregate seen at minute m each.
 *
 *
 * This implementation does not work for sliding windows, because windows overlap and the logical applied here would lead to wrong results.
 */
public class OnMinuteChangeEmitter {
    private static final Logger logger = LoggerFactory
            .getLogger(OnMinuteChangeEmitter.class);


    public static <K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> KStream<K, V> getForWindowed(KStream<K, V> sourceStream,
                                                                                                       Serde<K> keySerde,
                                                                                                       Serde<V> valueSerde,
                                                                                                       String storeName) {
        StoreBuilder<KeyValueStore<TimedKey<K>, V>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>(),
                valueSerde
        );

        OnMinuteChangeWindowedEmmitProcessorSupplier<K, V> punctuateSupplier = new OnMinuteChangeWindowedEmmitProcessorSupplier<>(storeName);

        return sourceStream.process(punctuateSupplier, storeName);
    }

    public static <K, V extends TimedOnMinute> KStream<K, V> get(KStream<K, V> sourceStream,
                                                                 Serde<K> keySerde,
                                                                 Serde<V> valueSerde,
                                                                 String storeName) {
        StoreBuilder<KeyValueStore<TimedKey<K>, V>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>(),
                valueSerde
        );

        OnMinuteChangeEmmitProcessorSupplier<K, V> punctuateSupplier = new OnMinuteChangeEmmitProcessorSupplier<>(storeName);

        return sourceStream.process(punctuateSupplier, storeName);
    }


    public static class OnMinuteChangeWindowedEmmitProcessorSupplier<K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> implements ProcessorSupplier<K, V, K, V> {

        private final String storeName;

        public OnMinuteChangeWindowedEmmitProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public Processor<K, V, K, V> get() {
            return new OnMinuteChangeWindowedEmmitProcessor<>(storeName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<TimedKey<K>, V>> storeBuilder = Stores.<TimedKey<K>, V>keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(storeName),
                    new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>(),
                    new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>()
            );
            return Collections.singleton(storeBuilder);
        }
    }

    private static class OnMinuteChangeWindowedEmmitProcessor<K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> implements Processor<K, V, K, V> {
        private ProcessorContext<K, V> context;
        private KeyValueStore<TimedKey<K>, V> kvStore;
        private final String storeName;
        private static final int checkUpTo = 2;

        public OnMinuteChangeWindowedEmmitProcessor(String storename) {
            this.storeName = storename;
        }


        @Override
        public void init(ProcessorContext<K, V> context) {
            this.context = context;
            kvStore = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<K, V> record) {
            K key = record.key();
            V value = record.value();
            kvStore.put(new TimedKey<>(key, value.getWindowEndMinute()), value);
            synchronized (this) {
                for (int i = 1; i <= checkUpTo; i++) {
                    TimedKey<K> oldKey = new TimedKey<>(key, value.getMinute() - i);
                    V oldVal = kvStore.get(oldKey);
                    if (oldVal != null) {
                        context.forward(new Record<>(oldKey.getKey(), oldVal, record.timestamp()));
                        kvStore.delete(oldKey);
                    }
                }
            }
        }

        @Override
        public void close() {
            kvStore.close();
        }
    }

    public static class OnMinuteChangeEmmitProcessorSupplier<K, V extends TimedOnMinute> implements ProcessorSupplier<K, V, K, V> {

        private final String storeName;

        public OnMinuteChangeEmmitProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public Processor<K, V, K, V> get() {
            return new OnMinuteChangeEmmitProcessor<>(storeName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<TimedKey<K>, V>> storeBuilder = Stores.<TimedKey<K>, V>keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(storeName),
                    new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>(),
                    new de.twiechert.linroad.kafka.core.serde.DefaultSerde<>()
            );
            return Collections.singleton(storeBuilder);
        }
    }

    private static class OnMinuteChangeEmmitProcessor<K, V extends TimedOnMinute> implements Processor<K, V, K, V> {
        private ProcessorContext<K, V> context;
        private KeyValueStore<TimedKey<K>, V> kvStore;
        private final String storeName;
        private static final int checkUpTo = 2;

        public OnMinuteChangeEmmitProcessor(String storename) {
            this.storeName = storename;
        }


        @Override
        public void init(ProcessorContext<K, V> context) {
            this.context = context;
            kvStore = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<K, V> record) {
            K key = record.key();
            V value = record.value();
            kvStore.put(new TimedKey<>(key, value.getMinute()), value);
            synchronized (this) {
                for (int i = 1; i <= checkUpTo; i++) {
                    TimedKey<K> oldKey = new TimedKey<>(key, value.getMinute() - i);
                    V oldVal = kvStore.get(oldKey);
                    if (oldVal != null) {
                        context.forward(new Record<>(oldKey.getKey(), oldVal, record.timestamp()));
                        kvStore.delete(oldKey);
                    }
                }
            }
        }

        @Override
        public void close() {
            kvStore.close();
        }
    }


}
