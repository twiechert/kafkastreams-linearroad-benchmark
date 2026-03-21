package de.twiechert.linroad.kafka.core.serde.provider;



import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * This Serde implementation uses Kryio as library.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class KryoSerde<T extends Serializable> implements Serde<T> {


    private final Class<T> classOb;

    public KryoSerde(Class<T> classOb) {
        this.classOb = classOb;
    }


    public static class KryoSerializer<A> implements Serializer<A> {


        private final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
        });


        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String s, A a) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (Output output = new Output(baos)) {
                kryos.get().writeObject(output, a);
                output.flush();
            }
            return baos.toByteArray();
        }

        @Override
        public void close() {

        }
    }

    public static class KryoDeserializer<A> implements Deserializer<A> {


        private final Class<A> classOb;

        private final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
        });

        public KryoDeserializer(Class<A> classOb) {
            this.classOb = classOb;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public A deserialize(String s, byte[] bytes) {
            return kryos.get().readObject(new ByteBufferInput(bytes), classOb);
        }

        @Override
        public void close() {

        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new KryoSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new KryoDeserializer<>(this.classOb);
    }
}
