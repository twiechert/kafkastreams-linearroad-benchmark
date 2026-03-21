package de.twiechert.linroad.kafka.core.serde.provider;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

/**
 * @deprecated Use KryoSerde or JacksonSmileSerde instead.
 */
@Deprecated
public class ByteArraySerde<T extends Serializable> implements Serde<T> {

    @Deprecated
    public static class BArraySerializer<A> implements Serializer<A> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public byte[] serialize(String s, A a) {
            if (a == null) return null;
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(a);
                return bos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize", e);
            }
        }

        @Override
        public void close() {
        }
    }

    @Deprecated
    public static class BArrayDeserializer<A> implements Deserializer<A> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        @SuppressWarnings("unchecked")
        public A deserialize(String s, byte[] bytes) {
            if (bytes == null) return null;
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                 ObjectInputStream ois = new ObjectInputStream(bis)) {
                return (A) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize", e);
            }
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
        return new BArraySerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new BArrayDeserializer<>();
    }
}
