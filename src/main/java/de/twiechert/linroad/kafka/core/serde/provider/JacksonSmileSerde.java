package de.twiechert.linroad.kafka.core.serde.provider;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * This Serde implementation uses Jackson with the Smile format adapter.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class JacksonSmileSerde<T extends Serializable> implements Serde<T> {

    private final Class<T> classOb;

    public JacksonSmileSerde(Class<T> classOb) {
        this.classOb = classOb;
    }

    @Override
    public Serializer<T> serializer() {
        return new SmileSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new SmileDeserializer<>(classOb);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    public static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper(new SmileFactory());
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        return mapper;
    }

    private static class SmileSerializer<T> implements Serializer<T> {
        private final ObjectMapper mapper = getObjectMapper();

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) return null;
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize", e);
            }
        }
    }

    private static class SmileDeserializer<T> implements Deserializer<T> {
        private final ObjectMapper mapper = getObjectMapper();
        private final Class<T> targetType;

        SmileDeserializer(Class<T> targetType) {
            this.targetType = targetType;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return mapper.readValue(data, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize", e);
            }
        }
    }
}
