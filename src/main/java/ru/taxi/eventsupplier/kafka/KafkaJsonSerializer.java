package ru.taxi.eventsupplier.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.IOException;

public class KafkaJsonSerializer<T> extends JsonSerializer<T> {

    private final TypeReference<T> typeReference;

    public KafkaJsonSerializer(TypeReference<T> typeReference, ObjectMapper objectMapper) {
        super(objectMapper);
        this.typeReference = typeReference;
    }

    public byte[] serialize(String topic, T data) {
        try {
            byte[] result = null;
            if (data != null) {
                result = this.objectMapper.writerFor(typeReference).writeValueAsBytes(data);
            }
            return result;
        } catch (IOException ex) {
            throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", ex);
        }
    }
}
