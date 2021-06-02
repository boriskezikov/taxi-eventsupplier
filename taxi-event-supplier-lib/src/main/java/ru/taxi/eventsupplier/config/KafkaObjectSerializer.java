package ru.taxi.eventsupplier.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;
import ru.taxi.eventsupplier.model.TripFinishedEvent;

import java.io.IOException;
import java.util.Map;

@Component
public class KafkaObjectSerializer implements Serializer<TripFinishedEvent> {

    private ObjectMapper mapper;

    public KafkaObjectSerializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, TripFinishedEvent data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {

    }
}
