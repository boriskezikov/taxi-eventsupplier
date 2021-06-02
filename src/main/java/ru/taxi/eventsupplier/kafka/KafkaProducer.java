package ru.taxi.eventsupplier.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<UUID, Object> kafkaTemplate;


    public void sendToFallbackTopic(TripFinishedEvent tripFinishedEvent, UUID rqId) {
        ProducerRecord<UUID, Object> producerRecord = new ProducerRecord<>(KafkaUtils.INGRESS_TRIP_STATUS_TOPIC, 1, UUID.randomUUID(), tripFinishedEvent);
        producerRecord.headers().add("messageRqId", rqId.toString().getBytes());
        producerRecord.headers().add(KafkaUtils.ORIGIN_TOPIC, KafkaUtils.INGRESS_TRIP_FINISHED_TOPIC.getBytes(UTF_8));
        kafkaTemplate.send(producerRecord)
                .addCallback(
                        result -> log.warn("KafkaProducer.sendToFallbackTopic - Successfully delivered to RETRY topic: {}", result),
                        error -> {
                            throw new KafkaException("Error while producing to RETRY topic", error);
                        });
    }

    public void sentToStatusTopic(ProcessingStatus processingStatus, UUID rqId) {
        StatusMessage statusMessage = StatusMessage.builder().rqId(rqId).processingStatus(processingStatus).build();
        ProducerRecord<UUID, Object> producerRecord = new ProducerRecord<>(KafkaUtils.INGRESS_TRIP_STATUS_TOPIC, 1, rqId, statusMessage);
        kafkaTemplate.send(producerRecord)
                .addCallback(
                        result -> log.warn("KafkaProducer.sendToStatusTopic - Successfully delivered to STATUS topic: {}", result),
                        error -> {
                            throw new KafkaException("Error while producing to STATUS topic", error);
                        });
    }
}
