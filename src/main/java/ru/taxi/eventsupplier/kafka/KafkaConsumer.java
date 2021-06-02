package ru.taxi.eventsupplier.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import ru.taxi.eventsupplier.hadoop.HadoopAdapter;

import java.util.UUID;

import static java.util.Objects.nonNull;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumer {

    private final HadoopAdapter hadoop;


    @KafkaListener(topics = KafkaUtils.INGRESS_TRIP_FINISHED_TOPIC, containerFactory = "kafkaMainListenerContainerFactory")
    public void receive(@Validated @Payload TripFinishedEvent tripFinishedEvent,
                        @Header("messageRqId") UUID rqId) {
        log.info("KafkaConsumer.receive.in - message received");
        hadoop.process(tripFinishedEvent, rqId);
        log.info("KafkaConsumer.receive.out - message processed");

    }

    @KafkaListener(topics = KafkaUtils.INGRESS_TRIP_FINISHED_RETRY_TOPIC, containerFactory = "kafkaRetryListenerContainerFactory")
    public void retry(@Validated @Payload TripFinishedEvent tripFinishedEvent,
                      @Header("messageRqId") UUID rqId,
                      @Header(KafkaUtils.ORIGIN_TOPIC) String origin) {
        if (nonNull(origin) && KafkaUtils.INGRESS_TRIP_FINISHED_TOPIC.equals(origin)) {
            hadoop.process(tripFinishedEvent, rqId);
        } else {
            throw new IllegalArgumentException("notifications() - unable to read DLQ message because it's missing the originalTopic header");
        }
    }
}
