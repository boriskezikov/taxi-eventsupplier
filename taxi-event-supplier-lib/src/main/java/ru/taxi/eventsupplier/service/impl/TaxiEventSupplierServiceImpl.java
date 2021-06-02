package ru.taxi.eventsupplier.service.impl;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import ru.taxi.eventsupplier.config.KafkaNotificationsProducerConfig;
import ru.taxi.eventsupplier.config.KafkaProperties;
import ru.taxi.eventsupplier.model.TripFinishedEvent;
import ru.taxi.eventsupplier.service.TaxiEventSupplierService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class TaxiEventSupplierServiceImpl implements TaxiEventSupplierService {

    private static final Logger log = LoggerFactory.getLogger(TaxiEventSupplierServiceImpl.class);

    private KafkaNotificationsProducerConfig kafkaNotificationsProducerConfig;
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void send(TripFinishedEvent messageDelivery) {
        ProducerRecord<String, Object> producerRecord =
                new ProducerRecord<>(KafkaProperties.INGRESS_TRIP_FINISHED_TOPIC, getPartition(), getKey(), messageDelivery, getHeaders());

        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(producerRecord);

        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                onSuccessProducing(result);
            }

            @Override
            public void onFailure(Throwable ex) {
                onFailureProducing(ex);
            }
        });
    }

    protected List<Header> getHeaders() {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(KafkaProperties.RQ_ID_HEADER, UUID.randomUUID().toString().getBytes(UTF_8)));
        return headers;
    }


    protected Integer getPartition() {
        return null;
    }


    protected String getKey() {
        return kafkaNotificationsProducerConfig.getSystemSenderId();
    }


    protected void onSuccessProducing(SendResult<String, Object> success) {
        log.warn("Message success delivered: {}", success);
    }

    protected void onFailureProducing(Throwable failure) {
        log.error("Error while producing: {}", failure.getMessage());
    }


    public KafkaNotificationsProducerConfig getKafkaNotificationsProducerConfig() {
        return kafkaNotificationsProducerConfig;
    }

    @Autowired
    public void setKafkaNotificationsProducerConfig(KafkaNotificationsProducerConfig kafkaNotificationsProducerConfig) {
        this.kafkaNotificationsProducerConfig = kafkaNotificationsProducerConfig;
    }

    public KafkaTemplate<String, Object> getKafkaTemplate() {
        return kafkaTemplate;
    }

    @Autowired
    public void setKafkaTemplate(@Qualifier("taxi-supplier") KafkaTemplate<String, Object> notificationsOsKafkaTemplate) {
        this.kafkaTemplate = notificationsOsKafkaTemplate;
    }
}
