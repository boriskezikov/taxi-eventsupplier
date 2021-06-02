package ru.taxi.eventsupplier.config;

public class KafkaProperties extends org.springframework.kafka.support.KafkaHeaders {

    public static final String INGRESS_TRIP_FINISHED_TOPIC = "INGRESS_TRIP.FINISHED.RQ";
    public static final String INGRESS_TRIP_FINISHED_RETRY_TOPIC = "INGRESS_TRIP.FINISHED.RETRY.RQ";
    public static final String INGRESS_TRIP_STATUS_TOPIC = "INGRESS_TRIP.STATUS.RETRY.RQ";

    public static final String RQ_ID_HEADER = "messageRqId";


    private KafkaProperties() {
        throw new UnsupportedOperationException("Can not be initialized");
    }
}
