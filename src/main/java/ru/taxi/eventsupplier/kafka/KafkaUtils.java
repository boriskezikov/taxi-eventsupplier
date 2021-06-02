package ru.taxi.eventsupplier.kafka;

import lombok.experimental.UtilityClass;

@UtilityClass
public class KafkaUtils {

    public static final String INGRESS_TRIP_FINISHED_TOPIC = "INGRESS_TRIP.FINISHED.RQ";
    public static final String INGRESS_TRIP_FINISHED_RETRY_TOPIC = "INGRESS_TRIP.FINISHED.RETRY.RQ";
    public static final String INGRESS_TRIP_STATUS_TOPIC = "INGRESS_TRIP.STATUS.RETRY.RQ";


    public static final String ORIGIN_TOPIC = "ORIGIN_TOPIC";

}
