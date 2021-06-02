package ru.taxi.eventsupplier.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TripFinishedEvent {

    @JsonProperty("VendorID")
    private String vendor_id;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime tpep_pickup_datetime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime tpep_dropoff_datetime;

    private Integer passenger_count;
    private Double trip_distance;
    private Double pickup_longitude;
    private Double pickup_latitude;

    @JsonProperty("RateCodeID")
    private String rate_code_id;
    private String store_and_fwd_flag;
    private Double dropoff_longitude;
    private Double dropoff_latitude;
    private String payment_type;
    private Double fare_amount;
    private String extra;
    private String mta_tax;
    private Double tip_amount;
    private Double tolls_amount;
    private Double improvement_surcharge;
    private Double total_amount;

}
