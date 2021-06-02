package ru.taxi.eventsupplier.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StatusMessage {

    private UUID rqId;
    private ProcessingStatus processingStatus;
}
