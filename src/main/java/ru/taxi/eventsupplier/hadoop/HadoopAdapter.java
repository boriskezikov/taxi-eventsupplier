package ru.taxi.eventsupplier.hadoop;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.taxi.eventsupplier.kafka.KafkaProducer;
import ru.taxi.eventsupplier.kafka.ProcessingStatus;
import ru.taxi.eventsupplier.kafka.TripFinishedEvent;

import java.util.Collections;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class HadoopAdapter {


    private final SparkSession sparkSession;
    private final KafkaProducer kafkaProducer;

    @Value("${hdfs.host}")
    private String hdfsHost;

    private void saveToHdfs(TripFinishedEvent recordToSave) {
        log.info("HadoopAdapter.saveToHdfs.in");
        var dataPath = hdfsHost + HadoopUtils.RAW_DATA_DRIVE + HadoopUtils.RAW_PREVIEW_FILENAME;
        Dataset<Row> csv = sparkSession.read().format("csv")
                .option("header", "true")
                .load(hdfsHost + HadoopUtils.RAW_DATA_DRIVE + HadoopUtils.RAW_PREVIEW_FILENAME);
        log.info("HadoopAdapter.saveToHdfs.in - actual data loaded from hdfs.");
        Row row = RowFactory.create(recordToSave);
        Dataset<Row> updatedDf = sparkSession.createDataFrame(Collections.singletonList(row), csv.schema());
        log.info("HadoopAdapter.saveToHdfs.in - actual data updated with new record.");
        csv.union(updatedDf).write().csv(dataPath);
        log.info("HadoopAdapter.saveToHdfs.out - actual data saved to hdfs");
    }

    public void process(TripFinishedEvent recordToSave, UUID rqId) {
        try {
            saveToHdfs(recordToSave);
        } catch (Exception e) {
            log.error("HadoopAdapter.process.in - saving to hdfs failed with reason: {}", e.getMessage());
            kafkaProducer.sendToFallbackTopic(recordToSave, rqId);
        }
        kafkaProducer.sentToStatusTopic(ProcessingStatus.PROCESSED, rqId);
    }
}
