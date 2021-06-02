package ru.taxi.eventsupplier.hadoop;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spring.application.name}")
    private String applicationName;
    @Value("${spark.driver.maxResultSize}")
    private String driverMaxResultSize;
    @Value("${spark.executor.maxResultSize}")
    private String executorMaxResultSize;
    @Value("${spark.sql.execution.arrow.pyspark.enabled}")
    private String arrowEnabled;
    @Value("${spark.sql.execution.arrow.pyspark.fallback.enabled}")
    private String arrowFallbackEnabled;
    @Value("${spark.master}")
    private String masterCoresNumber;

    @Bean
    public SparkSession configureSparkSession() {
        return SparkSession.builder()
                .appName(applicationName)
                .config("spark.driver.maxResultSize", driverMaxResultSize)
                .config("spark.executor.maxResultSize", executorMaxResultSize)
                .config("spark.sql.execution.arrow.pyspark.enabled", arrowEnabled)
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", arrowFallbackEnabled)
                .master(masterCoresNumber).getOrCreate();
    }
}
