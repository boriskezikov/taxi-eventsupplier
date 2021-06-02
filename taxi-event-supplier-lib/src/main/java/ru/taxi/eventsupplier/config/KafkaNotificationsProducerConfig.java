package ru.taxi.eventsupplier.config;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ComponentScan({"ru.taxi.eventsupplier"})
public class KafkaNotificationsProducerConfig {

    @Value("${taxi-supplier.bootstrap-server}")
    private String bootstrapServer;
    @Value("${taxi-supplier.system-sender-id}")
    private String systemSenderId;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaObjectSerializer.class);

        return properties;
    }


    @Bean
    @Qualifier(value = "taxi-supplier")
    public KafkaTemplate<String, Object> notificationsOsKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
