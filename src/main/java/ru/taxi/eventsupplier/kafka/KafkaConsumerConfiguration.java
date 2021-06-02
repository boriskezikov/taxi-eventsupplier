package ru.taxi.eventsupplier.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;


@Slf4j
@RequiredArgsConstructor
@Configuration
@EnableKafka
public class KafkaConsumerConfiguration implements KafkaListenerConfigurer {

    private final LocalValidatorFactoryBean validator;

    @Value("${supplier_kafka.retry.backoff_policy.init_interval}")
    private Long initInterval;
    @Value("${supplier_kafka.retry.backoff_policy.max_interval}")
    private Long maxInterval;
    @Value("${supplier_kafka.retry.backoff_policy.multiplier}")
    private Double multiplier;
    @Value("${supplier_kafka.retry.retry_policy.max_attempts}")
    private Integer maxAttempts;
    @Value("${supplier_kafka.consumer.group-id}")
    private String groupId;
    @Value("${supplier_kafka.consumer.max-poll-interval-ms}")
    private String maxPollIntervalMs;
    @Value("${supplier_kafka.consumer.max-poll-records}")
    private String maxPollRecords;
    @Value("${supplier_kafka.consumer.session-timeout-ms}")
    private String sessionTimeoutMs;
    @Value("${supplier_kafka.consumer.heartbeat-interval-ms}")
    private String heartbeatIntervalMs;
    @Value("${supplier_kafka.bootstrap-servers}")
    protected String bootstrap;


    public Map<String, Object> props() {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule()
                .addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(this.validator);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaRetryListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setMessageConverter(new StringJsonMessageConverter(objectMapper()));
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props()));
        factory.setRetryTemplate(retryTemplate());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaMainListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setMessageConverter(new StringJsonMessageConverter(objectMapper()));
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props()));
        return factory;
    }

    protected RetryPolicy retryPolicy() {
        SimpleRetryPolicy policy = new SimpleRetryPolicy();
        policy.setMaxAttempts(maxAttempts);
        return policy;
    }

    protected BackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
        policy.setInitialInterval(initInterval);
        policy.setMultiplier(multiplier);
        policy.setMaxInterval(maxInterval);
        return policy;
    }

    protected RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setBackOffPolicy(backOffPolicy());
        template.setRetryPolicy(retryPolicy());
        template.setThrowLastExceptionOnExhausted(true);
        return template;
    }
}
