package com.example.paymentservice.configuration;

import com.example.paymentservice.dto.ProcessPaymentCommand;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;


import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, ProcessPaymentCommand> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(GROUP_ID_CONFIG, groupId);
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.paymentservice.dto");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ProcessPaymentCommand.class.getName());
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // ← ИМЯ БИНА ДОЛЖНО БЫТЬ kafkaListenerContainerFactory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProcessPaymentCommand> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ProcessPaymentCommand> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}