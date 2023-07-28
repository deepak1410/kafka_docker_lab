package com.dpk.kafkadockerlab.configs;

import com.dpk.kafkadockerlab.entities.Person;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class PeopleProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    public Map<String, Object> producerConfigs(){
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.BATCH_SIZE_CONFIG, 2400,
                ProducerConfig.LINGER_MS_CONFIG, 600,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true, // This helps in avoiding duplicates
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1,
                ProducerConfig.RETRIES_CONFIG, 5
        );
    }

    @Bean
    public ProducerFactory<String, Person> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Person> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
