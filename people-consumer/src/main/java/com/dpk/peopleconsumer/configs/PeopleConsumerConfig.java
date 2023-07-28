package com.dpk.peopleconsumer.configs;

import com.dpk.peopleconsumer.entities.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class PeopleConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${topics.people-basic.name}")
    private String peopleBasicTopic;

    @Value("${topics.people-adv.name}")
    private String peopleAdvTopic;

    public Map<String, Object> consumerBasicConfigs() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "people-basic.java.grp-0"
        );
    }

    public Map<String, Object> consumerAdvConfigs() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "people-adv.java.grp-0"
        );
    }

    @Bean
    public ConsumerFactory<String, Person> basicConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerBasicConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(Person.class, false) // UseHeaders is ignored
        );
    }

    @Bean
    public ConsumerFactory<String, Person> advConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerAdvConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(Person.class, false) // UseHeaders is ignored
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Person> personBasicListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Person>();
        factory.setConsumerFactory(basicConsumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Person> personAdvListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Person>();
        factory.setConsumerFactory(advConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

}
