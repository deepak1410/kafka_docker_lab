package com.dpk.kafkadockerlab.configs;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Configuration
public class KafkaConfigAdv {

    private static Logger logger = LoggerFactory.getLogger(KafkaConfigAdv.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${topics.people-adv.name}")
    private String topicName;

    @Value("${topics.people-adv.partitions}")
    private int topicPartitions;

    @Value("${topics.people-adv.replicas}")
    private int topicReplicas;

    /**
     * Create Kafka topic
     */
    @Bean
    public NewTopic peopleAdvTopic() {
        logger.info("Create a people-adv Kafka topic");
        return TopicBuilder.name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

}
