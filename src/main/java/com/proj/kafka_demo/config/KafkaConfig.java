package com.proj.kafka_demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka configuration class.
 * This class sets up Kafka topics and other Kafka-related configurations.
 * 
 * Design Decision: Creating topics programmatically ensures the topic exists
 * when the application starts, even if auto-creation is disabled in Kafka.
 * This makes the application self-contained and easier to deploy.
 */
@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.user-requests}")
    private String userRequestsTopic;

    @Bean
    public NewTopic userRequestsTopic() {
        return TopicBuilder.name(userRequestsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
