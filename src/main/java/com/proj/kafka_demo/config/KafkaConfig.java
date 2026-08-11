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

    /**
     * Creates the user-requests topic if it doesn't exist.
     * 
     * Topic Configuration:
     * - partitions: 1 - Single partition for this phase. Can be increased later
     *               for parallel processing when adding multiple consumers.
     * - replicationFactor: 1 - Single replica since we have one broker.
     *                      Should match number of brokers in production.
     * 
     * Future Enhancement: Increase partitions when adding multiple consumers
     * to enable parallel processing and improve throughput.
     */
    @Bean
    public NewTopic userRequestsTopic() {
        return TopicBuilder.name(userRequestsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
