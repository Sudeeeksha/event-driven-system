package com.proj.kafka_demo.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.UserRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Kafka Consumer service for processing user requests.
 * This service listens to the user-requests topic and processes incoming messages.
 *
 * Design Decision: Using @KafkaListener annotation for simplicity and automatic
 * container management. Spring Kafka handles the consumer lifecycle.
 *
 * Future Enhancement: Add error handling, dead letter queue, and manual offset
 * management for production-grade reliability.
 */
@Service
public class UserRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(UserRequestConsumer.class);

    private final ObjectMapper objectMapper;

    public UserRequestConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Listens to the user-requests topic and processes incoming messages.
     * 
     * @param message The JSON string message from Kafka
     * 
     * Design Decision: The groupId is defined in application.yml, but can be
     * overridden here if needed. Multiple consumers with the same groupId will
     * share the partitions, enabling horizontal scaling.
     * 
     */
    @KafkaListener(topics = "${kafka.topic.user-requests}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeUserRequest(String message) {
        try {
            UserRequest userRequest = objectMapper.readValue(message, UserRequest.class);
            
            log.info("Received Request:");
            log.info("User: {}", userRequest.getUsername());
            log.info("Action: {}", userRequest.getAction());
            log.info("Timestamp: {}", userRequest.getTimestamp());
            
        } catch (Exception e) {
            log.error("Error processing message: {}", message, e);
            // Future Enhancement: Send to dead letter queue for failed messages
        }
    }
}
