package com.proj.kafka_demo.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.UserRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka Producer service for publishing user requests.
 * This service handles sending UserRequest objects to the Kafka topic.
 * 
 * Design Decision: Using String serialization for simplicity in Phase 1.
 * The UserRequest object is converted to JSON string before sending.
 * 
 * Future Enhancement: Use custom serializer (JsonSerializer) for direct
 * object serialization instead of manual JSON conversion.
 */
@Service
public class UserRequestProducer {

    private static final Logger log = LoggerFactory.getLogger(UserRequestProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.user-requests}")
    private String topic;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Publishes a UserRequest to the Kafka topic.
     * 
     * @param userRequest The user request to publish
     * @throws JsonProcessingException if JSON serialization fails
     * 
     * Design Decision: The method throws JsonProcessingException to handle
     * serialization errors at the controller level, allowing proper HTTP error responses.
     */
    public void sendUserRequest(UserRequest userRequest) throws JsonProcessingException {
        // Convert UserRequest object to JSON string
        String jsonMessage = objectMapper.writeValueAsString(userRequest);
        
        // Send to Kafka with userId as the key
        // Using userId as key ensures all requests from the same user go to the same partition
        // This maintains ordering per user when we add multiple partitions later
        kafkaTemplate.send(topic, userRequest.getUserId().toString(), jsonMessage);
        
        log.info("Message sent to Kafka topic: {}", topic);
    }
}
