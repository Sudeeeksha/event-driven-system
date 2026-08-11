package com.proj.kafka_demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.proj.kafka_demo.kafka.producer.UserRequestProducer;
import com.proj.kafka_demo.model.UserRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST Controller for handling user requests.
 * This controller receives HTTP requests and publishes them to Kafka asynchronously.
 * 
 * Design Decision: Returns HTTP 202 Accepted immediately without waiting for
 * processing. This demonstrates the asynchronous nature of the system.
 * 
 */
@RestController
@RequestMapping("/requests")
public class UserRequestController {

    private static final Logger log = LoggerFactory.getLogger(UserRequestController.class);

    private final UserRequestProducer userRequestProducer;

    public UserRequestController(UserRequestProducer userRequestProducer) {
        this.userRequestProducer = userRequestProducer;
    }

    /**
     * POST endpoint to receive user requests.
     * 
     * @param userRequest The user request from the client
     * @return ResponseEntity with HTTP 202 Accepted
     * 
     * Design Decision: Using constructor injection for better testability
     * and immutability. This is a Spring Boot best practice.
     * 
     * The request is published to Kafka and the method returns immediately.
     * The actual processing happens asynchronously in the consumer.
     */
    @PostMapping
    public ResponseEntity<String> createUserRequest(@RequestBody UserRequest userRequest) {
        try {
            log.info("Received request: {}", userRequest);
            
            userRequestProducer.sendUserRequest(userRequest);

            return ResponseEntity
                    .status(HttpStatus.ACCEPTED)
                    .body("Request accepted for processing");
            
        } catch (JsonProcessingException e) {
            log.error("Error serializing user request", e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing request");
        }
    }
}
