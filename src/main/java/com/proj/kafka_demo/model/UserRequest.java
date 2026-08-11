package com.proj.kafka_demo.model;

import java.time.LocalDateTime;

/**
 * Model class representing a user request.
 * This class will be serialized to JSON and sent to Kafka.
 */
public class UserRequest {

    private Long userId;
    private String username;
    private String action;
    private LocalDateTime timestamp;

    // Default constructor for JSON deserialization
    public UserRequest() {
    }

    // Constructor with fields
    public UserRequest(Long userId, String username, String action) {
        this.userId = userId;
        this.username = username;
        this.action = action;
        this.timestamp = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserRequest{" +
                "userId=" + userId +
                ", username='" + username + '\'' +
                ", action='" + action + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
