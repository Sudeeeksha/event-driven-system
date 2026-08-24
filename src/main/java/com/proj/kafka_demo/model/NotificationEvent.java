package com.proj.kafka_demo.model;

import java.time.LocalDateTime;

/**
 * Event published to the notifications topic after
 * NotificationService sends a confirmation to the customer.
 */
public class NotificationEvent {

    private String orderId;
    private String channel; // EMAIL, SMS, PUSH
    private String status;  // SENT, FAILED
    private LocalDateTime timestamp;

    public NotificationEvent() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "NotificationEvent{" +
                "orderId='" + orderId + '\'' +
                ", channel='" + channel + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
