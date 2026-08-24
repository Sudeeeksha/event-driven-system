package com.proj.kafka_demo.model;

import java.time.LocalDateTime;

/**
 * Event published to the payments topic after PaymentProcessor
 * finishes handling an order.
 */
public class PaymentEvent {

    private String orderId;
    private String status; // SUCCESS, FAILED
    private String transactionId;
    private LocalDateTime timestamp;

    public PaymentEvent() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PaymentEvent{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
