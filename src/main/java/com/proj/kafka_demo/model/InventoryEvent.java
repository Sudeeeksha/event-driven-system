package com.proj.kafka_demo.model;

import java.time.LocalDateTime;

/**
 * Event published to the inventory-updates topic after
 * InventoryService reserves stock for an order.
 */
public class InventoryEvent {

    private String orderId;
    private String status; // RESERVED, OUT_OF_STOCK
    private LocalDateTime timestamp;

    public InventoryEvent() {
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "InventoryEvent{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
