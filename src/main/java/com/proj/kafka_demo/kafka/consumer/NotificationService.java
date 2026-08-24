package com.proj.kafka_demo.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.NotificationEvent;
import com.proj.kafka_demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Consumes order events under its own consumer group ("notification-svc")
 * and simulates sending a confirmation email/SMS.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.notifications}")
    private String notificationsTopic;

    public NotificationService(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "notification-svc")
    public void sendNotification(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            log.info("Sending order confirmation to user {}", order.getUserId());

            // Simulated email/SMS provider call (SendGrid, Twilio, etc.) - demo only
            Thread.sleep(150);

            NotificationEvent event = new NotificationEvent();
            event.setOrderId(order.getOrderId());
            event.setChannel("EMAIL");
            event.setStatus("SENT");
            event.setTimestamp(LocalDateTime.now());

            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(notificationsTopic, order.getOrderId(), eventJson);

            log.info("Notification SENT for order {}", order.getOrderId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Notification interrupted", e);
        } catch (Exception e) {
            log.error("Notification failed for message: {}", message, e);
        }
    }
}
