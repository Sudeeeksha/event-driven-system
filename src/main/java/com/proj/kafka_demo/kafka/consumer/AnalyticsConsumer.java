package com.proj.kafka_demo.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Consumes order events under its own consumer group ("analytics-svc")
 * and re-publishes a copy to the analytics-events topic, which a
 * separate pipeline (Elasticsearch, a BI tool, etc.) can tail without
 * touching the operational orders topic at all.
 *
 * This demonstrates the core benefit of the fan-out pattern: this
 * consumer can lag arbitrarily far behind, or be down entirely, without
 * affecting payments, inventory, or notifications.
 */
@Service
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.analytics}")
    private String analyticsTopic;

    public AnalyticsConsumer(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "analytics-svc")
    public void recordEvent(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            log.info("Recording analytics event for order {}", order.getOrderId());

            // Forward the raw event as-is - a real pipeline might enrich
            // or reshape it first before landing it in Elasticsearch/BI.
            kafkaTemplate.send(analyticsTopic, order.getOrderId(), message);
        } catch (Exception e) {
            log.error("Analytics recording failed for message: {}", message, e);
        }
    }
}
