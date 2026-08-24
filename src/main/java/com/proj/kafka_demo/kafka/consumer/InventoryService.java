package com.proj.kafka_demo.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.InventoryEvent;
import com.proj.kafka_demo.model.Order;
import com.proj.kafka_demo.model.OrderItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Consumes order events under its own consumer group ("inventory-svc")
 * and reserves stock in Redis, independently of payment processing.
 *
 * Design Decision: uses StringRedisTemplate, which Spring Boot
 * auto-configures once spring-boot-starter-data-redis is on the
 * classpath - no manual bean needed. See README for the dependency
 * to add to your pom.xml.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.inventory}")
    private String inventoryTopic;

    public InventoryService(ObjectMapper objectMapper,
                             StringRedisTemplate redisTemplate,
                             KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "inventory-svc")
    public void reserveInventory(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            log.info("Reserving inventory for order {}", order.getOrderId());

            if (order.getItems() != null) {
                for (OrderItem item : order.getItems()) {
                    String stockKey = "stock:" + item.getProductId();
                    redisTemplate.opsForValue().decrement(stockKey, item.getQuantity());
                }
            }

            InventoryEvent event = new InventoryEvent();
            event.setOrderId(order.getOrderId());
            event.setStatus("RESERVED");
            event.setTimestamp(LocalDateTime.now());

            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(inventoryTopic, order.getOrderId(), eventJson);

            log.info("Inventory RESERVED for order {}", order.getOrderId());
        } catch (Exception e) {
            log.error("Inventory reservation failed for message: {}", message, e);
        }
    }
}
