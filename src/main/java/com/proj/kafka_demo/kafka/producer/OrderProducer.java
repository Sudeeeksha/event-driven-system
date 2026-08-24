package com.proj.kafka_demo.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes Order events to the orders topic.
 *
 * Design Decision: orderId is used as the partition key so every event
 * for a given order (and any future events that reference it) can be
 * routed consistently if needed.
 */
@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishOrder(Order order) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(order);

        kafkaTemplate.send(ordersTopic, order.getOrderId(), json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish order {}", order.getOrderId(), ex);
                    } else {
                        log.info("Order {} published to partition {}",
                                order.getOrderId(),
                                result.getRecordMetadata().partition());
                    }
                });
    }
}
