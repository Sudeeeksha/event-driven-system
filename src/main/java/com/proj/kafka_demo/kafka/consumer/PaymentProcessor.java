package com.proj.kafka_demo.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proj.kafka_demo.model.Order;
import com.proj.kafka_demo.model.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

/**
 * Consumes order events under its own consumer group ("payments-svc") and
 * simulates a call to a payment gateway (Stripe, Razorpay, etc).
 *
 * Design Decision: group-id is hardcoded here rather than sourced from
 * application.yml because each consumer service in this project needs its
 * OWN distinct group. Reusing spring.kafka.consumer.group-id across
 * services would make them share one group and split the traffic between
 * them instead of each seeing every order.
 *
 * Note: Thread.sleep here simulates gateway latency for demo purposes only.
 * It blocks the polling thread for this listener container. If you need
 * higher throughput, increase concurrency via
 * @KafkaListener(..., concurrency = "3") once the topic has enough
 * partitions to support it (orders topic has 3 in this setup).
 */
@Service
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);
    private static final Random RANDOM = new Random();

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.payments}")
    private String paymentsTopic;

    public PaymentProcessor(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "payments-svc")
    public void processPayment(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            log.info("Processing payment for order {}, amount {}", order.getOrderId(), order.getTotalAmount());

            // Simulated gateway latency - demo only
            Thread.sleep(300 + RANDOM.nextInt(700));

            PaymentEvent event = new PaymentEvent();
            event.setOrderId(order.getOrderId());
            event.setStatus("SUCCESS");
            event.setTransactionId(UUID.randomUUID().toString());
            event.setTimestamp(LocalDateTime.now());

            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(paymentsTopic, order.getOrderId(), eventJson);

            log.info("Payment SUCCESS for order {}", order.getOrderId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Payment processing interrupted", e);
        } catch (Exception e) {
            log.error("Payment processing failed for message: {}", message, e);
            // Future Enhancement: publish to a payments-dlq topic after N retries
        }
    }
}
