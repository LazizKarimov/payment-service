package com.example.paymentservice.kafka.consumer;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "order-events",
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("   Получено событие из Kafka:");
        log.info("   Partition: {}, Offset: {}", partition, offset);
        log.info("   OrderId: {}, Amount: {}", event.getOrderId(), event.getAmount());

        try {
            paymentService.processPayment(event);
            ack.acknowledge();
            log.info(" Событие подтверждено");
        } catch (Exception e) {
            log.error(" Ошибка обработки события: orderId={}", event.getOrderId(), e);
            // Не подтверждаем — Kafka перечитает сообщение
        }
    }
}