package com.example.paymentservice.kafka.consumer;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.event.SagaEvent;
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
public class SagaEventConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "saga-events",
            groupId = "${spring.kafka.consumer.group-id:payment-service-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeSagaEvent(@Payload SagaEvent event,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                 @Header(KafkaHeaders.OFFSET) long offset,
                                 Acknowledgment ack) {

        log.info("Получено SagaEvent: eventType={}, orderId={}, step={}",
                event.getEventType(), event.getOrderId(), event.getStep());

        // Обрабатываем только команду на оплату
        if (!"PROCESS_PAYMENT".equals(event.getEventType())) {
            log.debug("Пропущено событие типа {}", event.getEventType());
            ack.acknowledge();
            return;
        }

        try {
            paymentService.processPayment(event);
            ack.acknowledge();
            log.info("SagaEvent подтверждён: orderId={}", event.getOrderId());
        } catch (Exception e) {
            log.error("Ошибка обработки SagaEvent: orderId={}", event.getOrderId(), e);
            // не ack — Kafka перечитает
        }
    }
}