package com.example.paymentservice.kafka.consumer;

import com.example.paymentservice.dto.ProcessPaymentCommand;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCommandConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "payment-commands",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onProcessPaymentCommand(
            @Payload ProcessPaymentCommand command,
            ConsumerRecord<String, ProcessPaymentCommand> record,
            Acknowledgment ack) {

        log.info("Получено ProcessPaymentCommand: sagaId={}, orderId={}, customerId={}, amount={}",
                command.sagaId(), command.orderId(), command.customerId(), command.amount());

        try {
            paymentService.processPayment(command);
            ack.acknowledge();
            log.info("ProcessPaymentCommand подтверждён: sagaId={}, orderId={}",
                    command.sagaId(), command.orderId());
        } catch (Exception e) {
            log.error("Ошибка обработки ProcessPaymentCommand: sagaId={}, orderId={}, payload={}",
                    command.sagaId(), command.orderId(), command, e);
            // Не подтверждаем — Kafka перечитает сообщение согласно политике consumer'а.
        }
    }
}