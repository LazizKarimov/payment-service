package com.example.paymentservice.kafka.consumer;

import com.example.paymentservice.dto.RefundPaymentCommand;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundPaymentCommandConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "payment-refund-commands",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "refundPaymentListenerContainerFactory"
    )
    public void onRefundPaymentCommand(@Payload RefundPaymentCommand command,
                                       Acknowledgment ack) {
        log.info("Получено RefundPaymentCommand: sagaId={}, paymentId={}, reason={}",
                command.sagaId(), command.paymentId(), command.reason());
        try {
            paymentService.refundPayment(command);
            ack.acknowledge();
            log.info("RefundPaymentCommand подтверждён: paymentId={}", command.paymentId());
        } catch (Exception e) {
            log.error("Ошибка обработки RefundPaymentCommand: paymentId={}",
                    command.paymentId(), e);
            // не ack — Kafka перечитает
        }
    }
}