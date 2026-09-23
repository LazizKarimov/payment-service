package com.example.paymentservice.kafka.producer;

import com.example.paymentservice.dto.PaymentCompletedEvent;
import com.example.paymentservice.dto.PaymentRefundedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventProducer {

    private static final String TOPIC_NAME = "payment-events";
    private static final String REFUND_TOPIC = "payment-refunded-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendPaymentCompletedEvent(PaymentCompletedEvent event) {
        if (event == null) {
            log.warn("Попытка отправить null PaymentCompletedEvent");
            return;
        }

        kafkaTemplate.send(TOPIC_NAME, event.sagaId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PaymentCompletedEvent отправлен: sagaId={}, paymentId={}",
                                event.sagaId(), event.paymentId());
                        log.info("   Partition: {}, Offset: {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Ошибка отправки PaymentCompletedEvent: sagaId={}",
                                event.sagaId(), ex);
                    }
                });
    }

    public void sendPaymentRefundedEvent(PaymentRefundedEvent event) {
        if (event == null) {
            log.warn("Попытка отправить null PaymentRefundedEvent");
            return;
        }

        kafkaTemplate.send(REFUND_TOPIC, event.sagaId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PaymentRefundedEvent отправлен: sagaId={}, paymentId={}",
                                event.sagaId(), event.paymentId());
                        log.info("   Partition: {}, Offset: {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Ошибка отправки PaymentRefundedEvent: sagaId={}",
                                event.sagaId(), ex);
                    }
                });
    }
}