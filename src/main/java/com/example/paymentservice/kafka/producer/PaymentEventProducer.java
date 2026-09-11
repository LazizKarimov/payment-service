package com.example.paymentservice.kafka.producer;

import com.example.paymentservice.event.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;

    private static final String TOPIC_NAME = "payment-events";

    public void sendPaymentCompletedEvent(PaymentCompletedEvent event) {
        if (event == null) {
            log.warn("Попытка отправить null событие");
            return;
        }

        CompletableFuture<SendResult<String, PaymentCompletedEvent>> future =
                kafkaTemplate.send(TOPIC_NAME, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info(" PaymentCompletedEvent отправлен: {}", event);
                log.info("   Partition: {}, Offset: {}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error(" Ошибка отправки PaymentCompletedEvent: {}", event, ex);
            }
        });
    }
}