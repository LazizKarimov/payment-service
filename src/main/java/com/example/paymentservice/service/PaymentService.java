package com.example.paymentservice.service;

import com.example.paymentservice.entity.Payment;
import com.example.paymentservice.entity.PaymentStatus;
import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.event.PaymentCompletedEvent;
import com.example.paymentservice.kafka.producer.PaymentEventProducer;
import com.example.paymentservice.repository.PaymentRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    @Transactional
    public void processPayment(OrderCreatedEvent event) {
        log.info("  Начало обработки платежа: orderId={}, amount={}",
                event.getId(), event.getAmount());

        // 0. Защита от повторной обработки (идемпотентность)
        if (paymentRepository.existsByOrderId(event.getId())) {
            log.warn("️ Платеж для заказа {} уже существует, пропускаем", event.getId());
            return;
        }

        // 1. Создаем платеж со статусом PENDING
        Payment payment = Payment.builder()
                .orderId(event.getId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .status(PaymentStatus.PENDING)
                .build();

        Payment savedPayment = paymentRepository.save(payment);
        log.info(" Платеж создан: id={}, status=PENDING", savedPayment.getId());

        // 2. Обрабатываем платеж (здесь может быть вызов платежного шлюза)
        processPaymentWithGateway(savedPayment);

        // 3. Меняем статус на COMPLETED
        savedPayment.setStatus(PaymentStatus.COMPLETED);
        Payment completedPayment = paymentRepository.save(savedPayment);
        log.info(" Платеж обработан: id={}, status=COMPLETED", completedPayment.getId());

        // 4. Отправляем PaymentCompleted в Kafka
        PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                .paymentId(completedPayment.getId())
                .orderId(completedPayment.getOrderId())
                .customerId(completedPayment.getCustomerId())
                .amount(completedPayment.getAmount())
                .status(PaymentStatus.COMPLETED.name())
                .eventType("PAYMENT_COMPLETED")
                .timestamp(System.currentTimeMillis())
                .build();

        paymentEventProducer.sendPaymentCompletedEvent(completedEvent);
        log.info(" Платеж полностью обработан и событие отправлено");
    }

    /**
     * Имитация обработки платежа через внешний шлюз.
     * В реальном проекте здесь вызов Stripe, PayPal и т.д.
     */
    private void processPaymentWithGateway(Payment payment) {
        log.info("  Отправка запроса в платежный шлюз для суммы: {}", payment.getAmount());

        // Имитация задержки
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Прервано во время обработки платежа", e);
        }

        // Проверка: сумма должна быть > 0
        if (payment.getAmount().doubleValue() <= 0) {
            throw new RuntimeException("Сумма платежа должна быть больше 0");
        }

        log.info(" Платежный шлюз подтвердил операцию");
    }
}