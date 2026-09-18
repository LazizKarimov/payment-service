package com.example.paymentservice.service;

import com.example.paymentservice.entity.Payment;
import com.example.paymentservice.entity.PaymentStatus;
import com.example.paymentservice.event.PaymentCompletedEvent;
import com.example.paymentservice.event.SagaEvent;
import com.example.paymentservice.kafka.producer.PaymentEventProducer;
import com.example.paymentservice.repository.PaymentRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    @Transactional
    public void processPayment(SagaEvent event) {
        log.info("Начало обработки платежа: orderId={}, sagaId={}, amount={}",
                event.getOrderId(), event.getSagaId(), event.getAmount());

        if (event.getAmount() == null) {
            log.error("SagaEvent без amount: orderId={}, sagaId={}. Событие игнорируется",
                    event.getOrderId(), event.getSagaId());
            return;   // ack в consumer'е — да, ставим, чтобы не зацикливаться
        }

        if (paymentRepository.existsByOrderId(event.getOrderId())) {
            log.warn("Платёж для заказа {} уже существует, пропускаем", event.getOrderId());
            return;
        }

        Payment payment = Payment.builder()
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .status(PaymentStatus.PENDING)
                .build();

        Payment savedPayment = paymentRepository.save(payment);
        log.info("Платёж создан: id={}, status=PENDING", savedPayment.getId());

        processPaymentWithGateway(savedPayment);

        savedPayment.setStatus(PaymentStatus.COMPLETED);
        Payment completedPayment = paymentRepository.save(savedPayment);
        log.info("Платёж обработан: id={}, status=COMPLETED", completedPayment.getId());

        PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                .paymentId(completedPayment.getId())
                .orderId(completedPayment.getOrderId())
                .amount(completedPayment.getAmount())
                .status(PaymentStatus.COMPLETED.name())
                .eventType("PAYMENT_COMPLETED")
                .timestamp(System.currentTimeMillis())
                .build();

        paymentEventProducer.sendPaymentCompletedEvent(completedEvent);
        log.info("Платёж полностью обработан и событие отправлено");
    }

    private void processPaymentWithGateway(Payment payment) {
        log.info("Отправка запроса в платёжный шлюз для суммы: {}", payment.getAmount());
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Прервано во время обработки платежа", e);
        }
        if (payment.getAmount().doubleValue() <= 0) {
            throw new RuntimeException("Сумма платежа должна быть больше 0");
        }
        log.info("Платёжный шлюз подтвердил операцию");
    }
}