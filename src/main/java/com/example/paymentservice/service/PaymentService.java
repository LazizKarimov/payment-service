package com.example.paymentservice.service;

import com.example.paymentservice.dto.PaymentCompletedEvent;
import com.example.paymentservice.dto.PaymentRefundedEvent;
import com.example.paymentservice.dto.ProcessPaymentCommand;
import com.example.paymentservice.dto.RefundPaymentCommand;
import com.example.paymentservice.entity.Payment;
import com.example.paymentservice.entity.PaymentStatus;
import com.example.paymentservice.kafka.producer.PaymentEventProducer;
import com.example.paymentservice.repository.PaymentRepository;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    @Transactional
    public void processPayment(ProcessPaymentCommand command) {
        if (paymentRepository.existsByOrderId(command.orderId())) {
            log.warn("Платёж для заказа {} уже существует, пропускаем", command.orderId());
            return;
        }

        Payment payment = Payment.builder()
                .orderId(command.orderId())
                .customerId(command.customerId())
                .amount(command.amount())
                .status(PaymentStatus.PENDING)
                .build();

        Payment saved = paymentRepository.save(payment);
        processPaymentWithGateway(saved);
        saved.setStatus(PaymentStatus.COMPLETED);
        paymentRepository.save(saved);

        PaymentCompletedEvent completed = new PaymentCompletedEvent(
                command.sagaId(),
                saved.getId(),
                saved.getOrderId(),
                saved.getAmount()
        );
        paymentEventProducer.sendPaymentCompletedEvent(completed);
    }

    @Transactional
    public void refundPayment(RefundPaymentCommand command) {
        Payment payment = paymentRepository.findById(command.paymentId())
                .orElseThrow(() -> new RuntimeException(
                        "Платёж не найден: " + command.paymentId()));

        if (payment.getStatus() == PaymentStatus.REFUNDED) {
            log.warn("Платёж {} уже возвращён, пропускаем", payment.getId());
            return;
        }

        if (payment.getStatus() != PaymentStatus.COMPLETED) {
            log.warn("Платёж {} в статусе {}, нельзя вернуть",
                    payment.getId(), payment.getStatus());
            return;
        }

        payment.setStatus(PaymentStatus.REFUNDED);
        Payment saved = paymentRepository.save(payment);
        log.info("Платёж возвращён: id={}, status=REFUNDED", saved.getId());

        PaymentRefundedEvent event = new PaymentRefundedEvent(
                command.sagaId(),
                saved.getId(),
                saved.getOrderId(),
                saved.getAmount(),
                Instant.now()
        );
        paymentEventProducer.sendPaymentRefundedEvent(event);
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