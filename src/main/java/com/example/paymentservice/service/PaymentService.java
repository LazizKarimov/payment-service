package com.example.paymentservice.service;

import com.example.paymentservice.dto.PaymentCompletedEvent;
import com.example.paymentservice.dto.ProcessPaymentCommand;
import com.example.paymentservice.entity.Payment;
import com.example.paymentservice.entity.PaymentStatus;
import com.example.paymentservice.kafka.producer.PaymentEventProducer;
import com.example.paymentservice.repository.PaymentRepository;
import org.springframework.transaction.annotation.Transactional;
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