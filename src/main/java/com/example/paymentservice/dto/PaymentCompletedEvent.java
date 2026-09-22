package com.example.paymentservice.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentCompletedEvent(
        UUID sagaId,
        UUID paymentId,
        UUID orderId,
        BigDecimal amount
) {}
