package com.example.paymentservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentCompletedEvent {

    private UUID paymentId;
    private UUID orderId;
    private UUID customerId;
    private BigDecimal amount;
    private String status;       // "COMPLETED"
    private String eventType;    // "PAYMENT_COMPLETED"
    private Long timestamp;
}