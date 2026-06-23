package com.example.employeebenefits.event;

import java.math.BigDecimal;
import java.time.Instant;

public record BenefitApprovedEvent(
        String requestId,
        String employeeId,
        String benefitType,
        BigDecimal approvedAmount,
        Instant approvedAt
) {
}
