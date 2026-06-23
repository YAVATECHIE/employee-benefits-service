package com.example.employeebenefits.event;

import java.math.BigDecimal;
import java.time.Instant;

public record EmployeeBenefitEvent(
        String requestId,
        String employeeId,
        String benefitType,
        BigDecimal requestedAmount,
        Instant requestedAt
) {
}
