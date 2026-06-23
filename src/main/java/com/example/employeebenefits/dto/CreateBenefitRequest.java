package com.example.employeebenefits.dto;

import java.math.BigDecimal;

public record CreateBenefitRequest(
        String requestId,
        String employeeId,
        String benefitType,
        BigDecimal requestedAmount
) {
}
