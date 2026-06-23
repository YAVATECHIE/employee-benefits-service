package com.example.employeebenefits.dto;

import jakarta.persistence.Column;

import java.math.BigDecimal;

public record CreateBenefitRequest(
        @Column(unique = true)
        String requestId,
        String employeeId,
        String benefitType,
        BigDecimal requestedAmount
) {
}
