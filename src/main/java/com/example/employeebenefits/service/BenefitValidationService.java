package com.example.employeebenefits.service;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.exception.BusinessValidationException;
import com.example.employeebenefits.exception.ValidationServiceUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BenefitValidationService {

    public void validate(EmployeeBenefitEvent event) {
        log.info("Validating benefit request. requestId={}", event.requestId());

        // Demo triggers used to simulate different failure scenarios during training.
        if ("EMP-FAIL".equals(event.employeeId())
                || "SIMULATE-SERVICE-DOWN".equals(event.employeeId())) {
            log.error("Validation service unavailable for {}", event.requestId());
            throw new ValidationServiceUnavailableException(
                    "Validation service unavailable");
        }
        if ("EMP-INVALID".equals(event.employeeId())
                || "SIMULATE-BUSINESS-ERROR".equals(event.employeeId())) {
            log.error("Business validation failed for {}", event.requestId());
            throw new BusinessValidationException(
                    "Employee is not eligible for this benefit");
        }
    }
}
