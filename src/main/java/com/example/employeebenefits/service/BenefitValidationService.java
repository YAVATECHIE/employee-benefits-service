package com.example.employeebenefits.service;

import lombok.extern.slf4j.Slf4j;
import com.example.employeebenefits.event.EmployeeBenefitEvent;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BenefitValidationService {

    public void validate(EmployeeBenefitEvent event) {
        log.info("Calling validation service for {}", event.requestId());
        if ("EMP-FAIL".equals(event.employeeId())) {
            log.error("Validation service unavailable for {}", event.requestId());
            throw new RuntimeException(
                    "Validation service unavailable");
        }
    }
}