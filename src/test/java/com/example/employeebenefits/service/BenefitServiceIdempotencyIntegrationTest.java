package com.example.employeebenefits.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.kafka.BenefitApprovedProducer;
import com.example.employeebenefits.repository.BenefitRequestRepository;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;

@DataJpaTest
@Import({BenefitService.class, BenefitValidationService.class})
@ExtendWith(OutputCaptureExtension.class)
class BenefitServiceIdempotencyIntegrationTest {

    @Autowired
    private BenefitService benefitService;

    @Autowired
    private BenefitRequestRepository benefitRequestRepository;

    @MockBean
    private BenefitApprovedProducer benefitApprovedProducer;

    @Test
    void duplicateRequestDoesNotCreateSecondRecordOrApprovalEvent(CapturedOutput output) {
        EmployeeBenefitEvent event = benefitEvent("REQ-1001", "EMP-100");

        benefitService.approveBenefit(event);
        benefitService.approveBenefit(event);

        assertThat(benefitRequestRepository.findAll())
                .hasSize(1)
                .singleElement()
                .satisfies(saved -> assertThat(saved.getRequestId()).isEqualTo("REQ-1001"));

        verify(benefitApprovedProducer, times(1)).publish(org.mockito.ArgumentMatchers.any());
        assertThat(output).contains("Duplicate request detected. requestId=REQ-1001");
    }

    private EmployeeBenefitEvent benefitEvent(String requestId, String employeeId) {
        return new EmployeeBenefitEvent(
                requestId,
                employeeId,
                "HEALTH",
                new BigDecimal("125.50"),
                Instant.parse("2026-06-23T10:00:00Z")
        );
    }
}
