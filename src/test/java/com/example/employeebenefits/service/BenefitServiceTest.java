package com.example.employeebenefits.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.employeebenefits.domain.BenefitRequest;
import com.example.employeebenefits.domain.BenefitRequestStatus;
import com.example.employeebenefits.event.BenefitApprovedEvent;
import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.kafka.BenefitApprovedProducer;
import com.example.employeebenefits.repository.BenefitRequestRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BenefitServiceTest {

    @Mock
    private BenefitRequestRepository benefitRequestRepository;

    @Mock
    private BenefitApprovedProducer benefitApprovedProducer;

    @Mock
    private BenefitValidationService benefitValidationService;

    private BenefitService benefitService;

    @BeforeEach
    void setUp() {
        benefitService = new BenefitService(
                benefitRequestRepository,
                benefitApprovedProducer,
                benefitValidationService
        );
    }

    @Test
    void approvesAndSavesNewRequest() {
        EmployeeBenefitEvent event = benefitEvent("REQ-1001", "EMP-100");
        when(benefitRequestRepository.findByRequestId("REQ-1001")).thenReturn(Optional.empty());

        benefitService.approveBenefit(event);

        ArgumentCaptor<BenefitRequest> requestCaptor = ArgumentCaptor.forClass(BenefitRequest.class);
        verify(benefitRequestRepository).save(requestCaptor.capture());

        BenefitRequest savedRequest = requestCaptor.getValue();
        assertThat(savedRequest.getRequestId()).isEqualTo("REQ-1001");
        assertThat(savedRequest.getEmployeeId()).isEqualTo("EMP-100");
        assertThat(savedRequest.getBenefitType()).isEqualTo("HEALTH");
        assertThat(savedRequest.getRequestedAmount()).isEqualByComparingTo("125.50");
        assertThat(savedRequest.getStatus()).isEqualTo(BenefitRequestStatus.APPROVED);
        assertThat(savedRequest.getApprovedAt()).isNotNull();
    }

    @Test
    void skipsDuplicateRequest() {
        EmployeeBenefitEvent event = benefitEvent("REQ-1001", "EMP-100");
        when(benefitRequestRepository.findByRequestId("REQ-1001"))
                .thenReturn(Optional.of(BenefitRequest.builder().requestId("REQ-1001").build()));

        benefitService.approveBenefit(event);

        verify(benefitValidationService, never()).validate(any());
        verify(benefitRequestRepository, never()).save(any());
        verify(benefitApprovedProducer, never()).publish(any());
    }

    @Test
    void throwsWhenValidationFails() {
        EmployeeBenefitEvent event = benefitEvent("REQ-1001", "EMP-FAIL");
        when(benefitRequestRepository.findByRequestId("REQ-1001")).thenReturn(Optional.empty());
        org.mockito.Mockito.doThrow(new RuntimeException("Validation service unavailable"))
                .when(benefitValidationService)
                .validate(event);

        assertThatThrownBy(() -> benefitService.approveBenefit(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Validation service unavailable");

        verify(benefitRequestRepository, never()).save(any());
        verify(benefitApprovedProducer, never()).publish(any());
    }

    @Test
    void publishesBenefitApprovedEventAfterSuccessfulProcessing() {
        EmployeeBenefitEvent event = benefitEvent("REQ-1001", "EMP-100");
        when(benefitRequestRepository.findByRequestId("REQ-1001")).thenReturn(Optional.empty());

        benefitService.approveBenefit(event);

        ArgumentCaptor<BenefitApprovedEvent> eventCaptor = ArgumentCaptor.forClass(BenefitApprovedEvent.class);
        verify(benefitApprovedProducer).publish(eventCaptor.capture());

        BenefitApprovedEvent approvedEvent = eventCaptor.getValue();
        assertThat(approvedEvent.requestId()).isEqualTo("REQ-1001");
        assertThat(approvedEvent.employeeId()).isEqualTo("EMP-100");
        assertThat(approvedEvent.benefitType()).isEqualTo("HEALTH");
        assertThat(approvedEvent.approvedAmount()).isEqualByComparingTo("125.50");
        assertThat(approvedEvent.approvedAt()).isNotNull();
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
