package com.example.employeebenefits.service;

import com.example.employeebenefits.domain.BenefitRequest;
import com.example.employeebenefits.domain.BenefitRequestStatus;
import com.example.employeebenefits.event.BenefitApprovedEvent;
import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.kafka.BenefitApprovedProducer;
import com.example.employeebenefits.repository.BenefitRequestRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BenefitService {

    private final BenefitRequestRepository benefitRequestRepository;
    private final BenefitApprovedProducer benefitApprovedProducer;

    public void approveBenefit(EmployeeBenefitEvent event) {
        Instant approvedAt = Instant.now();

        BenefitRequest request = BenefitRequest.builder()
                .requestId(event.requestId())
                .employeeId(event.employeeId())
                .benefitType(event.benefitType())
                .requestedAmount(event.requestedAmount())
                .requestedAt(event.requestedAt())
                .approvedAt(approvedAt)
                .status(BenefitRequestStatus.APPROVED)
                .build();

        benefitRequestRepository.save(request);

        BenefitApprovedEvent approvedEvent = new BenefitApprovedEvent(
                event.requestId(),
                event.employeeId(),
                event.benefitType(),
                event.requestedAmount(),
                approvedAt
        );

        benefitApprovedProducer.publish(approvedEvent);
    }
}
