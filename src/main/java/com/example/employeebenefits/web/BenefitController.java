package com.example.employeebenefits.web;

import com.example.employeebenefits.dto.CreateBenefitRequest;
import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.kafka.EmployeeBenefitProducer;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/benefits")
@RequiredArgsConstructor
public class BenefitController {

    private final EmployeeBenefitProducer employeeBenefitProducer;

    @PostMapping
    public ResponseEntity<Void> createBenefit(@RequestBody CreateBenefitRequest request) {
        EmployeeBenefitEvent event = new EmployeeBenefitEvent(
                request.requestId(),
                request.employeeId(),
                request.benefitType(),
                request.requestedAmount(),
                Instant.now()
        );

        employeeBenefitProducer.publish(event);
        return ResponseEntity.accepted().build();
    }
}
