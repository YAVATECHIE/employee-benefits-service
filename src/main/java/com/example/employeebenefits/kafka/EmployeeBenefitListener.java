package com.example.employeebenefits.kafka;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.service.BenefitService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeBenefitListener {

    private final BenefitService benefitService;

    @KafkaListener(
            topics = "${app.kafka.topics.employee-benefits}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onEmployeeBenefit(EmployeeBenefitEvent event) {
        log.info("Received employee benefit request: requestId={}, employeeId={}", event.requestId(), event.employeeId());
        benefitService.approveBenefit(event);
    }
}
