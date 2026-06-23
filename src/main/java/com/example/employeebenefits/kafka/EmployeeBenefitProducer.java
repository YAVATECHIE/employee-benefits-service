package com.example.employeebenefits.kafka;

import com.example.employeebenefits.config.KafkaTopicProperties;
import com.example.employeebenefits.event.EmployeeBenefitEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeBenefitProducer {

    private final KafkaTemplate<String, EmployeeBenefitEvent> kafkaTemplate;
    private final KafkaTopicProperties topicProperties;

    public void publish(EmployeeBenefitEvent event) {
        kafkaTemplate.send(topicProperties.employeeBenefits(), event.requestId(), event);
        log.info("Published employee benefit event: requestId={}, employeeId={}", event.requestId(), event.employeeId());
    }
}
