package com.example.employeebenefits.kafka;

import com.example.employeebenefits.config.KafkaTopicProperties;
import com.example.employeebenefits.event.BenefitApprovedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BenefitApprovedProducer {

    private final KafkaTemplate<String, BenefitApprovedEvent> kafkaTemplate;
    private final KafkaTopicProperties topicProperties;

    public void publish(BenefitApprovedEvent event) {
        kafkaTemplate.send(topicProperties.benefitsApproved(), event.requestId(), event);
    }
}
