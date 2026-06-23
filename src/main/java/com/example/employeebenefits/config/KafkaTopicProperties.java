package com.example.employeebenefits.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.topics")
public record KafkaTopicProperties(
        String employeeBenefits,
        String benefitsApproved
) {
}
