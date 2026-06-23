package com.example.employeebenefits.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.service.BenefitValidationService;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {"employee-benefits", "benefits-approved"}
)
@TestPropertySource(properties = {
        "spring.kafka.properties.security.protocol=PLAINTEXT",
        "spring.kafka.properties.sasl.mechanism=",
        "spring.kafka.properties.sasl.jaas.config=",
        "spring.kafka.consumer.group-id=employee-benefits-dlt-test"
})
@DirtiesContext
class EmployeeBenefitDltIntegrationTest {

    @Autowired
    private KafkaTemplate<String, EmployeeBenefitEvent> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @SpyBean
    private BenefitValidationService benefitValidationService;

    @SpyBean
    private EmployeeBenefitListener employeeBenefitListener;

    @Test
    void sendsMessageToDltAndInvokesDltHandlerAfterRetries() {
        EmployeeBenefitEvent event = benefitEvent("REQ-DLT-1", "EMP-FAIL");
        kafkaTemplate.send("employee-benefits", event.requestId(), event);

        verify(benefitValidationService, timeout(15_000).times(3))
                .validate(org.mockito.ArgumentMatchers.any(EmployeeBenefitEvent.class));
        verify(employeeBenefitListener, timeout(15_000))
                .processDlt(org.mockito.ArgumentMatchers.any(EmployeeBenefitEvent.class));

        ConsumerRecord<String, EmployeeBenefitEvent> dltRecord = readRecord("employee-benefits-dlt", "dlt-proof-consumer");
        assertThat(dltRecord.value().requestId()).isEqualTo("REQ-DLT-1");
        assertThat(dltRecord.value().employeeId()).isEqualTo("EMP-FAIL");
    }

    private ConsumerRecord<String, EmployeeBenefitEvent> readRecord(String topic, String groupId) {
        Map<String, Object> properties = KafkaTestUtils.consumerProps(groupId, "false", embeddedKafkaBroker);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.employeebenefits.event");
        properties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EmployeeBenefitEvent.class.getName());

        try (Consumer<String, EmployeeBenefitEvent> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                properties,
                new StringDeserializer(),
                new JsonDeserializer<>(EmployeeBenefitEvent.class, false)
        )) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(10));
        }
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
