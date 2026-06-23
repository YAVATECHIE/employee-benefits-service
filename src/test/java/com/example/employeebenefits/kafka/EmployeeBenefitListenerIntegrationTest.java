package com.example.employeebenefits.kafka;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.service.BenefitService;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
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
        "spring.kafka.consumer.group-id=employee-benefits-listener-test"
})
@DirtiesContext
class EmployeeBenefitListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, EmployeeBenefitEvent> kafkaTemplate;

    @MockBean
    private BenefitService benefitService;

    @Test
    void consumesMessageAndInvokesBenefitService() {
        EmployeeBenefitEvent event = benefitEvent("REQ-LISTENER-1", "EMP-100");

        kafkaTemplate.send("employee-benefits", event.requestId(), event);

        verify(benefitService, timeout(10_000)).approveBenefit(argThat(received ->
                received.requestId().equals("REQ-LISTENER-1")
                        && received.employeeId().equals("EMP-100")
        ));
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
