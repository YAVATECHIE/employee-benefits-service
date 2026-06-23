package com.example.employeebenefits.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.example.employeebenefits.event.EmployeeBenefitEvent;
import com.example.employeebenefits.service.BenefitValidationService;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
        "spring.kafka.consumer.group-id=employee-benefits-retry-test"
})
@DirtiesContext
class EmployeeBenefitRetryIntegrationTest {

    @Autowired
    private KafkaTemplate<String, EmployeeBenefitEvent> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @SpyBean
    private BenefitValidationService benefitValidationService;

    @Test
    void retriesThreeTimesAndPublishesToRetryTopic() {
        EmployeeBenefitEvent event = benefitEvent("REQ-RETRY-1", "EMP-FAIL");
        kafkaTemplate.send("employee-benefits", event.requestId(), event);

        verify(benefitValidationService, timeout(15_000).times(3))
                .validate(org.mockito.ArgumentMatchers.any(EmployeeBenefitEvent.class));

        String retryTopic = retryTopicName();
        List<ConsumerRecord<String, EmployeeBenefitEvent>> retryRecords =
                readRecords(retryTopic, "retry-proof-consumer");

        assertThat(retryRecords)
                .anySatisfy(record -> {
                    assertThat(record.value().requestId()).isEqualTo("REQ-RETRY-1");
                    assertThat(record.value().employeeId()).isEqualTo("EMP-FAIL");
                });
    }

    private String retryTopicName() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());

        try (AdminClient adminClient = AdminClient.create(properties)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Set<String> names = adminClient.listTopics().names().get();
                assertThat(names).anyMatch(name -> name.startsWith("employee-benefits-retry"));
            });

            return adminClient.listTopics().names().get().stream()
                    .filter(name -> name.startsWith("employee-benefits-retry"))
                    .findFirst()
                    .orElseThrow();
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to resolve retry topic", ex);
        }
    }

    private List<ConsumerRecord<String, EmployeeBenefitEvent>> readRecords(String topic, String groupId) {
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
            ConsumerRecords<String, EmployeeBenefitEvent> records =
                    KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            List<ConsumerRecord<String, EmployeeBenefitEvent>> matchingRecords = new ArrayList<>();
            records.records(topic).forEach(matchingRecords::add);
            return matchingRecords;
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
