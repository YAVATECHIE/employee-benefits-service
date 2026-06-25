# employee-benefits-service

Sample Spring Kafka application for Kafka developer training and production-pattern demonstrations.

## Project Overview

This service demonstrates a simple employee benefits approval flow:

`EmployeeBenefitEvent` -> consume from Kafka -> validate -> persist request -> publish `BenefitApprovedEvent`

Business flow:

1. A benefit request is submitted to the `employee-benefits` topic.
2. `EmployeeBenefitListener` consumes the event with `@KafkaListener`.
3. `BenefitService` checks for duplicates using `requestId`.
4. `BenefitValidationService` validates the request.
5. The approved request is saved to H2 with Spring Data JPA.
6. A `BenefitApprovedEvent` is published to `benefits-approved`.

## Architecture

```text
employee-benefits topic
        |
        v
Spring Boot application
  - Kafka listener
  - validation
  - idempotency check
  - JPA persistence
        |
        +--> H2 database
        |
        +--> benefits-approved topic
        |
        +--> employee-benefits-retry topic
        |
        +--> employee-benefits-dlt topic
```

## Features Demonstrated

- Spring Kafka consumer with `@KafkaListener`
- Kafka producer with `KafkaTemplate`
- `@RetryableTopic`
- Dead Letter Topic handling with `@DltHandler`
- Idempotent processing based on `requestId`
- Spring Data JPA persistence
- H2 database
- Actuator health endpoint
- Unit and integration tests

## Topics

- `employee-benefits`
- `benefits-approved`
- `employee-benefits-retry`
- `employee-benefits-dlt`

## Running the Application

The application is configured for Confluent Cloud through environment variables.

Required environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="<your-confluent-bootstrap-server>"
export KAFKA_API_KEY="<your-confluent-api-key>"
export KAFKA_API_SECRET="<your-confluent-api-secret>"
```

Run the service:

```bash
mvn spring-boot:run
```

The app starts on `http://localhost:8080`.

## Sample Request

Submit a benefit request through the demo REST endpoint:

```bash
curl -i -X POST http://localhost:8080/benefits \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "REQ-1001",
    "employeeId": "EMP-100",
    "benefitType": "HEALTH",
    "requestedAmount": 125.50
  }'
```

Expected response:

```text
HTTP/1.1 202
```

## Demo Scenarios

Happy path:

- Send a request with a unique `requestId` and an employee ID other than `EMP-FAIL`.
- The request is consumed, validated, persisted, and approved.
- A `BenefitApprovedEvent` is published to `benefits-approved`.

Retry scenario:

- Send a request with `employeeId=EMP-FAIL`.
- `BenefitValidationService` throws an exception.
- `@RetryableTopic` retries processing 3 times with a 2 second backoff.
- The message is routed through `employee-benefits-retry`.

DLT scenario:

- Keep using `employeeId=EMP-FAIL`.
- After retry attempts are exhausted, the message is published to `employee-benefits-dlt`.
- The listener's `@DltHandler` logs the failed message.

Duplicate delivery / idempotency scenario:

- Send the same `requestId` twice, for example `REQ-1001`.
- The first message is persisted and approved.
- The second message is detected as a duplicate.
- No second database record is created and no second approval event is published.

## Failure Scenarios

The project includes training-only trigger values to demonstrate different failure paths:

| Employee ID | Scenario | Expected Behaviour |
|-------------|----------|-------------------|
| EMP-FAIL / SIMULATE-SERVICE-DOWN | Transient infrastructure failure | Retries with exponential backoff, then DLT |
| EMP-INVALID / SIMULATE-BUSINESS-ERROR | Permanent business validation failure | Sent directly to the DLT (no retries) |

## H2 Database

H2 console:

```text
http://localhost:8080/h2-console
```

Connection details:

```text
JDBC URL: jdbc:h2:mem:employee-benefits
Username: sa
Password:
```

Example query:

```sql
select * from benefit_request;
```

Note: H2 is in-memory and `ddl-auto` is `create-drop`, so data is reset when the app restarts.

## Actuator

```text
http://localhost:8080/actuator
http://localhost:8080/actuator/health
```

## Testing

Run all tests:

```bash
mvn test
```

Test coverage:

- Unit tests for `BenefitService` using Mockito only.
- Embedded Kafka integration test proving `employee-benefits` messages are consumed by the listener.
- Retry integration test proving `EMP-FAIL` is attempted 3 times and appears on the retry topic.
- DLT integration test proving exhausted retries are published to `employee-benefits-dlt` and handled by `@DltHandler`.
- Idempotency integration test proving duplicate `requestId` values do not create a second row or publish a second approval event.
