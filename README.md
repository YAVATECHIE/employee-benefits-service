# employee-benefits-service
Sample Spring Kafka application used for Kafka developer training and production-pattern demonstrations.

## Overview

This service consumes `EmployeeBenefitEvent` messages from the `employee-benefits` Kafka topic, persists the approved benefit request in H2 through Spring Data JPA, and publishes a `BenefitApprovedEvent` message to the `benefits-approved` topic.

## Stack

- Java 21
- Spring Boot 3.5
- Spring for Apache Kafka
- Spring Data JPA
- H2 Database
- Spring Boot Actuator
- Lombok

## Run

Start Kafka on `localhost:9092`, then run:

```bash
mvn spring-boot:run
```
