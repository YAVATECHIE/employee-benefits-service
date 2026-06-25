# Employee Benefits Demo - cURL Requests

> **Note**
>
> This project contains a few **training-only trigger values** to simulate production scenarios.
>
> * `SIMULATE-SERVICE-DOWN` → simulates a temporary infrastructure/dependency failure. The message is retried with exponential backoff and, if all retries fail, is sent to the Dead Letter Topic (DLT).
> * `SIMULATE-BUSINESS-ERROR` → simulates a permanent business validation failure. The message is **not retried** and is sent directly to the DLT.
>
> For backward compatibility with earlier training recordings, the project also accepts the legacy trigger values `EMP-FAIL` and `EMP-INVALID`.

---

## 1. Happy Path

```bash
curl -X POST http://localhost:8080/benefits \
-H "Content-Type: application/json" \
-d '{
  "requestId":"REQ-1001",
  "employeeId":"EMP-101",
  "benefitType":"HEALTHCARE",
  "requestedAmount":500
}'
```

### Expected outcome

* Event received
* Benefit validated
* Saved to the database
* `BenefitApprovedEvent` published to Kafka

---

## 2. Idempotency (Duplicate Request)

Run the same request again.

```bash
curl -X POST http://localhost:8080/benefits \
-H "Content-Type: application/json" \
-d '{
  "requestId":"REQ-1001",
  "employeeId":"EMP-101",
  "benefitType":"HEALTHCARE",
  "requestedAmount":500
}'
```

### Expected outcome

* Duplicate request detected
* No second database insert
* No second approval event published

Example log:

```text
Duplicate request detected. requestId=REQ-1001
```

---

## 3. Retry + DLT (Transient Infrastructure Failure)

```bash
curl -X POST http://localhost:8080/benefits \
-H "Content-Type: application/json" \
-d '{
  "requestId":"REQ-3001",
  "employeeId":"SIMULATE-SERVICE-DOWN",
  "benefitType":"HEALTHCARE",
  "requestedAmount":500
}'
```

### Expected outcome

* Validation service unavailable
* Retries with exponential backoff
* Message sent to the DLT after all retry attempts are exhausted

---

## 4. Direct DLT (Permanent Business Validation Failure)

```bash
curl -X POST http://localhost:8080/benefits \
-H "Content-Type: application/json" \
-d '{
  "requestId":"REQ-4001",
  "employeeId":"SIMULATE-BUSINESS-ERROR",
  "benefitType":"HEALTHCARE",
  "requestedAmount":500
}'
```

### Expected outcome

* Business validation fails
* No retries
* Message sent directly to the DLT
