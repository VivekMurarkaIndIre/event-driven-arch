# campaign-fanout — Architecture

## Overview

A TypeScript monorepo exploring fan-out patterns using AWS SNS, SQS, EventBridge, and DynamoDB,
developed locally against LocalStack.

```
                         ┌──────────────────────────────────────────────┐
                         │              LocalStack :4566                │
                         │                                              │
  Producer               │  SNS Topics            SQS Queues           │
  (future service)  ───► │  campaign-created  ──► email-sender         │
                         │  campaign-updated  │   email-sender-dlq     │
                         │  campaign-deleted  ├──► sms-sender          │
                         │  campaign-activated│   sms-sender-dlq       │
                         │                   ├──► push-sender          │
                         │                   │   push-sender-dlq       │
                         │                   └──► analytics-ingester   │
                         │                       analytics-ingester-dlq│
                         │                                              │
                         │  EventBridge Buses     DynamoDB Tables       │
                         │  campaign-bus     ─►  Campaigns             │
                         │  campaign-dlq-bus      (PK/SK + GSI)        │
                         └──────────────────────────────────────────────┘
```

---

### 2025-06-08 — Initial scaffold

**Decision:** Use SNS → SQS fan-out rather than EventBridge as the primary fan-out mechanism.

**Why:** SNS fan-out to SQS is the canonical AWS pattern for broadcasting a single event to
multiple independent consumers. Each consumer owns its own queue, processes at its own pace,
and can fail without blocking other consumers. EventBridge is added alongside for rule-based
routing (e.g. only route `campaign-activated` events to specific targets) — a complementary
concern, not a replacement.

**Trade-offs:**
- SNS/SQS adds operational surface (topic + N queues + N DLQs to manage)
- DLQs require a separate monitoring/alerting strategy to be useful
- LocalStack closely mirrors real AWS but not 100% — test against real AWS before production

**Decision:** DLQ maxReceiveCount = 5, VisibilityTimeout = 30s.

**Why:** 5 retries gives transient failures (network blips, cold starts) a fair chance while
preventing poison messages from looping indefinitely. 30s visibility timeout is a safe default
for fast processors; increase per-queue as processing time grows.
