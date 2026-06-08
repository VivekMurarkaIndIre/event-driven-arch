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

---

### 2026-06-08 — Event schema and publisher layer

**Decision:** Validate event payloads with Zod at the producer boundary (`CampaignPublishedSchema.parse()` in `publish.ts`) rather than in the publisher function or downstream consumers.

**Why:** The producer is the only point that knows the intended payload. Validating there catches schema mismatches before any bytes hit the network, produces a human-readable ZodError (field path + expected type), and keeps the publisher function a pure transport concern that trusts its input is already valid.

**Trade-offs:**
- Consumers still receive unvalidated bytes from SNS (any producer could bypass Zod). Consumers that need correctness guarantees must validate on receive — Zod makes this cheap since they can reuse the shared schema.
- `z.string().datetime()` accepts RFC-3339 strings but not `Date` objects, which is intentional: JSON serialization of `Date` is runtime-dependent; forcing producers to supply an explicit ISO string avoids silent timezone or precision differences.

**Decision:** Publish `tenantId`, `tenantTier`, and `eventType` as SNS message attributes, not only in the message body.

**Why:** SQS subscription filter policies match on message attributes, not body content. Publishing these fields as attributes lets SNS drop messages at the broker before they reach a consumer queue — a "pro-tier notifier" queue can filter `tenantTier = "pro" | "enterprise"` without the consumer deserialising every message.

**Trade-offs:**
- Duplication: the same fields appear in both the body (Zod-validated) and the attributes (plain strings). The attributes are the routing projection; the body is the source of truth.
- SNS message attribute values are strings; any numeric or boolean business field used for routing must be stringified, which is fine for enum fields like `tenantTier`.

**Decision:** `messageDeduplicationId` = SHA-256(`campaignId:version`) computed at the publisher, not stored in the schema.

**Why:** The dedup key is a publishing concern, not a domain concern — the event payload represents what happened, independent of how many times it was transmitted. The `version` dimension lets a schema change produce a new dedup key without changing the `campaignId`.

**Trade-offs:**
- Standard SNS topics do not accept `MessageDeduplicationId` (SNS rejects it with `InvalidParameter`). Consumer-side idempotency (e.g. DynamoDB conditional write keyed on `campaignId + version`) is required regardless of topic type.
- FIFO topics deduplicate within a 5-minute window only. A re-publish after the window delivers a duplicate; consumer idempotency remains the safety net.

**Why:** 5 retries gives transient failures (network blips, cold starts) a fair chance while
preventing poison messages from looping indefinitely. 30s visibility timeout is a safe default
for fast processors; increase per-queue as processing time grows.
