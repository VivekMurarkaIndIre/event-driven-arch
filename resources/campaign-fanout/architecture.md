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

---

### 2026-06-08 — Consumer layer: BaseConsumer, idempotency, partial batch response

**Decision:** Implement SQS polling mechanics once in an abstract `BaseConsumer<TBody>` rather than repeating them in each consumer.

**Why:** All SQS consumers share the same ceremony — long polling, visibility extension, batch deletion, SNS envelope unwrapping, error containment. Centralising this lets concrete consumers focus entirely on business logic (`processMessageBatch`). Changes to the polling strategy (e.g. tuning the extension interval, adding tracing) propagate to all consumers automatically.

**Trade-offs:**
- The base class becomes a load-bearing abstraction; bugs in it affect every consumer simultaneously. Mitigate with integration tests that exercise the base class against LocalStack.
- Generic `TBody` relies on a `JSON.parse(...) as TBody` cast, which is not type-safe at runtime. Concrete consumers must re-validate with Zod on receive; the TypeScript type is a documentation aid, not a runtime guarantee.

**Decision:** Composite idempotency key = `messageId:eventId`.

**Why:** Neither component alone is sufficient. `messageId` (the SQS envelope ID) deduplicates re-deliveries of the same physical message (crash-before-delete, rare SQS duplicate). `eventId` (the business ID, e.g. `campaignId`) deduplicates producer-level retries where a fresh publish produces a new `messageId` carrying the same business event. Both are needed to cover all realistic duplicate scenarios.

**Trade-offs:**
- The in-memory store is only safe for single-process consumers. Multiple replicas each deduplicate only their own history. Production replacement: DynamoDB conditional write (`attribute_not_exists(pk)`), which is atomic, durable, and shared across all replicas.
- Memory grows unboundedly in the current implementation. A production store must evict keys older than the SQS message retention period.

**Decision:** Return `BatchItemFailure[]` from `processMessageBatch` (partial batch response) rather than treating the whole batch as pass/fail.

**Why:** A binary outcome forces an untenable choice when one message in a ten-message batch fails: delete all (silently drop the failed message) or delete none (re-deliver the nine messages that already succeeded). Per-message failure reporting lets SQS retry exactly the failing messages while deleting the successes, giving each message its own independent retry lifecycle up to `maxReceiveCount`, then DLQ.

**Trade-offs:**
- Callers must not return a failure for a message they partially processed — that would re-deliver a message whose side effects are already partially applied. All side effects must be atomic or idempotent before returning success.

---

### 2026-06-08 — SNS subscription filter policies and idempotent infra:setup

**Decision:** Add `DlqMonitor` (peek-and-alert) and `replayDlq` (drain-and-replay) as operational tooling alongside the consumers.

**Why:** DLQs without tooling are a data graveyard — messages accumulate silently until an operator notices metrics. `DlqMonitor` closes the alert loop: it detects depth > 0 and emits structured JSON alerts that log forwarders can consume. `replayDlq` closes the recovery loop: it re-queues messages directly into the main SQS queue (not through SNS, which would fan-out to all queues) and marks each re-queued message with `replayedAt` to prevent infinite replay loops on persistent failures.

**Trade-offs:**
- `DlqMonitor` uses `ReceiveMessage` + `ChangeMessageVisibilityBatch(0)` to peek at messages. This is the standard peek pattern but it briefly hides messages from replay scripts running concurrently. The 5-second visibility window is short enough to be acceptable.
- `replayDlq` re-publishes to the main SQS queue directly, bypassing SNS. This means SNS filter policies don't re-apply on replay — which is intentional (the message already passed the filter when originally published) but means the operator must target the correct DLQ for the queue they want to re-populate.
- A message that fails after replay carries `replayedAt` when it hits the DLQ again. The replay script skips it. Recovery then requires manual investigation or a schema fix before the next replay.

**Decision:** Apply SNS subscription filter `{ tenantTier: ["pro", "enterprise"] }` to the `campaign-notifier` subscription; all other queue subscriptions carry no filter.

**Why:** The notifier sends push/SMS notifications, a paid-tier feature. Free-tier tenants generating campaign events should never trigger notification delivery. Enforcing this at the SNS broker (before the message enters the queue) means the `NotificationConsumer` receives zero free-tier messages and incurs zero processing cost for them. The alternative — receiving all messages and skipping in the consumer — wastes ReceiveMessage API calls, increases queue depth metrics, and requires consumer logic to know about tier rules.

**Trade-offs:**
- The filter operates on message *attributes*, not the body. The `tenantTier` attribute must be set at publish time (done in `campaignPublisher.ts`). Any producer that omits this attribute will have its messages filtered out entirely (SNS treats a missing attribute as non-matching), which may or may not be the desired behaviour.
- Adding a new paid-tier value (e.g. `"enterprise-plus"`) requires updating the filter policy on the subscription, not just the schema. Infrastructure and code must stay in sync.
- `FilterPolicyScope: "MessageAttributes"` is the default. Using `"MessageBody"` instead would allow filtering on JSON body fields but adds SNS parsing overhead and couples the filter to the body schema.

**Decision:** Make `infra:setup` fully idempotent for SQS queues using an `ensureQueue` helper that catches `QueueNameExists` and falls back to `SetQueueAttributes`.

**Why:** `CreateQueue` is only idempotent when all attributes match exactly. Lowering `maxReceiveCount` (or any future attribute change) throws `QueueNameExists` on re-run, leaving subscriptions uncreated. The `ensureQueue` helper resolves the existing URL and applies the desired attributes, making `infra:setup` safe to re-run after any configuration change without requiring a teardown.

**Trade-offs:**
- `SetQueueAttributes` on an existing queue with live consumers briefly changes its behaviour (e.g. a new `maxReceiveCount` takes effect for the next delivery). This is generally safe for non-destructive attribute changes like adjusting retry counts; it would be dangerous for changes like shrinking `VisibilityTimeout` below in-flight processing time.
- `QueueNameExists` is a full-stop error from SQS, not a "soft exists" signal — the fallback path adds two extra API calls on every re-run for queues with changed attributes.

**Decision:** Make `infra:setup` idempotent by catching "already exists" errors in `createEventBridgeBuses` and `createDynamoTables`.

**Why:** LocalStack persists EventBridge buses and DynamoDB tables across some container restarts (even with `PERSISTENCE: 0`). Without idempotency, re-running `infra:setup` after a partial restart fails partway through, leaving subscriptions uncreated. Making each creation step safe to re-run means `infra:setup` is a reliable "ensure everything exists" command rather than a one-shot bootstrap.

**Trade-offs:**
- SNS `CreateTopic` and SQS `CreateQueue` are already idempotent by the AWS API contract (they return the existing resource on duplicate calls). Only EventBridge and DynamoDB required explicit error handling.
- Idempotent `infra:setup` does not clean up stale resources from a previous schema version — that requires a separate teardown or migration step.
