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

### 2026-06-08 — FIFO delivery stack and DynamoDB idempotency

**Decision:** Add a parallel FIFO stack (`campaign-events.fifo` → `campaign-processor.fifo`) for the email consumer alongside the existing standard fan-out, rather than migrating the standard topics to FIFO.

**Why:** Standard and FIFO SNS topics cannot share subscriptions — a FIFO SQS queue can only subscribe to a FIFO SNS topic. Migrating the standard topics would require updating all four consumer subscriptions and introducing ordering constraints (FIFO throughput ceiling: 300 msg/s per group) on consumers that don't need them. Running FIFO in parallel lets the email consumer gain stronger delivery guarantees while analytics, audit, and notification consumers continue with the simpler, higher-throughput standard stack.

**Trade-offs:**
- Two parallel stacks mean the producer must decide which topic to publish to. Currently `publish.ts` targets the standard topic; to use the FIFO stack, the caller passes `topicType: "fifo"`.
- FIFO throughput limit: 300 published messages per second globally (3 000/s with high throughput mode). For campaign events this is acceptable; for high-volume event ingestion it would be a ceiling.
- The 5-minute SNS FIFO deduplication window means a re-publish with the same `MessageDeduplicationId` after the window IS delivered again. Consumer-side DynamoDB idempotency remains the durable safety net.

**Decision:** Replace `InMemoryIdempotencyStore` with `DynamoDBIdempotencyStore` for the email consumer; keep in-memory for analytics and notifications.

**Why:** Email send is the most dangerous duplicate — a user receiving the same campaign email twice is a concrete negative user experience and a CAN-SPAM / GDPR compliance risk. DynamoDB conditional write (`attribute_not_exists(pk)`) is atomic across any number of consumer replicas; the in-memory store is per-process and per-restart. Analytics and notification consumers have lower duplicate risk (a double-counted impression or an extra push notification is inconvenient but not a compliance issue) and don't justify the added DynamoDB latency.

**Trade-offs:**
- Every idempotency check for the email consumer now incurs a `GetItem` + `PutItem` round-trip to DynamoDB. For a consumer processing 10 messages per batch at 30ms average DynamoDB latency, this adds ~300ms to each batch — acceptable for an email workload, problematic for a high-throughput stream processor.
- The race window between `has()` returning false and `add()` succeeding is real. Two concurrent consumers can both pass the `has()` check, both send the email, and then one fails the `add()` conditional write. The failure surfaces as a `BatchItemFailure` and triggers an SQS retry, at which point `has()` returns true. The email was sent twice in this scenario — acceptable during a race condition, which is rare under SQS FIFO per-group ordering.

---

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

---

### 2026-06-08 — Per-tenant concurrency cap and rate limiting (noisy-neighbour mitigation)

**Decision:** Add `maxConcurrentPerTenant?: number` to `ConsumerConfig` and a `TenantRateLimiter` (token bucket) as separate, composable fairness mechanisms rather than one monolithic "fair queue" abstraction.

**Why:** Concurrency cap (semaphore) and throughput cap (token bucket) solve different problems and are often needed independently. A semaphore alone limits how many messages from tenant-a are in-flight simultaneously but does not cap their rate — with a fast processor, 2 concurrent slots can still process 200 msg/s. A token bucket alone caps throughput but does not prevent 10 messages from the same tenant from all starting at once and saturating I/O (database connections, HTTP sockets). Together they model the full constraint: at most N in-flight and at most R completions per second, per tenant.

Keeping them separate means existing consumers (EmailConsumer, AnalyticsConsumer) opt in by setting `maxConcurrentPerTenant` in their config and calling `rateLimiter.acquire()` in their `processMessageBatch` — no changes to their logic or constructor. Consumers that don't need fairness (e.g. a low-volume audit queue) pay zero overhead.

**Trade-offs:**
- The token bucket is per-consumer-instance. With N consumer replicas, a tenant can drive N × maxRatePerSecond total. For truly global rate limiting the bucket state must be externalised (Redis INCR+EXPIRE with Lua atomicity).
- `maxConcurrentPerTenant` changes the calling convention of `processMessageBatch`: from "receive the full batch at once" to "receive one message per call, called concurrently." Subclasses that rely on cross-message batch operations (e.g. DynamoDB BatchWrite across 10 messages) would need to override `extractTenantId` to return a constant key, collapsing per-tenant into global, to restore the batch-at-once behaviour.
- In-process throttle delays hold a SQS visibility slot open. At 1 msg/s rate limit with a 30 s visibility timeout, a message can spend up to 29 s waiting inside the consumer before its work starts — almost exhausting the window before the actual operation begins. Size `visibilityTimeout` relative to `1/maxRatePerSecond + expected_processing_time`.

---

### 2026-06-08 — EventBridge content-based routing (survey + high-volume queues)

**Decision:** Add two EventBridge rules on `campaign-bus` routing to dedicated SQS queues: `route-survey-campaigns` (matches `detail.campaignType = "survey"`) and `route-high-volume-campaigns` (matches `detail.audienceSize > 10000`). `SurveyConsumer` and `HighVolumeConsumer` read from these queues.

**Why:** These routing decisions cannot be expressed with SNS filter policies, which operate only on flat `MessageAttributes`. `campaignType` could be duplicated into an attribute (as `tenantTier` was for the notifier), but `audienceSize > 10000` requires a numeric range check on a body field — that is only expressible as an EventBridge pattern. Rather than mixing attribute-based and body-based routing in SNS, EventBridge handles both dimensions cleanly from a single `PutEvents` call.

A survey campaign with `audienceSize > 10000` matches both rules simultaneously — EventBridge delivers it to both target queues independently. Each queue represents a distinct downstream concern (survey tooling vs. high-throughput delivery pipeline), so the fan-out is intentional: the same event triggers different processing in parallel.

**Trade-offs:**
- EventBridge costs $1/million events entering the bus regardless of rule matches. SNS filters are free. For low-volume learning traffic this is negligible; for high-throughput production workloads the cost difference is material.
- `PutEvents` throughput default is 10 000 events/s per region (soft limit). SNS standard has no practical throughput ceiling. If campaign event volume exceeds this, EventBridge becomes a bottleneck unless the quota is raised.
- `PutEvents` partial failure: `FailedEntryCount > 0` does not throw — it must be checked explicitly. The publisher guards on this and throws, but callers must handle the re-throw.
- EventBridge delivers asynchronously with sub-second typical latency; SNS is synchronous at the broker (though SQS polling still adds latency). For time-sensitive routing, SNS + attribute-based filters are lower-latency.
- `HighVolumeConsumer` uses 60 s visibility timeout vs. 30 s for other consumers — reflects the assumption that high-volume campaigns take longer to process. If processing routinely exceeds 60 s, the visibility extension loop in `BaseConsumer` (fires at 30 s) will renew it; if it exceeds the first renewal window without completing, increase `visibilityTimeout` in the constructor.

---

### 2026-06-09 — Complete architecture SVG and structured README testing guide

**Decision:** Add `resources/campaign-fanout/complete_architecture_module8.svg` as the canonical visual overview of the campaign-fanout system, and embed it in the root README alongside a colour guide and narrative walkthrough. Restructure the README's Quick Start and Commands sections into named testing flows with per-flow terminal layouts, expected output snippets, and a 6-terminal full-layout table.

**Why:** As the system grew across eight modules (SNS fan-out, FIFO ordering, DLQ tooling, EventBridge routing, semaphore + rate limiter), a plain command list was no longer sufficient for onboarding. The SVG makes the relationship between transports, queues, and cross-cutting concerns (idempotency, fairness, DLQ replay) visible at a glance. The named-flow README structure answers "which terminals do I need open?" for each feature independently rather than requiring the reader to understand the whole system first.

**Trade-offs:**
- SVG in the repo adds ~50 KB but renders directly in GitHub/VS Code without a separate tool. An alternative (Mermaid in markdown) would be version-controlled text but lacks the visual density needed to show all seven components (publisher, three transports, consumers, DynamoDB, operational tooling) clearly.
- Named-flow sections in the README duplicate some information present in script comments. The duplication is acceptable because the README is the entry point for a reader who hasn't yet opened any source files.
