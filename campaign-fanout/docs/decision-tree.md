# Messaging Service Decision Tree

Use this tree to pick the right AWS messaging service given five inputs:

1. **Tenant count** — how many tenants (customers) share this event stream?
2. **Message rate** — peak messages per second
3. **Ordering required?** — must events for a given entity arrive in order?
4. **Replay required?** — do consumers need to re-read past events?
5. **Content routing required?** — do different consumers receive different subsets of events based on event attributes?

---

## Decision Tree

```
START
│
├─ Replay required?
│   │
│   ├─ YES ──────────────────────────────────────────────────────────────────┐
│   │                                                                        │
│   │   Message rate > 10 000/s (sustained)?                                 │
│   │   │                                                                    │
│   │   ├─ YES → KAFKA (MSK)                                                 │
│   │   │        Reason: Kinesis tops out around 1 MB/s per shard;           │
│   │   │        Kafka scales to GB/s with more partitions/brokers.          │
│   │   │        Accept: high operational overhead.                           │
│   │   │                                                                    │
│   │   └─ NO ──► Ordering required?                                          │
│   │             │                                                           │
│   │             ├─ YES → KINESIS (with partition key = entity ID)          │
│   │             │        Reason: strict per-shard ordering + replay.       │
│   │             │        Alternative: Kafka if you already operate it.     │
│   │             │                                                           │
│   │             └─ NO  → KINESIS                                            │
│   │                      Reason: simplest managed replay store.            │
│   │                      Use EventBridge archive only if rate <10 k/s      │
│   │                      and 24-hour replay granularity is acceptable.     │
│                                                                            │
└─ NO (no replay) ───────────────────────────────────────────────────────────┘
    │
    ├─ Content routing required?
    │   (different consumers see different subsets based on event attributes)
    │   │
    │   ├─ YES, AND fan-out to N consumers also required?
    │   │   │
    │   │   └─ SNS (fan-out) + EventBridge (routing)
    │   │      Reason: SNS broadcasts to all; EventBridge rules filter subsets.
    │   │      Wire SNS → SQS for each consumer; also publish to EventBridge bus
    │   │      for rule-based conditional targets.
    │   │
    │   ├─ YES, routing only (no broadcast fan-out needed)?
    │   │   │
    │   │   └─ EVENTBRIDGE
    │   │      Reason: content-based filtering built in; low ops overhead.
    │   │      Good for: "route campaign-activated to billing only".
    │   │
    │   └─ NO (all consumers see all events)
    │       │
    │       ├─ Ordering required?
    │       │   │
    │       │   ├─ YES ──► Message rate > 3 000/s?
    │       │   │           │
    │       │   │           ├─ YES → KINESIS (or KAFKA)
    │       │   │           │        SQS FIFO caps at 3 000 msg/s with batching.
    │       │   │           │
    │       │   │           └─ NO  → SQS FIFO (message group ID = entity ID)
    │       │   │                    Reason: strict ordering per entity; simple ops.
    │       │   │
    │       │   └─ NO  ──► Fan-out to N independent consumers?
    │       │               │
    │       │               ├─ YES → SNS → SQS (fan-out)
    │       │               │        Reason: canonical AWS fan-out pattern.
    │       │               │        Each consumer owns its queue + DLQ.
    │       │               │
    │       │               └─ NO  → SQS (standard)
    │       │                        Reason: simplest possible work queue.
    │       │                        Multiple workers compete on one queue.
    │
    └─ Multi-tenant: does each tenant need strict isolation at the queue level?
        │
        ├─ YES (regulated data, per-tenant SLAs, chargeback per tenant)
        │   │
        │   └─ Dedicated queue per tenant
        │      Options: SQS (fan-out per tenant), Kinesis (shard per tenant up to limits)
        │      Trade-off: queue sprawl — automate provisioning, use naming conventions.
        │
        └─ NO (tenants share queues, consumer filters by tenant ID in message body)
            │
            └─ Proceed up the tree with shared queues.
               Add tenant ID to message attributes for consumer-side filtering.
```

---

## Scoring Table

If the tree is ambiguous (multiple paths apply), score each candidate:

| Criterion | SNS + SQS | EventBridge | Kinesis | Kafka (MSK) |
|-----------|-----------|-------------|---------|-------------|
| Need replay | ✗ | Limited | ✓ | ✓ |
| Need content routing | ✗ (filter at consumer) | ✓ | ✗ | ✓ (streams) |
| Need strict ordering | ✗ (standard) / ✓ (FIFO) | ✗ | ✓ (per shard) | ✓ (per partition) |
| Rate < 3 000/s | ✓ | ✓ | ✓ | ✓ |
| Rate 3 000–10 000/s | ✓ (standard only) | ✓ | ✓ | ✓ |
| Rate > 10 000/s | ✓ (standard) | ✗ (soft limit) | ✓ (add shards) | ✓ |
| Low operational overhead | ✓ | ✓ | Medium | ✗ |
| Fan-out (1 event → N consumers) | ✓ | ✓ (rules) | ✗ native | ✗ native |

Higher score on your required rows = preferred service.

---

## Applied to campaign-fanout

| Input | Value |
|-------|-------|
| Tenant count | ~500 (B2B SaaS, shared infrastructure) |
| Message rate | < 500/s (peak) |
| Ordering required? | No — consumers are idempotent |
| Replay required? | No — DLQ handles failures; no historical backfill yet |
| Content routing required? | Yes — future: route `campaign-activated` to billing only |

**Result:** SNS (fan-out to all consumers) + EventBridge (conditional routing for billing/audit rules).
SQS queues back each consumer for independent scaling and failure isolation.
Kinesis and Kafka are out-of-scope: no replay requirement and operational overhead is unjustified
at this message rate.
