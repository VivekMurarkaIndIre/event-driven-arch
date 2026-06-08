# AWS Messaging Services — Decision Reference

Comparison of SNS, SQS, EventBridge, Kinesis Data Streams, and MSK (Kafka) across
the six dimensions that matter most when choosing a messaging backbone.

---

## At a Glance

| Dimension | SNS | SQS | EventBridge | Kinesis | Kafka (MSK) |
|-----------|-----|-----|-------------|---------|-------------|
| Delivery guarantee | At-least-once | At-least-once | At-least-once | At-least-once | At-least-once (configurable to effectively-once with idempotent producers) |
| Ordering | None (standard); per-group (FIFO) | None (standard); strict (FIFO) | None | Per-shard, strict | Per-partition, strict |
| Replay | No | No (up to 14 days retention, not replayable by default) | Limited (24 h archive replay) | Yes (up to 365 days) | Yes (unlimited, until log compaction/deletion) |
| Consumer model | Push (fan-out to N targets) | Pull (competing consumers) | Push (rule-matched targets) | Pull (shard iterator / enhanced fan-out push) | Pull (consumer groups) |
| Throughput ceiling | ~300 pub/s (standard); ~300 msg/s (FIFO) | Unlimited (standard); 300 msg/s (FIFO) | 10 k events/s per bus (soft) | 1 MB/s write per shard; 2 MB/s read per shard (5 consumers) | Effectively unlimited (add partitions/brokers) |
| Operational overhead | Very low (fully managed, no infra) | Very low (fully managed) | Low (fully managed, schema registry optional) | Medium (shard management, scaling, consumer checkpointing) | High (broker fleet, ZooKeeper/KRaft, replication, retention tuning) |

---

## Detailed Notes per Dimension

### Delivery Guarantee

All five services are **at-least-once** in their default configuration — every message is
delivered but duplicates are possible. Consumers must be idempotent.

- **SNS/SQS FIFO** together give exactly-once *delivery to the queue* via content-based
  deduplication (5-minute deduplication window), but the consumer can still receive a
  message more than once if it crashes mid-processing without deleting the message.
- **Kafka** can be configured for idempotent producers (`enable.idempotence=true`) and
  exactly-once semantics (EOS) via transactions, but EOS adds latency and complexity and
  requires careful consumer design. In practice most teams treat it as at-least-once.

### Ordering

- **SNS (standard)** — no ordering. Messages from the same publisher can arrive at
  subscribers out of order.
- **SQS (standard)** — best-effort ordering. Use FIFO queues when order matters, but note
  FIFO queues cap at 300 messages/s (3 000 with batching).
- **SQS FIFO + message group ID** — strict ordering within a group; different groups
  process in parallel.
- **EventBridge** — no ordering guarantees; events from a single bus may arrive at targets
  out of order.
- **Kinesis** — strict ordering within a shard. Messages with the same partition key always
  go to the same shard. Cross-shard ordering is not guaranteed.
- **Kafka** — strict ordering within a partition. The same key always maps to the same
  partition (assuming no repartitioning). Compacted topics retain only the latest value per
  key.

### Replay

- **SNS** — no replay. Once a message is delivered (or fails after retries), it is gone.
- **SQS** — not designed for replay. You can increase `MessageRetentionPeriod` (up to 14
  days) so unconsumed messages sit in the queue, but you cannot seek back and re-read
  already-consumed messages.
- **EventBridge** — the event archive feature lets you replay archived events from a
  specific time window back to an event bus (up to 24-hour granularity). Useful for
  backfilling new consumers, but limited compared to Kinesis/Kafka.
- **Kinesis** — consumers hold a shard iterator and can seek to any timestamp within the
  retention window (1–365 days). Multiple independent consumers can read the same stream
  at different offsets simultaneously. Enhanced fan-out gives each consumer dedicated 2 MB/s
  read throughput.
- **Kafka** — consumers commit offsets independently. You can reset a consumer group offset
  to any point in the log (subject to retention/compaction). The broker retains all messages
  until the retention policy removes them. New consumers can replay the entire history.

### Consumer Model

- **SNS** — push-based fan-out. SNS delivers to every subscribed endpoint (SQS, Lambda,
  HTTP, email, SMS) simultaneously. Adding a new consumer = subscribe a new endpoint.
- **SQS** — pull-based, competing consumers. Multiple workers poll the same queue; each
  message is processed by exactly one worker. Scales by adding workers.
- **EventBridge** — push-based, rule-matched. You define rules on the bus; only events
  that match the rule pattern are forwarded to the target. One bus, many rules, many
  independent targets.
- **Kinesis** — pull-based (polling via `GetRecords`) or push-based (Enhanced Fan-Out via
  HTTP/2 `SubscribeToShard`). Each shard supports up to 5 concurrent polling consumers
  without Enhanced Fan-Out; with EFO, each registered consumer gets its own 2 MB/s pipe.
- **Kafka** — pull-based consumer groups. All consumers in the same group share partitions
  (each partition assigned to exactly one consumer). Independent consumer groups each get
  full copies of all messages.

### Throughput Ceiling

- **SNS** — 300 publish/s for standard topics; 300 msg/s (up to 3 000 with high throughput
  mode) for FIFO. SNS itself rarely becomes the bottleneck; the SQS queues or Lambda
  downstream are more likely constraints.
- **SQS (standard)** — no published per-queue limit; effectively unlimited for most workloads.
  FIFO is capped at 300 msg/s (3 000 with batching).
- **EventBridge** — default 10 000 events/s per bus (soft limit, can be raised). Suitable
  for routing workloads, not high-frequency streaming.
- **Kinesis** — 1 MB/s or 1 000 records/s write per shard; 2 MB/s read per shard (shared
  across up to 5 consumers, or 2 MB/s per registered Enhanced Fan-Out consumer). Scale
  by adding shards (resharding). Suitable for hundreds of MB/s with many shards.
- **Kafka (MSK)** — throughput scales with partitions and brokers. A well-tuned MSK cluster
  handles hundreds of MB/s to GB/s. Partition count drives parallelism.

### Operational Overhead

- **SNS** — zero infrastructure. Create a topic, subscribe endpoints. No patching, no
  scaling config, no offset management.
- **SQS** — zero infrastructure. DLQ wiring, visibility timeout, and retention period are
  the only knobs that matter for most teams.
- **EventBridge** — zero infrastructure. Schema registry (optional) adds some complexity.
  Rule and archive management is the main operational surface.
- **Kinesis** — medium. You manage shard count (too few = throttling, too many = cost).
  Consumer checkpointing is your responsibility (KCL or manual iterator management). Shard
  splitting/merging requires care.
- **Kafka (MSK)** — high. You manage broker fleet size, replication factor, partition count,
  retention bytes/time, log compaction settings, consumer group lag monitoring, and
  upgrades. MSK reduces but does not eliminate this burden (you still manage broker configs,
  Kafka versions, and topic administration).

---

## When to Use What

| Use case | Recommended service |
|----------|---------------------|
| Broadcast one event to N independent consumers | **SNS → SQS** (fan-out) |
| Route events by content/attribute to different targets | **EventBridge** |
| Work queue (one message processed by one worker) | **SQS** |
| Strict ordering within a logical entity | **SQS FIFO** (low volume) or **Kinesis** / **Kafka** (high volume) |
| Replay events for a new consumer or backfill | **Kinesis** or **Kafka** |
| High-throughput streaming (>10 k events/s sustained) | **Kinesis** or **Kafka** |
| Complex event patterns, schema registry, long retention | **Kafka (MSK)** |
| Serverless, low-ops, small-to-medium scale | **SNS + SQS** or **EventBridge** |

---

## Why This Project Uses SNS + SQS + EventBridge (Not Kinesis or Kafka)

The campaign-fanout project is a learning scaffold for a medium-scale B2B SaaS use case:

- **Message rate** is low-to-medium (hundreds/s peak, not thousands/s sustained).
- **Ordering** is not required — downstream consumers (email, SMS, analytics) are idempotent
  and can handle duplicates.
- **Replay** is not required for the initial architecture — a DLQ captures failures for
  manual re-drive.
- **Operational overhead** should be near-zero — the goal is to understand fan-out patterns,
  not to operate a Kafka cluster.
- **Fan-out** (one event → N independent consumers) is the primary pattern, and SNS is the
  canonical AWS tool for exactly this.

EventBridge is added alongside SNS to explore **content-based routing** — the EventBridge
`campaign-bus` will eventually carry rules that filter events by type or tenant, forwarding
only relevant subsets to specific targets. This is a complementary concern to SNS fan-out:
SNS broadcasts to all; EventBridge filters then routes.
