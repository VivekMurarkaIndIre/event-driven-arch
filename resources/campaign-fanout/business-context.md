# campaign-fanout — Business Context

## Problem Statement

When a marketing campaign is created or changes state, multiple downstream systems need to
react independently:

- **Email sender** — renders and delivers campaign emails
- **SMS sender** — dispatches SMS messages to opted-in contacts
- **Push sender** — triggers mobile push notifications
- **Analytics ingester** — records the event for reporting and attribution

Each system has different SLAs, scaling profiles, and failure modes. Coupling them through
a synchronous API call chain creates a brittle single point of failure and forces every
consumer to scale together.

## Solution: Async Fan-out

A single `campaign-created` SNS topic broadcasts to N independent SQS queues. Each consumer
reads from its own queue at its own pace. A failure in the email sender does not affect the
analytics ingester. A spike in email volume does not slow down SMS delivery.

## Business Value

| Concern | Without fan-out | With fan-out |
|---------|----------------|--------------|
| Consumer failure blast radius | Entire pipeline fails | Only that consumer's queue backs up |
| Independent scaling | Must over-provision all services together | Each consumer auto-scales independently |
| Adding a new consumer | Requires changing the producer | Subscribe a new SQS queue to the topic |
| Debugging failures | Logs scattered across services | DLQ captures every failed message with full payload |

## Pros

- **Loose coupling** — producer doesn't know or care how many consumers exist
- **Durability** — SQS retains messages for up to 14 days (DLQ), surviving consumer outages
- **Operational visibility** — DLQ depth is a direct metric for consumer health
- **Easy extensibility** — adding a new downstream system is a single `Subscribe` call

## Cons

- **At-least-once delivery** — consumers must be idempotent; duplicate messages are possible
- **No ordering guarantee** (standard queues) — consumers must tolerate out-of-order events
- **Increased infrastructure** — N queues + N DLQs + topics to provision, monitor, and pay for
- **Eventual consistency** — downstream state reflects the event stream, not a single source of truth; compensating transactions are harder to reason about

## EventBridge's Role

EventBridge sits alongside SNS/SQS for rule-based routing — e.g. "only send `campaign-activated`
events to the billing service". Where SNS is broadcast-to-all, EventBridge is filter-then-route.
The two patterns compose naturally: use SNS for fan-out, EventBridge for conditional routing.
