# event-driven-design

A learning monorepo exploring event-driven architecture patterns in TypeScript — SNS fan-out,
SQS dead-letter queues, EventBridge routing, and DynamoDB, developed locally against LocalStack.

---

## Projects

| Project | Description |
|---------|-------------|
| [campaign-fanout](campaign-fanout/) | Fan-out pattern: one SNS topic broadcasts campaign events to multiple independent SQS consumers |

---

## Quick Start

```bash
# Start LocalStack (all projects share it)
docker compose -f campaign-fanout/docker-compose.yml up -d

# Wait for LocalStack to be healthy, then provision AWS resources
cd campaign-fanout && npm install && npm run infra:setup
```

---

## Commands — campaign-fanout

```bash
# Infrastructure
docker compose up -d          # start LocalStack (SNS, SQS, EventBridge, DynamoDB)
docker compose down           # stop LocalStack
docker compose down -v        # stop and wipe the LocalStack volume

# Dependencies
npm install                   # install all packages

# AWS resource provisioning
npm run infra:setup           # create SNS topics, SQS queues + DLQs, EventBridge buses, DynamoDB tables

# Publishing
npm run publish               # publish a test CampaignPublished event to SNS via LocalStack

# Tests
npm test                      # run vitest in watch mode
npm run test -- --run         # run vitest once (CI mode)

# Type checking
npx tsc --noEmit              # type-check without emitting output
```

---

## Resources

Architecture decisions, learnings, and business context live in [`resources/`](resources/):

| Path | Contents |
|------|----------|
| [resources/campaign-fanout/architecture.md](resources/campaign-fanout/architecture.md) | Infrastructure layout, service choices, design decisions |
| [resources/campaign-fanout/learnings.md](resources/campaign-fanout/learnings.md) | Key learnings, gotchas, TypeScript/SDK insights |
| [resources/campaign-fanout/business-context.md](resources/campaign-fanout/business-context.md) | Problem statement, use case, pros/cons |
| [campaign-fanout/docs/messaging-decision.md](campaign-fanout/docs/messaging-decision.md) | SNS vs SQS vs EventBridge vs Kinesis vs Kafka comparison |
| [campaign-fanout/docs/decision-tree.md](campaign-fanout/docs/decision-tree.md) | Decision tree: pick a service given rate, ordering, replay, routing requirements |

These files are updated on every `/commit`.
