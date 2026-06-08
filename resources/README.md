# Resources

Architecture notes, learnings, and business context for every project in this monorepo.
These files are updated automatically by the `/commit` skill on each commit.

| File | Contents |
|------|----------|
| [campaign-fanout/architecture.md](campaign-fanout/architecture.md) | Infrastructure layout, AWS service choices, and design decisions |
| [campaign-fanout/learnings.md](campaign-fanout/learnings.md) | Key learnings, gotchas, and pattern insights |
| [campaign-fanout/business-context.md](campaign-fanout/business-context.md) | Problem statement, use case, pros/cons |
| [../campaign-fanout/docs/messaging-decision.md](../campaign-fanout/docs/messaging-decision.md) | SNS vs SQS vs EventBridge vs Kinesis vs Kafka — six-dimension comparison |
| [../campaign-fanout/docs/decision-tree.md](../campaign-fanout/docs/decision-tree.md) | Decision tree for picking a messaging service |
| [campaign-fanout/complete_architecture_module8.svg](campaign-fanout/complete_architecture_module8.svg) | Complete architecture diagram — all transports, queues, DLQs, consumers, DynamoDB, operational tooling |
