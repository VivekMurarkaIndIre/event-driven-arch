# Changelog

All notable changes to this learning repo are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] — 2026-06-08

### Added
- `src/consumers/dlqMonitor.ts`: `DlqMonitor` class — polls all 4 DLQs every 10 s, peeks with `VisibilityTimeout: 5`, emits one structured JSON alert per message to stdout, then releases messages back with `ChangeMessageVisibilityBatch(0)` so they immediately return to the DLQ
- `src/scripts/replayDlq.ts`: drain-and-replay script — takes a DLQ name, derives the main queue name by stripping `-dlq`, re-queues each message to the main SQS queue with a `replayedAt` message attribute, and deletes from the DLQ only after successful `SendMessage`; skips messages that already carry `replayedAt` to prevent infinite replay loops
- `npm run dlq:monitor` and `npm run dlq:replay -- <dlq-name>` scripts

### Changed
- `src/infra/setup.ts`: `maxReceiveCount` lowered from 5 → 3 with an expanded comment explaining why `maxReceiveCount: 1` is dangerous and that the safe alert threshold is `ApproximateNumberOfMessages > 0`
- `src/infra/setup.ts`: SQS queue creation now goes through `ensureQueue()` helper — catches `QueueNameExists` (thrown when any attribute differs from the existing queue) and falls back to `GetQueueUrl` + `SetQueueAttributes` to update in place; `infra:setup` is now safe to re-run after any queue attribute change

---

## [Unreleased] — 2026-06-08

### Added
- `src/consumers/emailConsumer.ts`: `EmailConsumer` reading from `campaign-processor`; no filter — receives all tenant tiers
- `src/consumers/notificationConsumer.ts`: `NotificationConsumer` reading from `campaign-notifier`; trusts the SNS broker filter to deliver only paid-tier events
- `src/scripts/publishBatch.ts`: publishes 10 events with mixed `tenantTier` values (4 free, 3 pro, 3 enterprise); prints per-event routing and expected queue depths
- `src/scripts/consume-email.ts` and `src/scripts/consume-notification.ts`: runnable consumer entry points
- `npm run publish:batch`, `npm run consume:email`, `npm run consume:notification` scripts
- SNS → SQS subscriptions now created inside `infra:setup` (`createSnsSubscriptions`); filter policy `{ tenantTier: ["pro", "enterprise"] }` on `campaign-notifier` subscription
- Large comment block in `setup.ts` comparing SNS filter policies (attribute-based, broker-side, no body access) vs EventBridge rules (content-based, body + metadata, more expressive)

### Fixed
- `infra:setup` is now fully idempotent: `createEventBridgeBuses` and `createDynamoTables` catch `ResourceAlreadyExistsException` / `ResourceInUseException` and continue, so re-running after a partial LocalStack restart no longer fails

### Changed
- `createSnsTopics` and `createSqsQueuesWithDlqs` now return `Map<string, string>` (name → ARN) consumed by `createSnsSubscriptions`

---

## [Unreleased] — 2026-06-08

### Added
- `src/consumers/BaseConsumer.ts`: abstract generic SQS consumer with long polling (`WaitTimeSeconds: 20`), configurable batch size, visibility timeout extension loop (fires at `visibilityTimeout / 2`), SNS envelope unwrapping, partial batch response (`batchItemFailures`), and graceful error containment
- `src/lib/idempotency.ts`: `InMemoryIdempotencyStore` and `makeIdempotencyKey(messageId, eventId)` — composite key covering both SQS re-deliveries and producer-level retry duplicates
- `src/consumers/analyticsConsumer.ts`: `AnalyticsConsumer extends BaseConsumer<CampaignPublished>` with runtime Zod re-validation, idempotency guard, and `createAnalyticsConsumer` factory
- `src/scripts/consume.ts`: runnable consumer script with `SIGINT` handler for graceful shutdown
- `npm run consume` script in `package.json`

---

## [Unreleased] — 2026-06-08

### Added
- `src/events/schemas.ts`: Zod schema (`CampaignPublishedSchema`) and inferred TypeScript type for the `CampaignPublished` event, covering `campaignId`, `tenantId`, `tenantTier`, `campaignType`, `audienceSize`, `correlationId`, and `publishedAt`
- `src/publisher/campaignPublisher.ts`: SNS publisher function with `tenantId`, `tenantTier`, and `eventType` as message attributes; SHA-256 deduplication ID computed from `campaignId + version`; detailed comment explaining standard-vs-FIFO deduplication behaviour
- `src/scripts/publish.ts`: runnable test script that validates a payload through Zod before publishing
- `npm run publish` script in `package.json`
- `zod` added as a production dependency

---

## [Unreleased] — 2026-06-08

### Added
- `campaign-fanout/docs/messaging-decision.md`: comparison of SNS, SQS, EventBridge, Kinesis, and Kafka across delivery guarantee, ordering, replay, consumer model, throughput ceiling, and operational overhead
- `campaign-fanout/docs/decision-tree.md`: ASCII decision tree selecting a service given tenant count, message rate, ordering, replay, and content-routing requirements; includes scoring table and applied example for campaign-fanout
- Inline comments throughout `src/infra/setup.ts` explaining why each service is used (SNS as broadcast hub, SQS per-consumer queue rationale, DLQ sequencing constraint, EventBridge as complement for content routing, DynamoDB key design)

### Fixed
- Added `"types": ["node"]` to `tsconfig.json` — `lib: ["ES2022"]` does not pull in Node.js globals; without this, `console` is unresolved under strict type checking

---

## [Unreleased] — 2026-06-08

### Added
- `campaign-fanout` project: TypeScript monorepo scaffold with strict tsconfig
- LocalStack `docker-compose.yml` running SNS, SQS, EventBridge, and DynamoDB
- `src/infra/setup.ts` provisioning all AWS resources via SDK v3 against `http://localhost:4566`
  - 4 SNS topics: `campaign-created/updated/deleted/activated`
  - 4 SQS queues each with a paired DLQ: `campaign-processor/notifier/analytics/audit`
  - 2 EventBridge buses: `campaign-bus`, `campaign-dlq-bus`
  - 1 DynamoDB table: `Campaigns` with PK/SK and `StatusCreatedAtIndex` GSI
- `npm run infra:setup` script via `tsx`
- `vitest` v4 test runner
- `/commit` Claude Code skill (`.claude/commands/commit.md`) — updates README, CHANGELOG, and resources on every commit
- `resources/` folder with architecture notes, learnings, and business context for `campaign-fanout`
