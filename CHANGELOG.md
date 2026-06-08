# Changelog

All notable changes to this learning repo are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
