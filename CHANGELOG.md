# Changelog

All notable changes to this learning repo are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] — 2026-06-08

### Added
- `src/infra/setup.ts`: `createEventBridgeRulesAndQueues()` provisions `campaign-survey` + `campaign-high-volume` queues (each with DLQ), then creates two EventBridge rules on `campaign-bus` — `route-survey-campaigns` (matches `detail.campaignType = "survey"`) and `route-high-volume-campaigns` (matches `detail.audienceSize > 10000`) — and links each rule to its target queue via `PutTargets`; large comment block explaining EB vs SNS filter differences, 256 KB limit, 10K/s quota, and rule independence
- `src/publisher/eventBridgePublisher.ts`: `putCampaignEvent()` — single-entry `PutEvents` call with `FailedEntryCount` guard; includes comprehensive comment covering EB pattern operators, partial failure semantics, and when to choose EB over SNS
- `src/consumers/surveyConsumer.ts`: `SurveyConsumer extends BaseConsumer<CampaignPublished>` — processes survey campaigns routed by EventBridge; in-memory idempotency; 30 s visibility timeout
- `src/consumers/highVolumeConsumer.ts`: `HighVolumeConsumer extends BaseConsumer<CampaignPublished>` — processes large-audience campaigns; 60 s visibility timeout to accommodate slower delivery pipeline
- `src/scripts/consume-survey.ts`, `consume-high-volume.ts`: consumer entry points with SIGINT handler
- `src/scripts/put-event.ts`: test script publishing two events to `campaign-bus` — one survey (audienceSize 500) and one email with audienceSize 50 000 — demonstrating single-rule and potential dual-rule matching
- `npm run consume:survey`, `consume:high-volume`, `eb:put` scripts

### Changed
- `src/consumers/BaseConsumer.ts`: `parseMessages` now handles three envelope types — SNS (`Type: "Notification"` → re-parse `Message` string), EventBridge (`source` + `detail-type` + `detail` → `detail` is already a parsed object, no re-parse), and direct body (no envelope)
- `src/events/schemas.ts`: added `"survey"` to `CampaignTypeSchema` — required so `SurveyConsumer` can validate EventBridge-routed events with `campaignType: "survey"`

---

## [Unreleased] — 2026-06-08

### Added
- `campaign-events.fifo` SNS FIFO topic + `campaign-processor.fifo` / `campaign-processor-dlq.fifo` FIFO queue pair, provisioned in new `createFifoResources()` in `setup.ts`; FIFO topic subscribed to FIFO queue, separate from the standard fan-out stack so existing consumers are unaffected
- `IdempotencyKeys` DynamoDB table (HASH key `pk`; `ttl` attribute for future TTL enablement) provisioned alongside the `Campaigns` table in `createDynamoTables()`
- `DynamoDBIdempotencyStore` in `src/lib/idempotency.ts`: durable, multi-replica idempotency backed by `PutItem` with `ConditionExpression: "attribute_not_exists(pk)"` — `ConditionalCheckFailedException` from a concurrent write race propagates as a `BatchItemFailure` so SQS re-delivers and `has()` short-circuits on the retry
- `topicType?: "standard" | "fifo"` option in `PublishOptions`; when `"fifo"`, `publishCampaignEvent` includes `MessageGroupId: campaignId` and `MessageDeduplicationId: SHA-256(campaignId:version)` in the `PublishCommand`; neither field is sent for standard topics (SNS rejects them with `InvalidParameter`)
- Large delivery-guarantees comment in `src/publisher/campaignPublisher.ts` and `src/lib/idempotency.ts` covering at-least-once, at-most-once, exactly-once, and why FIFO deduplication prevents duplicate enqueuing but not duplicate processing

### Changed
- `IdempotencyStore` interface methods (`has`, `add`) are now `Promise<boolean>` / `Promise<void>`; `InMemoryIdempotencyStore` wraps in `Promise.resolve()` — no behaviour change, but all consumers now `await` the calls
- `EmailConsumer` factory (`createEmailConsumer`) switched from `InMemoryIdempotencyStore` to `DynamoDBIdempotencyStore`; points to the FIFO queue (`campaign-processor.fifo`)
- `consume-email.ts` updated to use the FIFO queue URL
- `ensureQueue` in `setup.ts` now filters `FifoQueue` and `ContentBasedDeduplication` from the `SetQueueAttributes` fallback call — these attributes are immutable after queue creation and cannot be updated

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
