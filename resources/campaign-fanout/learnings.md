# campaign-fanout — Learnings

### 2025-06-08

- **DLQ ARN must be resolved before the main queue is created.** The `RedrivePolicy` attribute
  takes the DLQ's ARN, not its URL. You must call `GetQueueAttributes` on the DLQ first and
  extract `QueueArn`, otherwise the `CreateQueue` call for the main queue fails silently or
  returns an error about an invalid policy.

- **LocalStack credential format:** Any non-empty string works for `accessKeyId` /
  `secretAccessKey` when pointing at LocalStack. Convention is `"test"/"test"` — using real
  credential env vars accidentally works too, but is confusing in logs.

- **`noUncheckedIndexedAccess` forces explicit nullability.** `dlqAttrs?.["QueueArn"]` returns
  `string | undefined` under this flag even though the type definition says `string`. The
  explicit null-check guard (`if (!dlqArn) throw`) is required — TypeScript will not compile
  without it. This is intentional: it surfaces a real runtime failure mode (attribute may be
  absent if the queue doesn't exist yet).

- **`exactOptionalPropertyTypes` interacts with AWS SDK response types.** Some SDK response
  objects have optional fields typed as `T | undefined`. Assigning them to a variable typed
  `T` requires an explicit assertion or guard — which is what you want; it forces handling the
  "request succeeded but attribute missing" case.

- **vitest v4 requires Node >=22.12 strictly** (via vite 8 / rolldown). On Node 22.2 you get
  an `EBADENGINE` warning but it still runs. Pin `engines.node` in package.json if you want
  a hard enforcement.

### 2026-06-08
- **`lib: ["ES2022"]` in tsconfig does not include Node.js globals.** `console`, `process`,
  `Buffer`, etc. are not part of the ES spec — they are Node.js runtime additions typed by
  `@types/node`. Without `"types": ["node"]` in compilerOptions, TypeScript raises
  `TS2584: Cannot find name 'console'` even though the code runs fine. Fix: add
  `"types": ["node"]` (the package is already installed as a devDependency).

- **One SNS topic per lifecycle event vs. one omnibus topic.** Using separate topics
  (`campaign-created`, `campaign-updated`, etc.) lets consumers subscribe selectively
  and keeps the SNS subscription filter policy simple. A single `campaign-events` topic
  forces each consumer to deserialize every message and branch on `eventType` — tight
  coupling to the full schema.

- **SQS vs. Kinesis/Kafka decision heuristic.** If you don't need replay and message rate
  is under ~3 000/s, SQS standard is almost always the right answer: zero operational
  overhead, no shard math, no offset management. Reach for Kinesis when you need ordered
  replay; reach for Kafka when you need ordered replay *and* rate > 10 000/s or you need
  compacted logs.

- **EventBridge and SNS are complementary, not alternatives.** SNS is broadcast-to-all;
  EventBridge is filter-then-route. A single event can be published to both: SNS fans
  out to all consumers, EventBridge carries it to conditional targets (e.g. billing only
  on `campaign-activated`). The two patterns compose; you don't choose one over the other.

### 2026-06-08
- **Zod `z.string().datetime()` rejects `Date` objects.** The schema must receive an ISO-8601
  string (`new Date().toISOString()`), not a raw `Date`. This is intentional: JSON has no
  native date type, and forcing an explicit string eliminates silent timezone or millisecond
  precision differences between serializers.

- **SNS rejects `MessageDeduplicationId` on standard topics.** Only FIFO topics honour the
  field; passing it to a standard `PublishCommand` returns an `InvalidParameter` error. The
  dedup key is still worth computing and logging — use it as the idempotency key in the
  consumer's DynamoDB conditional write (`attribute_not_exists(deduplicationId)`).

- **SNS message attributes vs. message body.** Attributes are indexed by SNS and evaluated
  by subscription filter policies *before* delivery. Body content is opaque to the broker.
  Any field you want to filter on at the queue subscription layer must be in attributes —
  even if it duplicates a body field. The body remains the authoritative source of truth.

- **`node:crypto` is the right import specifier for Node built-ins with `module: NodeNext`.**
  The bare `"crypto"` specifier works but the `node:` prefix makes it explicit that this is
  a Node.js built-in, not an npm package, and avoids shadowing by a hypothetically installed
  `crypto` package. TypeScript resolves both identically given `"types": ["node"]`.

- **Validate at the producer boundary, not in the transport layer.** Putting `schema.parse()`
  in the script that assembles the raw object (not in the publisher function) means the
  publisher receives a typed `CampaignPublished` value — no `unknown` casting, no runtime
  guards inside a function that is supposed to be a pure transport concern.

### 2026-06-08
- **`tsx --eval` with top-level await fails with CJS output format.** `tsx --eval` defaults to
  CommonJS, which does not support top-level `await`. The error is:
  `Top-level await is currently not supported with the "cjs" output format`.
  Fix: put the code in a `.ts` file (picked up as ESM by `"type": "module"` in package.json)
  and run it with `npx tsx src/scripts/file.ts`. Never use `--eval` for async entry points.

- **SNS wraps the payload in an envelope when delivering to SQS.** The SQS message body is
  not the raw event JSON — it is an outer JSON object with `{ "Type": "Notification", "Message": "<inner JSON string>", ... }`. The inner payload lives in `Message`. Any consumer
  that does `JSON.parse(sqs.Body)` and expects the event directly will get the envelope object
  instead. The fix (implemented in `BaseConsumer.parseMessages`) is to detect `Type === "Notification"` and parse `Message` as the actual payload.

- **Visibility timeout extension interval must be strictly less than the timeout itself.**
  `ChangeMessageVisibilityBatch` resets the deadline to `visibilityTimeout` seconds *from now*.
  If the extension fires after the deadline has already expired, the message is already visible
  to other consumers — `ChangeMessageVisibility` on an expired message returns
  `InvalidParameterValue`. The safe interval is `visibilityTimeout / 2`, ensuring the timer
  always fires while at least half the timeout remains.

- **Mark processed AFTER the write succeeds, not before.** If you add the idempotency key
  before the downstream write and then crash mid-write, the key exists but the side effect
  never happened — the message is permanently lost with no retry possible. Mark after success
  so a crash leaves the key absent, the message re-delivers, and the write is retried.

- **`for...of` over a typed array is safe under `noUncheckedIndexedAccess`.** The flag adds
  `| undefined` only to index-signature access (`arr[i]`), not to `for...of` iteration
  variables. Iterating with `for (const item of arr)` gives `item: T`, not `item: T | undefined`,
  so no extra guards are needed inside loop bodies.

### 2026-06-08
- **SNS filter policies match on attributes, not the body — and a missing attribute counts as non-matching.** If a producer publishes without setting `tenantTier` as a `MessageAttribute`, SNS evaluates the filter and finds no value to compare against. The message is silently dropped for that subscription. This is the correct behaviour for a badly-behaved producer, but it can surprise you during development if you forget to set the attribute.

- **`FilterPolicyScope` must be `"MessageAttributes"` (not `"MessageBody"`) when filtering on MessageAttributes.** The default is `MessageAttributes`, but specifying it explicitly in the `SubscribeCommand.Attributes` makes the intent clear and avoids confusion if the default ever changes.

- **SNS `SubscribeCommand` requires the queue ARN as `Endpoint`, not the queue URL.** Using the queue URL returns a validation error. The ARN must be retrieved via `GetQueueAttributes` after creation — it is not returned by `CreateQueue`. This is the same pattern used for the DLQ ARN in the redrive policy.

- **LocalStack `ResourceAlreadyExistsException` vs `ResourceInUseException`.** EventBridge throws `ResourceAlreadyExistsException`; DynamoDB throws `ResourceInUseException`. The error names are inconsistent across services — check the `err.name` field rather than the message string, and handle each service separately.

- **SNS `CreateTopic` and SQS `CreateQueue` are idempotent by the AWS API contract.** Calling them with the same name when the resource already exists returns the existing ARN/URL rather than an error. This makes topics and queues easy to provision safely on every deploy without explicit existence checks. EventBridge and DynamoDB do not share this property.

### 2026-06-08
- **SQS `CreateQueue` idempotency is attribute-exact, not just name-exact.** If any attribute (e.g. `RedrivePolicy.maxReceiveCount`) differs from the existing queue, SQS throws `QueueNameExists` rather than returning the existing URL. The only safe "upsert" pattern is: try `CreateQueue`, catch `QueueNameExists`, then call `GetQueueUrl` + `SetQueueAttributes` to update the existing queue in place.

- **SQS DLQ peek pattern: `ReceiveMessage(VisibilityTimeout: 5)` + `ChangeMessageVisibilityBatch(0)`.** To inspect DLQ messages without consuming them, receive with a short visibility timeout and immediately change it back to 0. This is the only way to read message content from SQS without deleting it — `GetQueueAttributes` gives you the count but not the body. The 5-second window is a race window; keep it as short as possible.

- **`maxReceiveCount: 1` fills DLQs with noise.** Any transient failure (network blip, cold start, process restart mid-batch) immediately DLQs the message. The DLQ then mixes noise with genuine failures, making `ApproximateNumberOfMessages > 0` an unreliable alert. Setting `maxReceiveCount ≥ 3` filters transient failures before they reach the DLQ, making the count a reliable signal — alert on 1, not on some higher threshold.

- **DLQ replay should target the main SQS queue directly, not SNS.** Re-publishing through SNS fans the message out to every subscribed queue, turning a single-queue replay into a broadcast. Publishing directly to the main SQS queue (`SendMessageCommand`) re-queues the message exactly where it failed, with all original attributes preserved plus a new `replayedAt` marker.

### 2026-06-08
### 2026-06-08
- **EventBridge `detail` is already a parsed object — do not `JSON.parse` it again.** When EventBridge delivers to an SQS target the SQS message body is the full EventBridge event JSON, with the business payload in `detail` as a parsed object. SNS puts the payload inside `Message` as a JSON *string* that must be re-parsed. The `BaseConsumer.parseMessages` handles this: `isEventBridgeEnvelope` detects the EB envelope and casts `outer.detail as TBody` directly; `isSnsEnvelope` detects the SNS envelope and calls `JSON.parse(outer.Message)`. Double-parsing the EB payload corrupts objects like dates into `"[object Object]"`.

- **EventBridge `PutRule` / `PutTargets` are idempotent upserts — no "already exists" error.** Unlike EventBridge `CreateEventBus` (which throws `ResourceAlreadyExistsException`), `PutRule` silently overwrites an existing rule with the same name. This makes rules safe to provision on every `infra:setup` run without an existence guard.

- **`PutEvents` does not throw on entry-level failure — check `FailedEntryCount`.** A `PutEvents` call can return HTTP 200 with `FailedEntryCount: 1`. The failure reason is in `Entries[i].ErrorCode` and `ErrorMessage`. If you only check for an exception you will silently lose events.

- **EventBridge numeric range pattern syntax: `[{ "numeric": [">", 10000] }]`.** The outer array is the "any of these conditions" wrapper. The inner array `[">", 10000]` is the operator-value pair. This is different from SNS numeric ranges, which use `[{ "numeric": [">=", 0, "<=", 100] }]` for closed ranges. EventBridge supports `=`, `!=`, `<`, `<=`, `>`, `>=`; SNS supports the same operators but only as a closed range syntax.

- **One EventBridge event can match multiple rules simultaneously.** A survey campaign with `audienceSize: 15000` matches both `route-survey-campaigns` (campaignType = "survey") and `route-high-volume-campaigns` (audienceSize > 10000). EventBridge delivers it to both target queues independently — neither rule "wins". Design consumers to handle this: `SurveyConsumer` and `HighVolumeConsumer` may receive the same event and must each process it for their own concern.

- **Adding a new enum value to a Zod schema (`"survey"`) is a breaking change for existing serialised data if strict parsing is used.** Any DLQ message serialised before the schema change that has `campaignType: "survey"` would have been invalid under the old schema and already in the DLQ. Conversely, adding the value makes the schema looser — old consumers that do `CampaignPublishedSchema.safeParse` will now accept events they previously rejected. Schema additions are backwards-compatible in the accept direction; removals are breaking.

- **SNS standard and FIFO topics cannot share subscriptions.** A FIFO SQS queue can only subscribe to a FIFO SNS topic; a standard SQS queue cannot subscribe to a FIFO topic. This is an AWS hard constraint, not a soft recommendation. Running both stacks in parallel is the only way to give some consumers FIFO semantics without migrating all consumers.

- **FIFO SQS queue DLQ must also be FIFO.** `CreateQueue` for a FIFO queue with a `RedrivePolicy` pointing to a standard DLQ throws `InvalidParameterValue`. Create the DLQ with `FifoQueue: "true"` first.

- **`FifoQueue` and `ContentBasedDeduplication` are immutable after queue creation.** `SetQueueAttributes` rejects them with `InvalidParameterValue`. Filter them out of the fallback update call in any "upsert" queue helper; otherwise a re-run that tries to update these attributes will always fail.

- **SNS FIFO deduplication window is 5 minutes; after that window a re-publish IS delivered.** If the producer re-publishes the same `MessageDeduplicationId` 6 minutes later, SNS treats it as a new message. Consumer-side idempotency (DynamoDB conditional write) remains the durable guarantee — it never expires within the SQS message retention period.

- **DynamoDB conditional write race: optimistic concurrency, not a hard lock.** Two consumers can both pass `has()` before either calls `add()`. Both may execute the business operation (send the email); only one `PutItem` with `attribute_not_exists(pk)` succeeds. The "loser" gets `ConditionalCheckFailedException`, which surfaces as a `BatchItemFailure`. On SQS re-delivery, `has()` returns true and the message is skipped cleanly. The race window is small (time between `has()` and `add()` across two processes) but real — this is why "mark after success" is the correct ordering, not "mark before work".

- **`IdempotencyStore` interface should be async even if the in-memory implementation is sync.** Async interfaces let you swap `InMemoryIdempotencyStore` for `DynamoDBIdempotencyStore` (or Redis, or any remote store) without changing the consumer contract. `Promise.resolve(value)` from the in-memory store has negligible overhead and keeps the interface honest about the semantics of the production implementation.

- **`replayedAt` message attribute breaks infinite DLQ replay loops.** On replay, add `replayedAt: ISO timestamp` to the SQS `MessageAttributes`. If the replayed message fails again and lands back in the DLQ, it carries this attribute. The replay script detects it and skips rather than re-queuing indefinitely. A twice-failed message needs manual investigation, not another automated replay.
