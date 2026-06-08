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

- **`replayedAt` message attribute breaks infinite DLQ replay loops.** On replay, add `replayedAt: ISO timestamp` to the SQS `MessageAttributes`. If the replayed message fails again and lands back in the DLQ, it carries this attribute. The replay script detects it and skips rather than re-queuing indefinitely. A twice-failed message needs manual investigation, not another automated replay.
