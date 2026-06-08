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
