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
