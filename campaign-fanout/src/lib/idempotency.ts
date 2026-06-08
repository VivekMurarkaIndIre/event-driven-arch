import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb";

// ---------------------------------------------------------------------------
// Delivery guarantees — at-least-once, at-most-once, exactly-once
// ---------------------------------------------------------------------------
//
// AT-LEAST-ONCE  (SQS standard and FIFO — the default guarantee)
//   The broker guarantees every message is eventually delivered, but may deliver
//   it more than once. Duplicate sources:
//     1. Consumer receives → processes → crashes before DeleteMessage. SQS
//        re-delivers once the visibility timeout expires.
//     2. SQS occasionally produces a spontaneous duplicate within the standard
//        queue, even with a healthy consumer. Rare but documented.
//     3. Producer retried a failed SNS publish, creating a new SQS message with
//        a different MessageId but the same business event.
//   Consequence: consumers must be idempotent. This store is the mechanism.
//
// AT-MOST-ONCE  (not used here; requires explicit design)
//   A message is delivered zero or one times — never twice. Pattern: receive
//   message → delete immediately → process. If processing fails, the message
//   is gone with no retry or DLQ. Acceptable only when duplicate side effects
//   (e.g. charging a card twice) are worse than missed events. Rare in practice.
//
// EXACTLY-ONCE  (an approximation, not a true guarantee)
//   The closest AWS gets is the combination of:
//     SNS FIFO:      MessageDeduplicationId prevents duplicate ENQUEUING within
//                    a 5-minute window. A second PublishCommand with the same ID
//                    is silently discarded — the message is never written to the
//                    queue a second time.
//     SQS FIFO:      Per-MessageGroupId ordering prevents concurrent delivery of
//                    the same group to multiple consumers.
//     DynamoDB store (this file): conditional write prevents duplicate PROCESSING
//                    when two consumer replicas race on the same message.
//
//   WHY FIFO DEDUPLICATION ≠ EXACTLY-ONCE PROCESSING
//   SNS FIFO deduplication operates at the publish → enqueue boundary. It stops
//   the same logical event from being enqueued twice within five minutes. After
//   the window closes, a re-publish with the same ID is treated as a new message.
//
//   Even with FIFO deduplication active, SQS delivers at-least-once TO THE
//   CONSUMER. If a consumer receives the message, successfully sends the email,
//   but then crashes before calling DeleteMessage, SQS re-delivers the message
//   after the visibility timeout — because SQS has no way to know the consumer
//   already succeeded. The message was never duplicated at the enqueue layer;
//   the re-delivery is entirely the SQS-to-consumer leg.
//
//   This is why FIFO deduplication only prevents duplicate ENQUEUING, not
//   duplicate PROCESSING. A consumer-side idempotency layer is always required,
//   regardless of topic/queue type.
// ---------------------------------------------------------------------------

// Builds a composite key covering two distinct sources of duplicate delivery:
//
//   messageId — the SQS Message.MessageId. Deduplicates infrastructure-level
//     re-deliveries (crash-before-delete, rare SQS spontaneous duplicate).
//
//   eventId — a business-level identifier from the payload (e.g. campaignId).
//     Deduplicates producer-level retries where a fresh SNS publish creates a
//     new SQS message with a different MessageId carrying the same business event.
//
// Together they protect against all known duplicate sources without over-deduplicating:
// two genuinely different events for the same campaign (e.g. campaign-created and
// campaign-activated) share the same eventId but arrive in separate SQS messages
// with different MessageIds — both are processed correctly.
export function makeIdempotencyKey(messageId: string, eventId: string): string {
  return `${messageId}:${eventId}`;
}

export interface IdempotencyStore {
  // Returns true if the key was already processed — caller should skip.
  has(key: string): Promise<boolean>;
  // Records the key as processed. May throw on a concurrent write race
  // (ConditionalCheckFailedException from DynamoDB); the caller's try/catch
  // treats this as a BatchItemFailure so the message is retried, at which
  // point has() returns true and it is cleanly skipped.
  add(key: string): Promise<void>;
}

// In-memory fallback for single-process, non-critical consumers (analytics,
// notifications). Not durable across restarts and not shared across replicas.
// Sufficient when the business operation is cheap to repeat (log write) or when
// the downstream system is naturally idempotent (upsert to a time-series store).
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly seen = new Set<string>();

  has(key: string): Promise<boolean> {
    return Promise.resolve(this.seen.has(key));
  }

  add(key: string): Promise<void> {
    this.seen.add(key);
    return Promise.resolve();
  }
}

// DynamoDB-backed idempotency store.
//
// Solves two problems that InMemoryIdempotencyStore cannot:
//   Durability: survives consumer process restarts. A message re-delivered after
//     a crash is recognised as a duplicate on the very first has() call.
//   Shared state: multiple consumer replicas (Lambda, ECS) share the same table.
//     Only the first replica to successfully call add() processes the message.
//
// Conditional write (attribute_not_exists(pk)):
//   If two consumers race — both pass has() before either calls add() — only one
//   PutItem succeeds. The other gets ConditionalCheckFailedException. The "losing"
//   consumer's try/catch returns a BatchItemFailure; SQS re-delivers after the
//   visibility timeout; has() then returns true and the message is skipped.
//   This is optimistic concurrency: the race window exists but self-heals without
//   data loss or permanent duplicate processing.
//
// TTL (ttl attribute, Unix seconds):
//   Enable TTL on the `ttl` attribute via UpdateTimeToLive (AWS console or CLI).
//   Set the TTL longer than the SQS message retention period (4 days default) so
//   a key is never evicted while its message could still be re-delivered.
//   This store uses 7 days. Without TTL the table grows unboundedly.
export class DynamoDBIdempotencyStore implements IdempotencyStore {
  private static readonly TABLE = "IdempotencyKeys";
  // 7 days: safely longer than the 4-day SQS default retention.
  private static readonly TTL_SECONDS = 7 * 24 * 60 * 60;

  constructor(private readonly dynamo: DynamoDBClient) {}

  async has(key: string): Promise<boolean> {
    const { Item } = await this.dynamo.send(
      new GetItemCommand({
        TableName: DynamoDBIdempotencyStore.TABLE,
        Key: { pk: { S: key } },
      }),
    );
    return Item !== undefined;
  }

  async add(key: string): Promise<void> {
    const ttl = Math.floor(Date.now() / 1000) + DynamoDBIdempotencyStore.TTL_SECONDS;
    await this.dynamo.send(
      new PutItemCommand({
        TableName: DynamoDBIdempotencyStore.TABLE,
        Item: {
          pk: { S: key },
          createdAt: { S: new Date().toISOString() },
          ttl: { N: String(ttl) },
        },
        ConditionExpression: "attribute_not_exists(pk)",
      }),
    );
  }
}
