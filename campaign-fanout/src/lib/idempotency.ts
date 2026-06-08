// Builds a composite key that covers two distinct classes of duplicate:
//
//   messageId  — the SQS Message.MessageId. SQS guarantees at-least-once
//     delivery: the same physical message can be received more than once if the
//     consumer crashes after ReceiveMessage but before DeleteMessage, or because
//     SQS occasionally produces a duplicate within the visibility window even
//     when the consumer is healthy. Keying on messageId deduplicates those
//     infrastructure-level re-deliveries.
//
//   eventId  — a business-level identifier from the event payload (e.g. campaignId).
//     A producer that retried a failed SNS publish may create a second SQS message
//     with a completely different messageId but an identical business event.
//     messageId alone would not catch that case; eventId does.
//
// Together they protect against every known source of duplicate delivery without
// over-deduplicating: two genuinely different events for the same campaign (e.g.
// campaign-created and campaign-activated) will have the same eventId but arrive
// in separate SQS messages with different messageIds, so both are processed.
export function makeIdempotencyKey(messageId: string, eventId: string): string {
  return `${messageId}:${eventId}`;
}

export interface IdempotencyStore {
  has(key: string): boolean;
  add(key: string): void;
}

// In-memory idempotency store for single-process consumers.
//
// Safe only when one process polls one queue. Limitations:
//   - State is lost on process restart: messages delivered after a restart are
//     processed again, even if they were processed before the crash.
//   - Multiple replicas each maintain their own store: a message routed to
//     replica B after replica A already processed it will be processed again.
//
// Production replacement: a DynamoDB conditional write keyed on the idempotency
// key with attribute_not_exists(pk) — atomic, durable, and shared across all
// replicas. A Redis SETNX works too if eventual consistency is acceptable.
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly seen = new Set<string>();

  has(key: string): boolean {
    return this.seen.has(key);
  }

  add(key: string): void {
    this.seen.add(key);
  }
}
