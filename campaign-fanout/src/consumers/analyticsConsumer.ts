import { SQSClient } from "@aws-sdk/client-sqs";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  InMemoryIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

export class AnalyticsConsumer extends BaseConsumer<CampaignPublished> {
  constructor(
    sqs: SQSClient,
    queueUrl: string,
    private readonly idempotency: IdempotencyStore,
  ) {
    super(sqs, {
      queueUrl,
      // Fetch up to 10 messages per poll. At-most-10 is the SQS ceiling; using
      // the maximum reduces the number of ReceiveMessage API calls per unit of
      // throughput, which matters at high message rates.
      batchSize: 10,
      // 30 s is a safe default for a fast analytics write. If the downstream
      // store (DynamoDB, BigQuery, etc.) has p99 latency > 30 s, increase this
      // — or the extension loop will fire mid-processing and SQS will see
      // duplicate deliveries when the timeout expires.
      visibilityTimeout: 30,
    });
  }

  override async processMessageBatch(
    messages: ParsedMessage<CampaignPublished>[],
  ): Promise<BatchItemFailure[]> {
    const failures: BatchItemFailure[] = [];

    for (const msg of messages) {
      // Re-validate against the Zod schema at receive time.
      // The TypeScript type on msg.body is correct only if the producer used the
      // same schema; a schema-mismatched producer (e.g. an older deploy still
      // sending v1 payloads) would pass TypeScript's static check but fail here.
      const result = CampaignPublishedSchema.safeParse(msg.body);
      if (!result.success) {
        console.error(
          `[AnalyticsConsumer] schema mismatch on message ${msg.messageId}:`,
          result.error.flatten(),
        );
        // Treat as failure so SQS retries. After maxReceiveCount retries the
        // message lands in the DLQ — a schema-incompatible message should not
        // silently vanish.
        failures.push({ itemIdentifier: msg.messageId });
        continue;
      }

      const event = result.data;

      // Idempotency guard.
      //
      // What idempotency protects against:
      //   SQS guarantees at-least-once delivery, not exactly-once. A message
      //   can be delivered more than once in several scenarios:
      //     1. The consumer receives the message, processes it successfully, but
      //        crashes before calling DeleteMessage. SQS re-delivers after the
      //        visibility timeout expires.
      //     2. SQS itself occasionally produces a duplicate delivery within the
      //        standard (non-FIFO) queue, even when the consumer is healthy and
      //        the visibility timeout has not expired. This is rare but documented.
      //     3. The producer retried a failed SNS publish, creating a second SQS
      //        message with a different messageId carrying the same business event.
      //
      //   Without an idempotency check, any of the above scenarios would cause
      //   the analytics consumer to double-count: audience reach inflated, campaign
      //   performance metrics skewed, and billing aggregates overstated. The fix is
      //   to record a key on first successful processing and skip on re-delivery.
      //
      // Key design:
      //   messageId covers scenarios 1 and 2 (same physical SQS message re-delivered).
      //   campaignId covers scenario 3 (different messageId, same business event).
      const key = makeIdempotencyKey(msg.messageId, event.campaignId);

      if (this.idempotency.has(key)) {
        console.log(
          `[AnalyticsConsumer] duplicate — skipping ${msg.messageId} ` +
            `(campaignId=${event.campaignId})`,
        );
        // Not a failure: the message was already processed. Delete it so it
        // does not re-appear and inflate the DLQ or receive-count metric.
        continue;
      }

      try {
        await this.recordAnalytics(event);
        // Mark AFTER successful processing. Marking before means a crash between
        // mark and write leaves the event permanently unprocessed with no retry.
        this.idempotency.add(key);
      } catch (err) {
        console.error(
          `[AnalyticsConsumer] failed to record analytics for ${msg.messageId}:`,
          err,
        );
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async recordAnalytics(event: CampaignPublished): Promise<void> {
    // Placeholder: production implementation would write to a time-series store,
    // append to a DynamoDB aggregate, or emit to a streaming pipeline (Kinesis,
    // BigQuery streaming insert, etc.).
    console.log("[AnalyticsConsumer] recording analytics event:", {
      campaignId: event.campaignId,
      tenantId: event.tenantId,
      tenantTier: event.tenantTier,
      campaignType: event.campaignType,
      audienceSize: event.audienceSize,
      publishedAt: event.publishedAt,
    });
  }
}

// Convenience factory wiring up the LocalStack SQS client and a fresh
// in-memory idempotency store. In production, replace InMemoryIdempotencyStore
// with a DynamoDB-backed implementation shared across all consumer replicas.
export function createAnalyticsConsumer(queueUrl: string): AnalyticsConsumer {
  const sqs = new SQSClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });

  return new AnalyticsConsumer(sqs, queueUrl, new InMemoryIdempotencyStore());
}
