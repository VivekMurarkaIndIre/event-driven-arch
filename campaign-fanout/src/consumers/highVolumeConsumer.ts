import { SQSClient } from "@aws-sdk/client-sqs";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  InMemoryIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

// HighVolumeConsumer reads from the campaign-high-volume SQS queue, which
// EventBridge populates by matching events where detail.audienceSize > 10000.
// The numeric range operator is unique to EventBridge — SNS filter policies
// support string prefix and exact-match only, not numeric comparisons.
export class HighVolumeConsumer extends BaseConsumer<CampaignPublished> {
  constructor(
    sqs: SQSClient,
    queueUrl: string,
    private readonly idempotency: IdempotencyStore,
  ) {
    super(sqs, {
      queueUrl,
      batchSize: 10,
      // High-volume campaigns may take longer to queue for delivery (rate limiting,
      // external API back-pressure). Use a longer visibility timeout so the consumer
      // has more time before SQS assumes the message was lost and re-delivers it.
      visibilityTimeout: 60,
    });
  }

  override async processMessageBatch(
    messages: ParsedMessage<CampaignPublished>[],
  ): Promise<BatchItemFailure[]> {
    const failures: BatchItemFailure[] = [];

    for (const msg of messages) {
      const result = CampaignPublishedSchema.safeParse(msg.body);
      if (!result.success) {
        console.error(`[HighVolumeConsumer] schema mismatch on ${msg.messageId}:`, result.error.flatten());
        failures.push({ itemIdentifier: msg.messageId });
        continue;
      }

      const event = result.data;
      const key = makeIdempotencyKey(msg.messageId, event.campaignId);

      if (await this.idempotency.has(key)) {
        console.log(`[HighVolumeConsumer] duplicate — skipping ${msg.messageId} (campaignId=${event.campaignId})`);
        continue;
      }

      try {
        await this.processHighVolumeCampaign(event);
        await this.idempotency.add(key);
      } catch (err) {
        console.error(`[HighVolumeConsumer] failed to process ${msg.messageId}:`, err);
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async processHighVolumeCampaign(event: CampaignPublished): Promise<void> {
    // Placeholder: production would route to a dedicated high-throughput delivery
    // pipeline (e.g. Kinesis Data Streams, a bulk-send API with rate limiting,
    // or a batch-aware job scheduler) instead of the standard single-send path.
    console.log("[HighVolumeConsumer] processing high-volume campaign:", {
      campaignId: event.campaignId,
      tenantId: event.tenantId,
      campaignType: event.campaignType,
      audienceSize: event.audienceSize,
    });
  }
}

export function createHighVolumeConsumer(queueUrl: string): HighVolumeConsumer {
  const sqs = new SQSClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
  return new HighVolumeConsumer(sqs, queueUrl, new InMemoryIdempotencyStore());
}
