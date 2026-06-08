import { SQSClient } from "@aws-sdk/client-sqs";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  InMemoryIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

// NotificationConsumer reads from campaign-notifier, which carries an SNS
// subscription filter policy: { tenantTier: ["pro", "enterprise"] }.
//
// This means the broker (SNS) silently discards free-tier events before they
// ever enter this queue. The consumer never polls, processes, or pays for them.
// This is the key advantage of broker-side filtering over consumer-side filtering:
// the queue stays short, ReceiveMessage calls return real work rather than
// messages that will be immediately skipped, and there is no wasted CPU or API cost.
//
// The consumer itself does NOT re-check tenantTier. The filter policy is the
// enforcement boundary. If a free-tier message somehow arrived (e.g. the
// subscription was misconfigured), it would be processed — a defence-in-depth
// argument for adding an assertion here, depending on the business risk.
export class NotificationConsumer extends BaseConsumer<CampaignPublished> {
  constructor(
    sqs: SQSClient,
    queueUrl: string,
    private readonly idempotency: IdempotencyStore,
  ) {
    super(sqs, { queueUrl, batchSize: 10, visibilityTimeout: 30 });
  }

  override async processMessageBatch(
    messages: ParsedMessage<CampaignPublished>[],
  ): Promise<BatchItemFailure[]> {
    const failures: BatchItemFailure[] = [];

    for (const msg of messages) {
      const result = CampaignPublishedSchema.safeParse(msg.body);
      if (!result.success) {
        console.error(`[NotificationConsumer] schema mismatch on ${msg.messageId}:`, result.error.flatten());
        failures.push({ itemIdentifier: msg.messageId });
        continue;
      }

      const event = result.data;
      const key = makeIdempotencyKey(msg.messageId, event.campaignId);

      if (await this.idempotency.has(key)) {
        console.log(`[NotificationConsumer] duplicate — skipping ${msg.messageId} (campaignId=${event.campaignId})`);
        continue;
      }

      try {
        await this.sendNotification(event);
        await this.idempotency.add(key);
      } catch (err) {
        console.error(`[NotificationConsumer] failed to send notification for ${msg.messageId}:`, err);
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async sendNotification(event: CampaignPublished): Promise<void> {
    // Placeholder: in production this would call a push notification service
    // (APNs, FCM) or an SMS gateway (Twilio, SNS SMS).
    console.log("[NotificationConsumer] sending push/SMS notification:", {
      campaignId: event.campaignId,
      tenantId: event.tenantId,
      tenantTier: event.tenantTier, // always "pro" or "enterprise" due to filter policy
      campaignType: event.campaignType,
      audienceSize: event.audienceSize,
    });
  }
}

export function createNotificationConsumer(queueUrl: string): NotificationConsumer {
  const sqs = new SQSClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
  return new NotificationConsumer(sqs, queueUrl, new InMemoryIdempotencyStore());
}
