import { SQSClient } from "@aws-sdk/client-sqs";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  InMemoryIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

// EmailConsumer reads from campaign-processor and simulates sending campaign emails.
// It has no SNS filter policy — it receives events from all tenant tiers because
// every tenant, including free tier, can send email campaigns.
export class EmailConsumer extends BaseConsumer<CampaignPublished> {
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
        console.error(`[EmailConsumer] schema mismatch on ${msg.messageId}:`, result.error.flatten());
        failures.push({ itemIdentifier: msg.messageId });
        continue;
      }

      const event = result.data;
      const key = makeIdempotencyKey(msg.messageId, event.campaignId);

      if (this.idempotency.has(key)) {
        console.log(`[EmailConsumer] duplicate — skipping ${msg.messageId} (campaignId=${event.campaignId})`);
        continue;
      }

      try {
        await this.sendEmail(event);
        this.idempotency.add(key);
      } catch (err) {
        console.error(`[EmailConsumer] failed to send email for ${msg.messageId}:`, err);
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async sendEmail(event: CampaignPublished): Promise<void> {
    // Placeholder: in production this would render an email template and call
    // SES, SendGrid, or another email delivery service.
    console.log("[EmailConsumer] sending email:", {
      campaignId: event.campaignId,
      tenantId: event.tenantId,
      tenantTier: event.tenantTier,
      campaignType: event.campaignType,
      audienceSize: event.audienceSize,
    });
  }
}

export function createEmailConsumer(queueUrl: string): EmailConsumer {
  const sqs = new SQSClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
  return new EmailConsumer(sqs, queueUrl, new InMemoryIdempotencyStore());
}
