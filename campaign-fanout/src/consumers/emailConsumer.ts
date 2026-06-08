import { SQSClient } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  DynamoDBIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

// EmailConsumer reads from campaign-processor.fifo and simulates sending campaign
// emails. It uses a DynamoDB-backed idempotency store so that:
//   1. A crash between sendEmail() and DeleteMessage is safe — on re-delivery
//      has() returns true immediately and the email is not sent again.
//   2. Multiple scaled-out replicas share the same store. Only the first replica
//      to call add() processes the message; concurrent adds race on the DynamoDB
//      conditional write and the loser receives ConditionalCheckFailedException,
//      which surfaces as a BatchItemFailure and triggers SQS retry, at which
//      point has() cleanly short-circuits.
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

      if (await this.idempotency.has(key)) {
        console.log(`[EmailConsumer] duplicate — skipping ${msg.messageId} (campaignId=${event.campaignId})`);
        continue;
      }

      try {
        await this.sendEmail(event);
        // Mark AFTER the email succeeds. Marking before risks a permanent skip
        // if the send throws: the key exists but no email was ever sent.
        // ConditionalCheckFailedException here means a concurrent replica already
        // processed this message — the exception propagates as a BatchItemFailure,
        // SQS retries, and has() returns true on the next delivery.
        await this.idempotency.add(key);
      } catch (err) {
        console.error(`[EmailConsumer] failed to send email for ${msg.messageId}:`, err);
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async sendEmail(event: CampaignPublished): Promise<void> {
    // Placeholder: production would render an email template and call SES,
    // SendGrid, or another delivery service.
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
  const dynamo = new DynamoDBClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
  return new EmailConsumer(sqs, queueUrl, new DynamoDBIdempotencyStore(dynamo));
}
