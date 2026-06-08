import { SQSClient } from "@aws-sdk/client-sqs";
import { BaseConsumer, type BatchItemFailure, type ParsedMessage } from "./BaseConsumer.js";
import { CampaignPublishedSchema, type CampaignPublished } from "../events/schemas.js";
import {
  InMemoryIdempotencyStore,
  makeIdempotencyKey,
  type IdempotencyStore,
} from "../lib/idempotency.js";

// SurveyConsumer reads from the campaign-survey SQS queue, which EventBridge
// populates by matching events where detail.campaignType === "survey".
// This is content-based routing: the routing decision lives in the EventBridge
// rule, not in an attribute the producer had to set explicitly.
export class SurveyConsumer extends BaseConsumer<CampaignPublished> {
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
        console.error(`[SurveyConsumer] schema mismatch on ${msg.messageId}:`, result.error.flatten());
        failures.push({ itemIdentifier: msg.messageId });
        continue;
      }

      const event = result.data;
      const key = makeIdempotencyKey(msg.messageId, event.campaignId);

      if (await this.idempotency.has(key)) {
        console.log(`[SurveyConsumer] duplicate — skipping ${msg.messageId} (campaignId=${event.campaignId})`);
        continue;
      }

      try {
        await this.processSurveyCampaign(event);
        await this.idempotency.add(key);
      } catch (err) {
        console.error(`[SurveyConsumer] failed to process ${msg.messageId}:`, err);
        failures.push({ itemIdentifier: msg.messageId });
      }
    }

    return failures;
  }

  private async processSurveyCampaign(event: CampaignPublished): Promise<void> {
    // Placeholder: production would trigger survey tooling (SurveyMonkey, Typeform,
    // an in-house survey service) to schedule delivery to the audience.
    console.log("[SurveyConsumer] processing survey campaign:", {
      campaignId: event.campaignId,
      tenantId: event.tenantId,
      campaignType: event.campaignType,
      audienceSize: event.audienceSize,
    });
  }
}

export function createSurveyConsumer(queueUrl: string): SurveyConsumer {
  const sqs = new SQSClient({
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
  return new SurveyConsumer(sqs, queueUrl, new InMemoryIdempotencyStore());
}
