import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { createHash } from "node:crypto";
import type { CampaignPublished } from "../events/schemas.js";

export interface PublishResult {
  messageId: string;
  // Returned so callers can log or store the dedup key alongside the messageId.
  messageDeduplicationId: string;
}

export interface PublishOptions {
  // Event schema version. Incrementing version changes the deduplication key,
  // which is intentional: a schema-breaking change produces a new logical event.
  version: number;
}

// Compute a stable identifier for this (event, version) pair.
//
// Standard topic behaviour — what happens when the same event is published twice:
//   SNS delivers both messages independently. The MessageDeduplicationId is NOT
//   sent to standard topics (SNS rejects it with InvalidParameter). Deduplication
//   must happen at the consumer: store the campaignId+version key in DynamoDB or
//   Redis on first successful processing and idempotency-check on every receive.
//
// FIFO topic behaviour — what happens when the same event is published twice:
//   SNS uses MessageDeduplicationId to suppress the second publish within a
//   5-minute deduplication window. The second PublishCommand call returns the
//   same MessageId as the first and delivers nothing to subscribers. After the
//   window expires, a third publish with the same key is treated as a new message.
//   FIFO topics also guarantee exactly-once delivery *to the queue* (combined with
//   SQS FIFO), but the consumer can still receive the same message more than once
//   if it crashes after receive but before deletion — so idempotency is still required.
function computeDeduplicationId(campaignId: string, version: number): string {
  return createHash("sha256")
    .update(`${campaignId}:${version}`)
    .digest("hex");
}

export async function publishCampaignEvent(
  client: SNSClient,
  topicArn: string,
  event: CampaignPublished,
  opts: PublishOptions,
): Promise<PublishResult> {
  const messageDeduplicationId = computeDeduplicationId(
    event.campaignId,
    opts.version,
  );

  // Message attributes are indexed by SNS and can be used in SQS subscription
  // filter policies so consumers receive only the subset of events they need —
  // without deserialising the message body. For example, a "pro-tier notifier"
  // queue can filter on tenantTier = "pro" | "enterprise" at the SNS layer.
  const { MessageId } = await client.send(
    new PublishCommand({
      TopicArn: topicArn,
      Message: JSON.stringify(event),
      MessageAttributes: {
        tenantId: { DataType: "String", StringValue: event.tenantId },
        tenantTier: { DataType: "String", StringValue: event.tenantTier },
        eventType: { DataType: "String", StringValue: "CampaignPublished" },
      },
    }),
  );

  if (!MessageId) throw new Error("SNS did not return a MessageId");

  return { messageId: MessageId, messageDeduplicationId };
}
