import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { createHash } from "node:crypto";
import type { CampaignPublished } from "../events/schemas.js";

export interface PublishResult {
  messageId: string;
  messageDeduplicationId: string;
}

export interface PublishOptions {
  // Event schema version. Changing this produces a new deduplication key,
  // which is intentional: a schema-breaking change is a new logical event.
  version: number;
  // "standard" (default): no ordering or deduplication at the broker layer.
  //   MessageGroupId and MessageDeduplicationId are not sent — SNS rejects
  //   them on standard topics with InvalidParameter.
  // "fifo": requires a FIFO topic ARN. Enables per-campaign ordered delivery
  //   and 5-minute deduplication window at the enqueue boundary.
  topicType?: "standard" | "fifo";
}

// Compute a deterministic identifier for this (campaignId, schema version) pair.
function computeDeduplicationId(campaignId: string, version: number): string {
  return createHash("sha256").update(`${campaignId}:${version}`).digest("hex");
}

// ---------------------------------------------------------------------------
// Delivery guarantees — what each topic type provides and what it does not
// ---------------------------------------------------------------------------
//
// STANDARD TOPIC (default, used for analytics / notifier / audit queues)
//   Delivery: at-least-once. SQS may deliver the same message more than once.
//   Ordering: best-effort. Messages from the same publisher may arrive out of
//     order at the consumer.
//   Deduplication: none at the broker. The MessageDeduplicationId computed
//     below is NOT sent to standard topics (SNS rejects it); it is logged
//     for use as a consumer-side idempotency key.
//
// FIFO TOPIC (topicType: "fifo", used for the email consumer)
//   Delivery: at-least-once TO THE CONSUMER — see note below.
//   Ordering: strict, per MessageGroupId. All messages with the same
//     campaignId are delivered to the consumer in the order they were
//     published. Different campaign groups are independent.
//   Deduplication (enqueue): within a 5-minute window, a second PublishCommand
//     with the same MessageDeduplicationId is silently discarded — the message
//     is never written to the SQS queue a second time. After the window, a
//     re-publish with the same ID is treated as a new message.
//
//   WHY FIFO DEDUPLICATION ≠ EXACTLY-ONCE PROCESSING
//   SNS FIFO deduplication operates only at the publish → enqueue boundary.
//   Once a message is in the SQS FIFO queue, SQS delivers it at-least-once:
//   if a consumer receives the message, processes it (sends the email), but
//   crashes before calling DeleteMessage, SQS re-delivers the message after
//   the visibility timeout expires. At that point the message was never
//   duplicated at the enqueue layer; SQS simply cannot know the consumer
//   already succeeded. Consumer-side idempotency (DynamoDB conditional write)
//   is required in addition to — not instead of — FIFO deduplication.
//
//   The combination of FIFO enqueue deduplication + DynamoDB consumer
//   idempotency is the closest AWS approximation of exactly-once delivery.
//   True exactly-once would require a distributed transaction spanning the
//   consumer's business operation and the idempotency store — not available
//   without 2-phase commit.
// ---------------------------------------------------------------------------

export async function publishCampaignEvent(
  client: SNSClient,
  topicArn: string,
  event: CampaignPublished,
  opts: PublishOptions,
): Promise<PublishResult> {
  const messageDeduplicationId = computeDeduplicationId(event.campaignId, opts.version);
  const topicType = opts.topicType ?? "standard";

  // Message attributes are evaluated by SNS subscription filter policies before
  // delivery. Any field used for broker-side filtering must be present here even
  // if it duplicates a body field — the body is opaque to the broker.
  const { MessageId } = await client.send(
    new PublishCommand({
      TopicArn: topicArn,
      Message: JSON.stringify(event),
      MessageAttributes: {
        tenantId: { DataType: "String", StringValue: event.tenantId },
        tenantTier: { DataType: "String", StringValue: event.tenantTier },
        eventType: { DataType: "String", StringValue: "CampaignPublished" },
      },
      // FIFO-only fields. Standard topics reject these with InvalidParameter.
      ...(topicType === "fifo" && {
        // All messages for the same campaign are ordered within their group.
        // Consumers process one group at a time; a slow campaign does not
        // block messages for other campaigns.
        MessageGroupId: event.campaignId,
        // Prevents the same (campaignId, version) from being enqueued twice
        // within the 5-minute SNS deduplication window.
        MessageDeduplicationId: messageDeduplicationId,
      }),
    }),
  );

  if (!MessageId) throw new Error("SNS did not return a MessageId");

  return { messageId: MessageId, messageDeduplicationId };
}
