import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import type { CampaignPublished } from "../events/schemas.js";

// ---------------------------------------------------------------------------
// EventBridge content-based filtering vs SNS filter policies
//
// SNS filter policies — attribute-level, synchronous, free
//   Filter rules are attached to each SNS → SQS subscription and evaluated
//   against MessageAttributes (the flat key-value map set at publish time).
//   The message body is completely opaque: you cannot reference any body field
//   in an SNS filter policy. If a field is needed for routing, it must be
//   duplicated into a MessageAttribute at publish time (see campaignPublisher.ts).
//   Cost: filtered messages are free — they never enter the queue.
//   Throughput: transparent; SNS absorbs the fan-out.
//
// EventBridge event patterns — body-level, asynchronous, charged per event
//   Rules are attached to an event bus and can match any field in the event
//   structure: source, detail-type, account, region, time, and every nested
//   field inside detail.*. A single pattern can AND conditions across
//   multiple fields and depths — impossible with SNS attributes alone.
//
//   Operators available inside a pattern:
//     Exact match:    ["value"]
//     Anything-but:  [{ "anything-but": ["x", "y"] }]
//     Prefix:        [{ "prefix": "camp" }]
//     Numeric range: [{ "numeric": [">", 10000] }]
//     Exists:        [{ "exists": true }]  /  [{ "exists": false }]
//     IP CIDR:       [{ "cidr": "10.0.0.0/8" }]
//     Multiple fields in one pattern are implicitly ANDed.
//
// PutEvents — size and throughput constraints
//   Batch limit:      Up to 10 event entries per PutEvents call.
//   Entry size:       256 KB per event entry. This covers the ENTIRE EventBridge
//                     event object (version + id + source + detail-type +
//                     resources + detail combined). SNS and SQS share the same
//                     256 KB limit, but only on the message body. EventBridge's
//                     envelope fields count against your quota, so a real-world
//                     detail payload should stay well below 250 KB to leave room.
//   Throughput quota: 10,000 events/second per region (default, soft limit).
//                     Exceeding this returns ThrottlingException. Retry with
//                     exponential backoff and jitter; consider batching publishes
//                     if your event rate approaches the quota.
//   Partial failure:  PutEvents is NOT atomic. Each entry succeeds or fails
//                     independently. Always check FailedEntryCount > 0 and
//                     retry only the failed entries — retrying the whole batch
//                     risks double-processing successful entries.
//
// When to reach for EventBridge instead of (or alongside) SNS
//   Use SNS when:
//     - Routing dimension is known at publish time and fits in an attribute.
//     - You need zero-cost filtering (filtered messages never enter queues).
//     - Throughput > 10,000 msg/s (SNS standard has no practical ceiling).
//   Use EventBridge when:
//     - Routing requires inspecting the event body (e.g. campaignType = "survey").
//     - You need numeric range matching on body fields (audienceSize > 10,000).
//     - The same event must fan out to heterogeneous targets: Lambda + SQS + SNS
//       + Step Functions + API destinations — from a single publish call.
//     - Event schema is owned by a third-party (SaaS connector, AWS service)
//       and you cannot add MessageAttributes.
// ---------------------------------------------------------------------------

export interface PutEventResult {
  readonly eventId: string;
}

export async function putCampaignEvent(
  client: EventBridgeClient,
  busName: string,
  event: CampaignPublished,
): Promise<PutEventResult> {
  const { FailedEntryCount, Entries } = await client.send(
    new PutEventsCommand({
      Entries: [
        {
          // source + DetailType are queryable in EventBridge patterns:
          //   { "source": ["campaign.service"], "detail-type": ["CampaignPublished"] }
          // They appear alongside detail.* fields in the same pattern object.
          Source: "campaign.service",
          DetailType: "CampaignPublished",
          // Detail must be a JSON string. EventBridge parses it on ingestion;
          // rule patterns match against the parsed object, not the raw string.
          Detail: JSON.stringify(event),
          EventBusName: busName,
        },
      ],
    }),
  );

  if ((FailedEntryCount ?? 0) > 0) {
    const failed = Entries?.[0];
    throw new Error(
      `PutEvents failed: ${failed?.ErrorCode ?? "unknown"} — ${failed?.ErrorMessage ?? "no message"}`,
    );
  }

  const entry = Entries?.[0];
  const eventId = entry?.EventId;
  if (!eventId) throw new Error("PutEvents returned no EventId");

  return { eventId };
}
