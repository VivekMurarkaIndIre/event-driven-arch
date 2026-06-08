import { randomUUID } from "node:crypto";
import { SNSClient } from "@aws-sdk/client-sns";
import { CampaignPublishedSchema, type TenantTier } from "../events/schemas.js";
import { publishCampaignEvent } from "../publisher/campaignPublisher.js";

const TOPIC_ARN = "arn:aws:sns:us-east-1:000000000000:campaign-created";

const client = new SNSClient({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

// 10 events: 4 free, 3 pro, 3 enterprise.
// Expected queue depths after publishing (with filter policy active):
//   campaign-processor   → 10  (no filter — receives all tiers)
//   campaign-notifier    →  6  (filter: pro + enterprise only)
//   campaign-analytics   → 10  (no filter — receives all tiers)
//   campaign-audit       → 10  (no filter — receives all tiers)
const BATCH: { tenantTier: TenantTier; campaignType: "email" | "sms" | "push" }[] = [
  { tenantTier: "free",       campaignType: "email" },
  { tenantTier: "free",       campaignType: "sms"   },
  { tenantTier: "free",       campaignType: "email" },
  { tenantTier: "free",       campaignType: "push"  },
  { tenantTier: "pro",        campaignType: "email" },
  { tenantTier: "pro",        campaignType: "sms"   },
  { tenantTier: "pro",        campaignType: "push"  },
  { tenantTier: "enterprise", campaignType: "email" },
  { tenantTier: "enterprise", campaignType: "sms"   },
  { tenantTier: "enterprise", campaignType: "push"  },
];

console.log(`Publishing ${BATCH.length} events to ${TOPIC_ARN}\n`);

const tenantId = randomUUID();
const correlationId = randomUUID();
let free = 0, paid = 0;

for (const [i, spec] of BATCH.entries()) {
  const event = CampaignPublishedSchema.parse({
    campaignId:   randomUUID(),
    tenantId,
    tenantTier:   spec.tenantTier,
    campaignType: spec.campaignType,
    audienceSize: (i + 1) * 100,
    correlationId,
    publishedAt:  new Date().toISOString(),
  });

  const { messageId } = await publishCampaignEvent(client, TOPIC_ARN, event, { version: 1 });

  const tier = spec.tenantTier.padEnd(10);
  const type = spec.campaignType.padEnd(5);
  const routed = spec.tenantTier === "free" ? "→ processor/analytics/audit only" : "→ ALL queues (incl. notifier)";
  console.log(`  [${String(i + 1).padStart(2)}] tier=${tier} type=${type} msgId=${messageId}  ${routed}`);

  spec.tenantTier === "free" ? free++ : paid++;
}

console.log(`
Summary
  Published : ${BATCH.length}
  Free tier : ${free}  (skipped by campaign-notifier filter)
  Paid tier : ${paid}  (delivered to all queues)

Expected queue depths
  campaign-processor   ${BATCH.length}
  campaign-notifier    ${paid}   ← SNS filter dropped ${free} free-tier event(s) before enqueue
  campaign-analytics   ${BATCH.length}
  campaign-audit       ${BATCH.length}
`);
