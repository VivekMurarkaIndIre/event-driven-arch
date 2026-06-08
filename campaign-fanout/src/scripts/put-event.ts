import { EventBridgeClient } from "@aws-sdk/client-eventbridge";
import { CampaignPublishedSchema } from "../events/schemas.js";
import { putCampaignEvent } from "../publisher/eventBridgePublisher.js";

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const BUS_NAME = "campaign-bus";

const client = new EventBridgeClient({
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

// Two test events — one matches the survey rule, the other the high-volume rule.
// Run `npm run infra:setup` first, then open two terminals running
// `npm run consume:survey` and `npm run consume:high-volume` before running this.
const testEvents = [
  {
    campaignId: "018e8e8e-0000-7000-8000-000000000010",
    tenantId: "018e8e8e-0000-7000-8000-000000000002",
    tenantTier: "pro",
    // campaignType "survey" matches the route-survey-campaigns EventBridge rule.
    campaignType: "survey",
    audienceSize: 500,
    correlationId: "018e8e8e-0000-7000-8000-000000000011",
    publishedAt: new Date().toISOString(),
  },
  {
    campaignId: "018e8e8e-0000-7000-8000-000000000020",
    tenantId: "018e8e8e-0000-7000-8000-000000000002",
    tenantTier: "enterprise",
    campaignType: "email",
    // audienceSize 50000 matches the route-high-volume-campaigns rule (> 10000).
    audienceSize: 50000,
    correlationId: "018e8e8e-0000-7000-8000-000000000021",
    publishedAt: new Date().toISOString(),
  },
] as const;

for (const raw of testEvents) {
  const event = CampaignPublishedSchema.parse(raw);
  const result = await putCampaignEvent(client, BUS_NAME, event);
  console.log(`Put event to ${BUS_NAME}`);
  console.log(`  eventId:      ${result.eventId}`);
  console.log(`  campaignId:   ${event.campaignId}`);
  console.log(`  campaignType: ${event.campaignType}`);
  console.log(`  audienceSize: ${event.audienceSize}`);
  console.log();
}
