import { SNSClient } from "@aws-sdk/client-sns";
import { CampaignPublishedSchema } from "../events/schemas.js";
import { publishCampaignEvent } from "../publisher/campaignPublisher.js";

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
// Must match the topic name created by infra:setup.
const TOPIC_ARN = `arn:aws:sns:${REGION}:000000000000:campaign-created`;

const client = new SNSClient({
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

const rawEvent = {
  campaignId: "018e8e8e-0000-7000-8000-000000000001",
  tenantId: "018e8e8e-0000-7000-8000-000000000002",
  tenantTier: "pro",
  campaignType: "email",
  audienceSize: 4200,
  correlationId: "018e8e8e-0000-7000-8000-000000000003",
  publishedAt: new Date().toISOString(),
};

// Validate at the boundary before touching the network. parse() throws a
// ZodError with a human-readable message if any field is wrong — catch it
// early rather than sending a malformed payload that silently reaches consumers.
const event = CampaignPublishedSchema.parse(rawEvent);

const result = await publishCampaignEvent(client, TOPIC_ARN, event, {
  version: 1,
});

console.log("Published CampaignPublished event");
console.log("  messageId:              ", result.messageId);
console.log("  messageDeduplicationId: ", result.messageDeduplicationId);
console.log("  payload:                ", JSON.stringify(event, null, 2));
