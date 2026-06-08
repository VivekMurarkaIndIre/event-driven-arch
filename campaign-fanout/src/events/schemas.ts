import { z } from "zod";

export const TenantTierSchema = z.enum(["free", "pro", "enterprise"]);
export type TenantTier = z.infer<typeof TenantTierSchema>;

export const CampaignTypeSchema = z.enum(["email", "sms", "push"]);
export type CampaignType = z.infer<typeof CampaignTypeSchema>;

export const CampaignPublishedSchema = z.object({
  campaignId: z.string().uuid(),
  tenantId: z.string().uuid(),
  tenantTier: TenantTierSchema,
  campaignType: CampaignTypeSchema,
  // Total number of recipients targeted by this campaign run.
  audienceSize: z.number().int().positive(),
  // Propagated from the originating HTTP request; links all downstream events
  // for this campaign action in distributed traces and logs.
  correlationId: z.string().uuid(),
  // ISO-8601 UTC timestamp set by the producer at publish time, not by any
  // broker. Consumers must not use SQS/SNS timestamps as a source of truth
  // for event time because they reflect delivery time, not business time.
  publishedAt: z.string().datetime(),
});

export type CampaignPublished = z.infer<typeof CampaignPublishedSchema>;
