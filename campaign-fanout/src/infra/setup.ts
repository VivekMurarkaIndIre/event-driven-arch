import { SNSClient, CreateTopicCommand, SubscribeCommand } from "@aws-sdk/client-sns";
import {
  SQSClient,
  CreateQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";
import {
  EventBridgeClient,
  CreateEventBusCommand,
} from "@aws-sdk/client-eventbridge";
import {
  DynamoDBClient,
  CreateTableCommand,
  BillingMode,
} from "@aws-sdk/client-dynamodb";

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";

// LocalStack accepts any non-empty credential string; "test/test" is the conventional default.
const sharedConfig = {
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: {
    accessKeyId: "test",
    secretAccessKey: "test",
  },
};

const sns = new SNSClient(sharedConfig);
const sqs = new SQSClient(sharedConfig);
const eb = new EventBridgeClient(sharedConfig);
const dynamo = new DynamoDBClient(sharedConfig);

// ---------------------------------------------------------------------------
// Resource definitions
// ---------------------------------------------------------------------------

// One topic per lifecycle event so consumers can subscribe only to the events they care about.
// A single "campaign-events" topic would force every consumer to filter in-process,
// coupling consumer logic to the full event schema.
const SNS_TOPICS = [
  "campaign-created",
  "campaign-updated",
  "campaign-deleted",
  "campaign-activated",
] as const;

// Each downstream concern (processing, notifications, analytics, audit) gets its own queue.
// This is the fan-out pattern: SNS delivers to all queues simultaneously; each consumer
// scales and fails independently without affecting the others.
const SQS_QUEUES = [
  "campaign-processor",
  "campaign-notifier",
  "campaign-analytics",
  "campaign-audit",
] as const;

// campaign-bus: receives all campaign events for content-based routing rules
//   (e.g. route campaign-activated to billing targets only).
// campaign-dlq-bus: receives events that failed all SQS retries — lets you build
//   alerting rules on the DLQ bus without polluting the main bus.
const EVENT_BUSES = ["campaign-bus", "campaign-dlq-bus"] as const;

// ---------------------------------------------------------------------------
// SNS — broadcast layer
//
// SNS is the fan-out hub: one publish call delivers to every subscribed SQS queue
// simultaneously. Consumers are decoupled from the producer and from each other;
// adding a new consumer is a single Subscribe call with no producer changes.
// We use standard (non-FIFO) topics because ordering is not required and message
// rate may exceed the 300 msg/s FIFO ceiling in future.
// ---------------------------------------------------------------------------

async function createSnsTopics(): Promise<Map<string, string>> {
  console.log("Creating SNS topics...");
  const arns = new Map<string, string>();
  for (const name of SNS_TOPICS) {
    const { TopicArn } = await sns.send(
      new CreateTopicCommand({ Name: name, Attributes: {} }),
    );
    if (!TopicArn) throw new Error(`No TopicArn returned for: ${name}`);
    arns.set(name, TopicArn);
    console.log(`  ✓ ${name}  →  ${TopicArn}`);
  }
  return arns;
}

// ---------------------------------------------------------------------------
// SQS — consumer queues + dead-letter queues
//
// Each logical consumer (notifier, analytics, etc.) gets its own queue so it
// can scale, fail, and drain independently. A paired DLQ captures messages that
// exhaust all retries, giving operators a safe place to inspect and re-drive
// poison messages without losing them.
//
// Why SQS over Kinesis or Kafka here?
//   - No replay requirement: once a consumer processes a message it's done.
//   - No strict ordering requirement: consumers are idempotent.
//   - Message rate is well within SQS standard limits.
//   - Zero operational overhead vs. shard management (Kinesis) or broker ops (Kafka).
// ---------------------------------------------------------------------------

// SQS CreateQueue is idempotent only when every attribute is identical to the
// existing queue. Changing any attribute (e.g. maxReceiveCount in RedrivePolicy)
// throws QueueNameExists instead of returning the existing URL.
// This helper handles that case: on QueueNameExists, resolve the existing URL
// and apply the desired attributes via SetQueueAttributes so infra:setup is safe
// to re-run after any configuration change.
//
// FifoQueue and ContentBasedDeduplication are set at creation time and cannot
// be modified via SetQueueAttributes — they are filtered out of the update call.
const IMMUTABLE_QUEUE_ATTRS: ReadonlySet<string> = new Set([
  "FifoQueue",
  "ContentBasedDeduplication",
]);

async function ensureQueue(
  queueName: string,
  attributes: Record<string, string>,
): Promise<string> {
  try {
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: queueName, Attributes: attributes }),
    );
    if (!QueueUrl) throw new Error(`No QueueUrl returned for: ${queueName}`);
    return QueueUrl;
  } catch (err) {
    if ((err as { name?: string }).name !== "QueueNameExists") throw err;
    // Queue exists with different attributes — resolve URL and update mutable ones.
    const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
    if (!QueueUrl) throw new Error(`Could not resolve URL for: ${queueName}`);
    const mutableAttrs = Object.fromEntries(
      Object.entries(attributes).filter(([k]) => !IMMUTABLE_QUEUE_ATTRS.has(k)),
    );
    if (Object.keys(mutableAttrs).length > 0) {
      await sqs.send(new SetQueueAttributesCommand({ QueueUrl, Attributes: mutableAttrs }));
    }
    return QueueUrl;
  }
}

async function createSqsQueuesWithDlqs(): Promise<Map<string, string>> {
  console.log("\nCreating SQS queues + DLQs...");
  const queueArns = new Map<string, string>();

  for (const name of SQS_QUEUES) {
    const dlqName = `${name}-dlq`;

    // 1. Create DLQ first — its ARN is required before the main queue can be created.
    const dlqUrl = await ensureQueue(dlqName, {
      MessageRetentionPeriod: "1209600", // 14 days — long enough to investigate failures
    });

    // 2. Resolve DLQ ARN — RedrivePolicy requires the ARN, not the URL.
    //    GetQueueAttributes must be called explicitly; CreateQueue does not return it.
    const { Attributes: dlqAttrs } = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    const dlqArn = dlqAttrs?.["QueueArn"];
    if (!dlqArn) throw new Error(`Could not retrieve ARN for DLQ: ${dlqName}`);

    // 3. Create main queue with a redrive policy pointing at the DLQ.
    //
    // maxReceiveCount = 3: why not 1, and what is the right alerting threshold?
    //
    //   maxReceiveCount: 1 is dangerous:
    //     A single transient failure — network timeout, cold start, downstream 503,
    //     or a consumer process restart mid-batch — immediately DLQs the message.
    //     No retry window, no self-healing. Under partial batch response, one bad
    //     message in a ten-message batch exhausts its sole receive attempt on the
    //     very first error. DLQ depth becomes useless as a health signal because
    //     it fills with noise from transient failures, making it impossible to tell
    //     a genuine processing bug from a blip.
    //
    //   maxReceiveCount: 3 provides:
    //     Two soft retries before a definitive failure. A message that lands in the
    //     DLQ on the third receive has survived two separate delivery attempts —
    //     transient failures self-heal before that point. The DLQ then contains only
    //     messages the consumer genuinely cannot process (schema mismatch, downstream
    //     outage longer than retry window, poison-pill data).
    //
    //   Safe alerting threshold:
    //     Alert when DLQ ApproximateNumberOfMessages > 0. Each DLQ message has
    //     already exhausted all retries and is never noise. Using a threshold of 0
    //     is safe *because* maxReceiveCount > 1 filters transient failures before
    //     they ever reach the DLQ. A higher threshold (e.g. > 5) risks missing
    //     low-volume persistent failures that affect only a specific tenant or
    //     payload shape. Alert on 1, investigate immediately.
    //
    //    VisibilityTimeout=30s: safe default for fast processors; increase per-queue
    //    if processing time grows (timeout < processing time = duplicate delivery).
    const mainUrl = await ensureQueue(name, {
      VisibilityTimeout: "30",
      MessageRetentionPeriod: "345600", // 4 days
      RedrivePolicy: JSON.stringify({
        deadLetterTargetArn: dlqArn,
        maxReceiveCount: "3",
      }),
    });

    // 4. Resolve the main queue ARN — needed when creating SNS subscriptions.
    const { Attributes: mainAttrs } = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: mainUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    const mainArn = mainAttrs?.["QueueArn"];
    if (!mainArn) throw new Error(`Could not retrieve ARN for queue: ${name}`);

    queueArns.set(name, mainArn);
    console.log(`  ✓ ${name}  →  DLQ: ${dlqName}`);
  }

  return queueArns;
}

// ---------------------------------------------------------------------------
// EventBridge — content-based routing layer
//
// EventBridge complements SNS rather than replacing it:
//   SNS = broadcast to all subscribers unconditionally.
//   EventBridge = forward only to targets whose rule pattern matches the event.
//
// campaign-bus will carry rules like "only send campaign-activated events to
// the billing service" — impossible with SNS alone without consumer-side filtering.
// campaign-dlq-bus receives events forwarded from SQS DLQs, enabling alerting
// rules (e.g. PagerDuty target) without mixing failure traffic into the main bus.
// ---------------------------------------------------------------------------

async function createEventBridgeBuses(): Promise<void> {
  console.log("\nCreating EventBridge buses...");
  for (const name of EVENT_BUSES) {
    try {
      const { EventBusArn } = await eb.send(new CreateEventBusCommand({ Name: name }));
      console.log(`  ✓ ${name}  →  ${EventBusArn}`);
    } catch (err) {
      // Idempotent: if the bus already exists (e.g. LocalStack retained state
      // between runs), treat it as success. Any other error is re-thrown.
      if ((err as { name?: string }).name === "ResourceAlreadyExistsException") {
        console.log(`  ✓ ${name}  (already exists)`);
      } else {
        throw err;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// SNS → SQS subscriptions + filter policies
//
// SNS FILTER POLICIES (used here)
//   Match on message *attributes* — the key-value metadata set in MessageAttributes
//   at publish time, evaluated by SNS BEFORE the message enters the SQS queue.
//   A non-matching message is never enqueued; the consumer never sees it and is
//   never billed for it.
//
//   Expressiveness: string exact match, prefix, suffix, numeric range,
//   anything-but. Operates only on flat attribute values — cannot navigate into
//   the message body.
//
//   Best for: routing dimensions that are known at publish time and fit naturally
//   into message attributes: tenantTier, region, eventType, priority, source service.
//
// EVENTBRIDGE RULES (campaign-bus, wired separately)
//   Match on the event *body* — arbitrary nested JSON fields in detail.* — and
//   on event metadata (source, detail-type, account, region).
//
//   Expressiveness: same operators as SNS filters PLUS: nested field access,
//   anything-but arrays, IP CIDR ranges, exists/not-exists on optional fields,
//   and complex AND combinations across multiple fields.
//
//   Cost model: you pay per event that enters the bus regardless of whether a
//   rule matches. Rules that match incur a small per-invocation charge.
//
//   Best for: routing that requires inspecting the body (e.g. "only route events
//   where detail.campaign.type = 'email' AND detail.audienceSize > 1000"), or
//   combining body conditions with metadata conditions.
//
// Why this project uses BOTH:
//   SNS filter → tenantTier on the notifier subscription.
//     tenantTier is already published as a message attribute (campaignPublisher.ts)
//     so SNS can filter without deserialising the body. Fast, cheap, zero consumer cost.
//   EventBridge → future rules on campaign-bus such as "only route campaign-activated
//     to the billing service". The billing rule needs detail.campaignType AND the
//     event type — a body+metadata combination that SNS attributes cannot express.
//
// Production note: in real AWS each SQS queue needs an access policy granting
//   sns.amazonaws.com permission to call sqs:SendMessage from the topic ARN.
//   LocalStack does not enforce this policy, so it is omitted here to keep the
//   local setup simple. Add it before deploying to a real AWS account.
// ---------------------------------------------------------------------------

// "Paid" tiers receive notification messages. Free tier does not.
// This filter is enforced at the broker — free-tier events never enter the
// campaign-notifier queue, so the consumer processes zero extra messages for them.
const PAID_TIERS = ["pro", "enterprise"] as const;

async function createSnsSubscriptions(
  topicArns: Map<string, string>,
  queueArns: Map<string, string>,
): Promise<void> {
  console.log("\nCreating SNS → SQS subscriptions...");

  const createdTopicArn = topicArns.get("campaign-created");
  if (!createdTopicArn) throw new Error("campaign-created topic ARN not found");

  for (const [queueName, queueArn] of queueArns) {
    // campaign-notifier only receives events from paid-tier tenants.
    // All other queues receive every event regardless of tenantTier.
    const filterPolicy =
      queueName === "campaign-notifier"
        ? JSON.stringify({ tenantTier: [...PAID_TIERS] })
        : undefined;

    await sns.send(
      new SubscribeCommand({
        TopicArn: createdTopicArn,
        Protocol: "sqs",
        // SNS subscriptions require the queue ARN, not the queue URL.
        Endpoint: queueArn,
        Attributes: {
          // FilterPolicyScope defaults to MessageAttributes (not MessageBody),
          // matching against the flat MessageAttributes map set at publish time.
          ...(filterPolicy !== undefined && {
            FilterPolicy: filterPolicy,
            FilterPolicyScope: "MessageAttributes",
          }),
        },
      }),
    );

    const filterNote =
      queueName === "campaign-notifier"
        ? `  [filter: tenantTier ∈ {${PAID_TIERS.join(", ")}}]`
        : "  [no filter — receives all tiers]";
    console.log(`  ✓ campaign-created → ${queueName}${filterNote}`);
  }
}

// ---------------------------------------------------------------------------
// FIFO resources — ordered, deduplicated delivery for the email consumer
//
// Standard SNS → SQS fan-out is unordered and at-least-once. For the email
// consumer, sending the same campaign email twice to a recipient is a worse
// outcome than a brief delay. The FIFO stack addresses this:
//
//   campaign-events.fifo (SNS FIFO topic)
//     ↓ MessageGroupId = campaignId  (one ordered stream per campaign)
//     ↓ MessageDeduplicationId = SHA-256(campaignId:version)
//   campaign-processor.fifo (SQS FIFO queue)
//     → EmailConsumer (DynamoDB-backed idempotency store)
//
// Why a separate FIFO topic rather than migrating the standard one?
//   Standard and FIFO topics cannot share subscriptions — a FIFO queue can
//   only subscribe to a FIFO topic and vice versa. Running both in parallel
//   lets the existing standard consumers (analytics, audit, notifier) continue
//   unchanged while the email consumer moves to the stronger delivery model.
// ---------------------------------------------------------------------------

async function createFifoResources(): Promise<void> {
  console.log("\nCreating FIFO resources...");

  // 1. FIFO SNS topic.
  //    ContentBasedDeduplication: "false" — we provide explicit MessageDeduplicationId
  //    values (SHA-256 of campaignId:version), which is more reliable than a hash
  //    of the message body (body hash collides if two campaigns happen to produce
  //    identical JSON, however unlikely).
  const { TopicArn: fifoTopicArn } = await sns.send(
    new CreateTopicCommand({
      Name: "campaign-events.fifo",
      Attributes: {
        FifoTopic: "true",
        ContentBasedDeduplication: "false",
      },
    }),
  );
  if (!fifoTopicArn) throw new Error("No TopicArn returned for campaign-events.fifo");
  console.log(`  ✓ campaign-events.fifo  →  ${fifoTopicArn}`);

  // 2. FIFO DLQ — must also be a FIFO queue; a FIFO main queue's DLQ must match.
  const fifoDlqUrl = await ensureQueue("campaign-processor-dlq.fifo", {
    FifoQueue: "true",
    ContentBasedDeduplication: "false",
    MessageRetentionPeriod: "1209600", // 14 days
  });

  const { Attributes: fifoDlqAttrs } = await sqs.send(
    new GetQueueAttributesCommand({ QueueUrl: fifoDlqUrl, AttributeNames: ["QueueArn"] }),
  );
  const fifoDlqArn = fifoDlqAttrs?.["QueueArn"];
  if (!fifoDlqArn) throw new Error("Could not retrieve ARN for campaign-processor-dlq.fifo");

  // 3. FIFO main queue.
  //    ContentBasedDeduplication: "false" — the SNS-level dedup ID flows through
  //    to SQS when delivered via a FIFO SNS subscription, so we don't need a
  //    second hash at the queue level.
  const fifoMainUrl = await ensureQueue("campaign-processor.fifo", {
    FifoQueue: "true",
    ContentBasedDeduplication: "false",
    VisibilityTimeout: "30",
    MessageRetentionPeriod: "345600", // 4 days
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: fifoDlqArn, maxReceiveCount: "3" }),
  });
  console.log(`  ✓ campaign-processor.fifo  →  DLQ: campaign-processor-dlq.fifo`);

  const { Attributes: fifoMainAttrs } = await sqs.send(
    new GetQueueAttributesCommand({ QueueUrl: fifoMainUrl, AttributeNames: ["QueueArn"] }),
  );
  const fifoMainArn = fifoMainAttrs?.["QueueArn"];
  if (!fifoMainArn) throw new Error("Could not retrieve ARN for campaign-processor.fifo");

  // 4. Subscribe the FIFO queue to the FIFO topic.
  //    No filter policy — the FIFO topic is purpose-built for the email consumer
  //    and only receives events explicitly published to it.
  //    Note: in real AWS, the FIFO queue also needs a resource policy allowing
  //    sns.amazonaws.com to call sqs:SendMessage. LocalStack does not enforce this.
  await sns.send(
    new SubscribeCommand({
      TopicArn: fifoTopicArn,
      Protocol: "sqs",
      Endpoint: fifoMainArn,
      Attributes: {},
    }),
  );
  console.log(`  ✓ campaign-events.fifo → campaign-processor.fifo  [ordered, deduplicated]`);
}

// ---------------------------------------------------------------------------
// DynamoDB — campaign state store
//
// DynamoDB is not part of the messaging fan-out itself; it stores the canonical
// campaign record that consumers reference after receiving an SNS event.
// PAY_PER_REQUEST billing avoids capacity planning at this stage.
//
// Key design:
//   pk = "CAMPAIGN#<id>"  (partition key — isolates all items for one campaign)
//   sk = "METADATA"       (sort key — room for future child records, e.g. "RECIPIENT#<id>")
//   StatusCreatedAtIndex GSI swaps pk/sk so you can query by status + creation time
//   (e.g. "all ACTIVE campaigns ordered by createdAt") without a full table scan.
// ---------------------------------------------------------------------------

async function createDynamoTables(): Promise<void> {
  console.log("\nCreating DynamoDB tables...");

  try {
    await dynamo.send(
      new CreateTableCommand({
        TableName: "Campaigns",
        BillingMode: BillingMode.PAY_PER_REQUEST,
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "pk", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
        ],
        // GSI for querying by status + createdAt
        GlobalSecondaryIndexes: [
          {
            IndexName: "StatusCreatedAtIndex",
            KeySchema: [
              { AttributeName: "sk", KeyType: "HASH" },
              { AttributeName: "pk", KeyType: "RANGE" },
            ],
            Projection: { ProjectionType: "ALL" },
          },
        ],
      }),
    );
    console.log("  ✓ Campaigns");
  } catch (err) {
    if ((err as { name?: string }).name === "ResourceInUseException") {
      console.log("  ✓ Campaigns  (already exists)");
    } else {
      throw err;
    }
  }

  // IdempotencyKeys: shared deduplication store for all consumer replicas.
  //   pk (S, HASH) — the composite idempotency key (messageId:eventId)
  //   createdAt (S) — ISO timestamp for debugging
  //   ttl (N)       — Unix epoch; enable TTL on this attribute via UpdateTimeToLive
  //                   to auto-expire keys older than the SQS retention period.
  // No GSI needed: consumers only do exact key lookups (GetItem + PutItem).
  try {
    await dynamo.send(
      new CreateTableCommand({
        TableName: "IdempotencyKeys",
        BillingMode: BillingMode.PAY_PER_REQUEST,
        KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      }),
    );
    console.log("  ✓ IdempotencyKeys");
  } catch (err) {
    if ((err as { name?: string }).name === "ResourceInUseException") {
      console.log("  ✓ IdempotencyKeys  (already exists)");
    } else {
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log(`Connecting to LocalStack at ${LOCALSTACK_ENDPOINT}\n`);

  const topicArns = await createSnsTopics();
  const queueArns = await createSqsQueuesWithDlqs();
  await createEventBridgeBuses();
  await createDynamoTables();
  await createSnsSubscriptions(topicArns, queueArns);
  await createFifoResources();

  console.log("\nInfrastructure setup complete.");
}

main().catch((err: unknown) => {
  console.error("Setup failed:", err);
  process.exit(1);
});
