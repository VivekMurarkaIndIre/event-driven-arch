import { SNSClient, CreateTopicCommand } from "@aws-sdk/client-sns";
import {
  SQSClient,
  CreateQueueCommand,
  GetQueueAttributesCommand,
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

async function createSnsTopics(): Promise<void> {
  console.log("Creating SNS topics...");
  for (const name of SNS_TOPICS) {
    const { TopicArn } = await sns.send(
      new CreateTopicCommand({
        Name: name,
        Attributes: {},
      }),
    );
    console.log(`  ✓ ${name}  →  ${TopicArn}`);
  }
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

async function createSqsQueuesWithDlqs(): Promise<void> {
  console.log("\nCreating SQS queues + DLQs...");
  for (const name of SQS_QUEUES) {
    const dlqName = `${name}-dlq`;

    // 1. Create DLQ first — its ARN is required before the main queue can be created.
    const { QueueUrl: dlqUrl } = await sqs.send(
      new CreateQueueCommand({
        QueueName: dlqName,
        Attributes: {
          MessageRetentionPeriod: "1209600", // 14 days — long enough to investigate failures
        },
      }),
    );
    if (!dlqUrl) throw new Error(`No QueueUrl returned for DLQ: ${dlqName}`);

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
    //    maxReceiveCount=5: tolerate transient failures (network blips, cold starts)
    //    while preventing a poison message from looping indefinitely.
    //    VisibilityTimeout=30s: safe default for fast processors; increase per-queue
    //    if processing time grows (timeout < processing time = duplicate delivery).
    await sqs.send(
      new CreateQueueCommand({
        QueueName: name,
        Attributes: {
          VisibilityTimeout: "30",
          MessageRetentionPeriod: "345600", // 4 days
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: dlqArn,
            maxReceiveCount: "5",
          }),
        },
      }),
    );

    console.log(`  ✓ ${name}  →  DLQ: ${dlqName}`);
  }
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
    const { EventBusArn } = await eb.send(
      new CreateEventBusCommand({ Name: name }),
    );
    console.log(`  ✓ ${name}  →  ${EventBusArn}`);
  }
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
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log(`Connecting to LocalStack at ${LOCALSTACK_ENDPOINT}\n`);

  await createSnsTopics();
  await createSqsQueuesWithDlqs();
  await createEventBridgeBuses();
  await createDynamoTables();

  console.log("\nInfrastructure setup complete.");
}

main().catch((err: unknown) => {
  console.error("Setup failed:", err);
  process.exit(1);
});
