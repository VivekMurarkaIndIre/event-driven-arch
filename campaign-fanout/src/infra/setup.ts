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

const SNS_TOPICS = [
  "campaign-created",
  "campaign-updated",
  "campaign-deleted",
  "campaign-activated",
] as const;

const SQS_QUEUES = [
  "campaign-processor",
  "campaign-notifier",
  "campaign-analytics",
  "campaign-audit",
] as const;

const EVENT_BUSES = ["campaign-bus", "campaign-dlq-bus"] as const;

// ---------------------------------------------------------------------------
// SNS
// ---------------------------------------------------------------------------

async function createSnsTopics(): Promise<void> {
  console.log("Creating SNS topics...");
  for (const name of SNS_TOPICS) {
    const { TopicArn } = await sns.send(
      new CreateTopicCommand({
        Name: name,
        Attributes: {
          // Enable content-based deduplication for FIFO (remove if using standard)
        },
      }),
    );
    console.log(`  ✓ ${name}  →  ${TopicArn}`);
  }
}

// ---------------------------------------------------------------------------
// SQS  (each queue gets a paired DLQ)
// ---------------------------------------------------------------------------

async function createSqsQueuesWithDlqs(): Promise<void> {
  console.log("\nCreating SQS queues + DLQs...");
  for (const name of SQS_QUEUES) {
    const dlqName = `${name}-dlq`;

    // 1. Create DLQ
    const { QueueUrl: dlqUrl } = await sqs.send(
      new CreateQueueCommand({
        QueueName: dlqName,
        Attributes: {
          MessageRetentionPeriod: "1209600", // 14 days
        },
      }),
    );
    if (!dlqUrl) throw new Error(`No QueueUrl returned for DLQ: ${dlqName}`);

    // 2. Resolve DLQ ARN (needed for the redrive policy)
    const { Attributes: dlqAttrs } = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    const dlqArn = dlqAttrs?.["QueueArn"];
    if (!dlqArn) throw new Error(`Could not retrieve ARN for DLQ: ${dlqName}`);

    // 3. Create main queue pointing at DLQ
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
// EventBridge
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
// DynamoDB
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
