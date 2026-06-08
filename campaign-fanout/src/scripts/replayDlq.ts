// Usage: npx tsx src/scripts/replayDlq.ts <dlq-name>
// Example: npx tsx src/scripts/replayDlq.ts campaign-analytics-dlq
//
// Reads all messages from the named DLQ and sends each one to the corresponding
// main SQS queue. The main queue name is derived by stripping the "-dlq" suffix.
// Messages are deleted from the DLQ only after a successful SendMessage, so a
// crash mid-replay leaves the message in place for the next run.
//
// Idempotency — why replayedAt prevents infinite replay loops:
//   Every re-queued message carries a "replayedAt" string MessageAttribute.
//   If the message fails processing again and lands back in the DLQ, it will
//   carry this attribute. On the next replay run, the script detects replayedAt
//   and skips the message rather than re-queuing it endlessly. A message that
//   has survived one replay and still fails is a persistent bug (bad data shape,
//   missing downstream dependency, logic error) — replaying it again will not
//   help and will only delay the investigation.
//
// Note: we re-publish directly to the main SQS queue, NOT through SNS.
//   Re-publishing through SNS would fan-out to all subscribed queues, turning a
//   targeted single-queue replay into a broadcast and polluting unrelated queues
//   with a second copy of every replayed message.

import {
  SQSClient,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";

const dlqName = process.argv[2];
if (!dlqName) {
  console.error("Usage: npx tsx src/scripts/replayDlq.ts <dlq-name>");
  console.error("Example: npx tsx src/scripts/replayDlq.ts campaign-analytics-dlq");
  process.exit(1);
}

if (!dlqName.endsWith("-dlq")) {
  console.error(`DLQ name must end with "-dlq". Got: ${dlqName}`);
  process.exit(1);
}

const mainQueueName = dlqName.replace(/-dlq$/, "");

const sqs = new SQSClient({
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

async function resolveQueueUrl(queueName: string): Promise<string> {
  const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
  if (!QueueUrl) throw new Error(`Could not resolve URL for queue: ${queueName}`);
  return QueueUrl;
}

const dlqUrl = await resolveQueueUrl(dlqName);
const mainQueueUrl = await resolveQueueUrl(mainQueueName);

console.log(`Replaying: ${dlqName} → ${mainQueueName}`);
console.log(`  DLQ:        ${dlqUrl}`);
console.log(`  Main queue: ${mainQueueUrl}`);
console.log();

let replayed = 0;
let skipped = 0;
let failed = 0;

// Drain loop — stop when ReceiveMessage returns empty.
// WaitTimeSeconds: 0 is intentional: we want to exhaust the queue and exit
// rather than wait for new messages. Long polling (20s) is correct for normal
// consumers but wrong for a drain-and-exit replay script.
while (true) {
  const { Messages: messages = [] } = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: dlqUrl,
      MaxNumberOfMessages: 10,
      // 60s: enough time to call SendMessage + DeleteMessage for all 10 messages
      // in this batch before SQS makes them visible to another process.
      VisibilityTimeout: 60,
      WaitTimeSeconds: 0,
      AttributeNames: ["All"],
      MessageAttributeNames: ["All"],
    }),
  );

  if (messages.length === 0) break;

  for (const msg of messages) {
    if (!msg.MessageId || !msg.ReceiptHandle || msg.Body === undefined) {
      console.warn(
        JSON.stringify({ event: "warn_malformed", detail: "missing MessageId/ReceiptHandle/Body" }),
      );
      continue;
    }

    const messageId = msg.MessageId;
    const receiptHandle = msg.ReceiptHandle;

    // Idempotency check: replayedAt present means this message was already
    // replayed once. It either failed again (back in DLQ after another
    // maxReceiveCount attempts) or the DLQ delete failed on the previous run.
    // Either way, re-queuing it again will not break the infinite loop because
    // the attribute persists on the message in the main queue, and if it DLQs
    // again we will skip it on the next replay. Log for manual investigation.
    const replayedAtAttr = msg.MessageAttributes?.["replayedAt"];
    if (replayedAtAttr !== undefined) {
      console.log(
        JSON.stringify({
          event: "skipped_already_replayed",
          messageId,
          replayedAt: replayedAtAttr.StringValue ?? "(unknown)",
          detail: "message has been replayed before and failed again — needs manual investigation",
        }),
      );
      skipped++;
      continue;
    }

    const replayTimestamp = new Date().toISOString();

    try {
      // Re-queue into the main queue with all original attributes preserved
      // plus the new replayedAt marker. The message body is unchanged so the
      // consumer sees the exact same payload it would have seen from SNS.
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: mainQueueUrl,
          MessageBody: msg.Body,
          MessageAttributes: {
            ...(msg.MessageAttributes ?? {}),
            replayedAt: {
              DataType: "String",
              StringValue: replayTimestamp,
            },
          },
        }),
      );

      // Delete from the DLQ only after SendMessage succeeds.
      // If this delete fails, the message stays in the DLQ without a replayedAt
      // attribute (DLQ message attributes are immutable). On the next replay run
      // it will be re-sent to the main queue. The consumer's idempotency layer
      // (keyed on messageId + campaignId) handles the resulting duplicate.
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: dlqUrl,
          ReceiptHandle: receiptHandle,
        }),
      );

      console.log(JSON.stringify({ event: "replayed", messageId, replayedAt: replayTimestamp }));
      replayed++;
    } catch (err) {
      // SendMessage or DeleteMessage failed. Leave the message in the DLQ —
      // it will become visible again after the VisibilityTimeout expires and
      // can be retried on the next replay run.
      console.error(JSON.stringify({ event: "failed", messageId, error: String(err) }));
      failed++;
    }
  }
}

console.log();
console.log(`Done.  replayed=${replayed}  skipped=${skipped}  failed=${failed}`);
if (skipped > 0) {
  console.log(
    `  ${skipped} message(s) skipped — already replayed once. Check logs above for messageId details.`,
  );
}
if (failed > 0) {
  console.log(
    `  ${failed} message(s) failed to re-queue — still in DLQ. Re-run this script to retry.`,
  );
}
