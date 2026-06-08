import {
  SQSClient,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  ChangeMessageVisibilityBatchCommand,
  type MessageAttributeValue,
} from "@aws-sdk/client-sqs";

// DLQs that exist in this project — one per main queue created by infra:setup.
const DLQ_NAMES = [
  "campaign-processor-dlq",
  "campaign-notifier-dlq",
  "campaign-analytics-dlq",
  "campaign-audit-dlq",
] as const;

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";

// Structured alert emitted to stdout as newline-delimited JSON.
// One object per message so log aggregators (CloudWatch Logs Insights,
// Datadog, etc.) can parse and query individual fields without a schema.
interface DlqAlert {
  readonly timestamp: string;
  readonly queueName: string;
  // ApproximateNumberOfMessages from GetQueueAttributes at poll time.
  // This is an estimate; use it as a trend signal, not an exact count.
  readonly approximateDepth: number;
  readonly messageId: string;
  // Number of times SQS has delivered this message to any consumer.
  // By the time a message is in the DLQ this equals maxReceiveCount (3).
  readonly receiveCount: number;
  readonly body: unknown;
  // String-typed message attributes only (binary attributes are excluded).
  readonly attributes: Record<string, string>;
}

export class DlqMonitor {
  private running = false;

  constructor(
    private readonly sqs: SQSClient,
    // dlqName → queue URL, resolved at startup by the entry point below.
    private readonly dlqUrls: ReadonlyMap<string, string>,
    private readonly pollIntervalMs: number,
  ) {}

  async start(): Promise<void> {
    this.running = true;
    console.error(
      `[DlqMonitor] starting — monitoring ${this.dlqUrls.size} DLQ(s) every ${this.pollIntervalMs / 1000}s`,
    );
    while (this.running) {
      for (const [name, url] of this.dlqUrls) {
        await this.checkDlq(name, url).catch((err: unknown) => {
          // A failure on one DLQ should not stop the monitor for the others.
          console.error(`[DlqMonitor] error checking ${name}:`, err);
        });
      }
      await sleep(this.pollIntervalMs);
    }
    console.error("[DlqMonitor] stopped");
  }

  stop(): void {
    this.running = false;
  }

  private async checkDlq(dlqName: string, dlqUrl: string): Promise<void> {
    const { Attributes } = await this.sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ["ApproximateNumberOfMessages"],
      }),
    );

    const approxCount = parseInt(
      Attributes?.["ApproximateNumberOfMessages"] ?? "0",
      10,
    );
    // No messages — nothing to alert on for this DLQ this round.
    if (approxCount === 0) return;

    // Receive up to 10 messages for inspection.
    //
    // VisibilityTimeout: 5 — just long enough to log, short enough that messages
    // return to the DLQ almost immediately. The monitor is read-only; it must not
    // prevent the replay script from claiming the same messages.
    //
    // WaitTimeSeconds: 0 — no long poll. We already know the queue has messages
    // (from GetQueueAttributes) so there is no value in holding an open connection.
    const { Messages: messages = [] } = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: dlqUrl,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 5,
        WaitTimeSeconds: 0,
        AttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      }),
    );

    for (const msg of messages) {
      const receiveCount = parseInt(
        msg.Attributes?.["ApproximateReceiveCount"] ?? "0",
        10,
      );

      let parsedBody: unknown = msg.Body ?? null;
      try {
        parsedBody = JSON.parse(msg.Body ?? "null");
      } catch {
        // Body is not JSON — emit as a raw string.
      }

      const alert: DlqAlert = {
        timestamp: new Date().toISOString(),
        queueName: dlqName,
        approximateDepth: approxCount,
        messageId: msg.MessageId ?? "unknown",
        receiveCount,
        body: parsedBody,
        attributes: extractStringAttributes(msg.MessageAttributes),
      };

      // One JSON object per line — use stdout so this stream can be piped,
      // redirected, or consumed by a log forwarder without mixing with the
      // operational status lines emitted to stderr above.
      console.log(JSON.stringify(alert));
    }

    // Release all peeked messages back to the DLQ immediately (VisibilityTimeout=0).
    // Without this, messages stay hidden for the 5-second window we requested above.
    // Explicit release ensures replay and subsequent monitor polls see them right away.
    const toRelease: Array<{ id: string; receiptHandle: string }> = [];
    for (const [i, m] of messages.entries()) {
      if (m.MessageId !== undefined && m.ReceiptHandle !== undefined) {
        toRelease.push({ id: String(i), receiptHandle: m.ReceiptHandle });
      }
    }

    if (toRelease.length > 0) {
      await this.sqs.send(
        new ChangeMessageVisibilityBatchCommand({
          QueueUrl: dlqUrl,
          Entries: toRelease.map(({ id, receiptHandle }) => ({
            Id: id,
            ReceiptHandle: receiptHandle,
            VisibilityTimeout: 0,
          })),
        }),
      );
    }
  }
}

function extractStringAttributes(
  attrs: Record<string, MessageAttributeValue> | undefined,
): Record<string, string> {
  if (!attrs) return {};
  const result: Record<string, string> = {};
  for (const [key, val] of Object.entries(attrs)) {
    if (val.DataType === "String" && val.StringValue !== undefined) {
      result[key] = val.StringValue;
    }
  }
  return result;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

const sqs = new SQSClient({
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

async function main(): Promise<void> {
  console.error("[DlqMonitor] resolving DLQ URLs...");
  const dlqUrls = new Map<string, string>();
  for (const name of DLQ_NAMES) {
    const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
    if (!QueueUrl) throw new Error(`Could not resolve URL for DLQ: ${name}`);
    dlqUrls.set(name, QueueUrl);
    console.error(`  ${name} → ${QueueUrl}`);
  }

  const monitor = new DlqMonitor(sqs, dlqUrls, 10_000);

  process.on("SIGINT", () => {
    monitor.stop();
  });

  await monitor.start();
}

main().catch((err: unknown) => {
  console.error("[DlqMonitor] fatal:", err);
  process.exit(1);
});
