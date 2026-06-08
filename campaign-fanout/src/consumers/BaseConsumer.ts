import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageBatchCommand,
  ChangeMessageVisibilityBatchCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import { Semaphore } from "../lib/semaphore.js";

// Returned by processMessageBatch for each message that failed processing.
// itemIdentifier must be the SQS MessageId of the failing message.
//
// Partial batch response: by returning exactly the messages that failed, the
// base class can delete every successful message while leaving failed messages
// in the queue for SQS to retry (up to the queue's maxReceiveCount, then DLQ).
//
// Without partial batch response a failure anywhere in the batch forces a
// binary choice:
//   Delete all  → messages that genuinely failed are silently dropped (data loss).
//   Delete none → messages that already succeeded are re-delivered and processed
//                 again (duplicate work, requiring consumers to be idempotent for
//                 the wrong reason — not because SQS can duplicate, but because
//                 we chose to re-deliver on purpose).
// Per-message failure reporting eliminates both problems.
export interface BatchItemFailure {
  readonly itemIdentifier: string;
}

// The fully parsed message handed to processMessageBatch.
// body is the deserialized event payload; the raw SQS envelope is not exposed.
export interface ParsedMessage<TBody> {
  readonly messageId: string;
  readonly receiptHandle: string;
  readonly body: TBody;
}

export interface ConsumerConfig {
  readonly queueUrl: string;
  // 1–10; SQS hard maximum per ReceiveMessage call.
  readonly batchSize: number;
  // Seconds a received message stays hidden from other consumers.
  // Also controls the extension loop interval (fires at visibilityTimeout / 2).
  readonly visibilityTimeout: number;
  // Maximum number of messages from a single tenant that may be processed
  // concurrently within one poll cycle. When set, the poll loop fans the batch
  // out as individual concurrent tasks; each task acquires that tenant's
  // semaphore before calling processMessageBatch([msg]).
  //
  // Messages from different tenants always proceed independently — only same-tenant
  // concurrency is capped. Override extractTenantId() to identify tenants;
  // the default implementation returns "default", making this a global cap.
  //
  // When unset (default), processMessageBatch receives the full batch at once
  // and the subclass controls all concurrency internally.
  readonly maxConcurrentPerTenant?: number;
}

// SNS wraps the original event payload in an outer JSON object when it delivers
// to an SQS subscription. The actual event is inside the Message string field.
interface SnsEnvelope {
  Type: "Notification";
  MessageId: string;
  Message: string; // JSON-stringified inner payload
}

function isSnsEnvelope(value: unknown): value is SnsEnvelope {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  return obj["Type"] === "Notification" && typeof obj["Message"] === "string";
}

// EventBridge wraps the payload in a full event envelope when it delivers to
// an SQS target. The business payload lives in the `detail` field as a parsed
// object — unlike SNS, where `Message` is a JSON string that must be re-parsed.
interface EventBridgeEnvelope {
  version: string;
  id: string;
  source: string;
  "detail-type": string;
  detail: unknown;
}

function isEventBridgeEnvelope(value: unknown): value is EventBridgeEnvelope {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    typeof obj["source"] === "string" &&
    typeof obj["detail-type"] === "string" &&
    "detail" in obj
  );
}

export abstract class BaseConsumer<TBody> {
  private running = false;
  // One semaphore per observed tenant id. Created lazily on first message from
  // each tenant. Only populated when maxConcurrentPerTenant is configured.
  private readonly tenantSemaphores = new Map<string, Semaphore>();

  constructor(
    protected readonly sqs: SQSClient,
    protected readonly config: ConsumerConfig,
  ) {}

  // Subclasses implement business logic for a received batch.
  // Return a BatchItemFailure for every message that could not be processed.
  // Messages not in the failure list are deleted from the queue on return.
  //
  // When maxConcurrentPerTenant is set the base class calls this method with
  // a single-message array per message (each wrapped in its tenant's semaphore).
  // Subclasses that do batch operations (e.g. DynamoDB BatchWrite) should be
  // aware that with per-tenant limiting they receive one message at a time.
  abstract processMessageBatch(
    messages: ParsedMessage<TBody>[],
  ): Promise<BatchItemFailure[]>;

  // Override to enable per-tenant concurrency capping when maxConcurrentPerTenant
  // is set. Return a stable string that uniquely identifies the tenant for a given
  // message body. The default "default" collapses all messages into one semaphore,
  // acting as a global concurrency cap.
  protected extractTenantId(_body: TBody): string {
    return "default";
  }

  private getOrCreateSemaphore(tenantId: string): Semaphore {
    let sem = this.tenantSemaphores.get(tenantId);
    if (sem === undefined) {
      // maxConcurrentPerTenant is guaranteed defined at call sites.
      sem = new Semaphore(this.config.maxConcurrentPerTenant as number);
      this.tenantSemaphores.set(tenantId, sem);
    }
    return sem;
  }

  async start(): Promise<void> {
    this.running = true;
    console.log(`[${this.constructor.name}] starting — queue: ${this.config.queueUrl}`);
    while (this.running) {
      await this.poll();
    }
    console.log(`[${this.constructor.name}] stopped`);
  }

  stop(): void {
    this.running = false;
  }

  private async poll(): Promise<void> {
    const { Messages: raw = [] } = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: this.config.queueUrl,
        MaxNumberOfMessages: this.config.batchSize,
        // Long polling: the SQS service holds the connection open for up to
        // WaitTimeSeconds before returning an empty response. Without it, the
        // polling loop would hammer the endpoint thousands of times per minute
        // on a quiet queue — burning API request budget and CPU for nothing.
        // WaitTimeSeconds: 20 is the maximum and is the recommended default.
        WaitTimeSeconds: 20,
        VisibilityTimeout: this.config.visibilityTimeout,
        AttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      }),
    );

    if (raw.length === 0) return;

    const messages = this.parseMessages(raw);
    if (messages.length === 0) return;

    const receiptHandles = messages.map((m) => m.receiptHandle);

    // Visibility timeout extension loop.
    //
    // When SQS delivers a message it starts a countdown: after visibilityTimeout
    // seconds the message becomes visible again and any available consumer can
    // receive it — even if we are still processing it. For short timeouts (30 s)
    // this is fine for fast processors, but a batch of 10 messages with non-trivial
    // per-message work can easily exceed it.
    //
    // ChangeMessageVisibilityBatch resets the deadline to "now + visibilityTimeout"
    // for each handle in the call. The extension interval MUST be strictly less
    // than visibilityTimeout. If the interval fires at or after the timeout has
    // already expired, the message is already visible to other consumers — we can
    // no longer extend it and a duplicate delivery is in flight. Firing at
    // Math.floor(visibilityTimeout / 2) guarantees we always renew while at least
    // half the timeout remains, providing a comfortable safety margin.
    //
    // Example: visibilityTimeout = 30 s, extensionIntervalMs = 15 000 ms.
    //   t=0s   message received, visibility deadline = t+30s
    //   t=15s  extension fires → deadline reset to t+45s
    //   t=30s  extension fires → deadline reset to t+60s
    //   t=45s  processing completes, message deleted
    // The message was never visible during processing.
    const extensionIntervalMs = Math.floor(this.config.visibilityTimeout / 2) * 1000;
    const extensionTimer = setInterval(() => {
      void this.extendVisibility(receiptHandles);
    }, extensionIntervalMs);

    let failures: BatchItemFailure[] = [];
    try {
      if (this.config.maxConcurrentPerTenant !== undefined) {
        // Fan the batch out as individual concurrent tasks. Each task acquires
        // its tenant's semaphore before calling processMessageBatch([msg]), so
        // at most maxConcurrentPerTenant messages from the same tenant run at once.
        // Messages from different tenants proceed independently — a noisy tenant
        // can only block itself, not its neighbours.
        //
        // processMessageBatch is called with a single-message array; subclasses
        // that iterate with for...of are unaffected. Subclasses relying on batch
        // operations across messages (e.g. DynamoDB BatchWrite) should override
        // extractTenantId to return the same key for all messages, which collapses
        // the per-tenant cap into a global concurrency cap and preserves batching.
        const results = await Promise.all(
          messages.map(async (msg) => {
            const tenantId = this.extractTenantId(msg.body);
            const sem = this.getOrCreateSemaphore(tenantId);
            await sem.acquire();
            try {
              return await this.processMessageBatch([msg]);
            } catch (err) {
              console.error(
                `[${this.constructor.name}] unhandled error for ${msg.messageId}:`, err,
              );
              return [{ itemIdentifier: msg.messageId }];
            } finally {
              sem.release();
            }
          }),
        );
        failures = results.flat();
      } else {
        failures = await this.processMessageBatch(messages);
      }
    } catch (err) {
      // Unexpected throw outside the per-message paths above (e.g. Promise.all
      // itself, which cannot happen when each map callback catches its own errors).
      // Kept as a safety net so a bug here doesn't silently delete messages.
      console.error(`[${this.constructor.name}] unhandled batch error:`, err);
      failures = messages.map((m) => ({ itemIdentifier: m.messageId }));
    } finally {
      clearInterval(extensionTimer);
    }

    await this.deleteSuccessful(messages, failures);
  }

  private parseMessages(rawMessages: Message[]): ParsedMessage<TBody>[] {
    const parsed: ParsedMessage<TBody>[] = [];

    for (const raw of rawMessages) {
      if (!raw.MessageId || !raw.ReceiptHandle || !raw.Body) {
        console.warn(`[${this.constructor.name}] skipping message with missing fields`);
        continue;
      }

      try {
        const outer: unknown = JSON.parse(raw.Body);
        // Unwrap delivery envelopes so subclasses always receive the plain payload.
        // SNS envelope:         { Type: "Notification", Message: "<json-string>" }
        //   → re-parse Message string to get the payload object.
        // EventBridge envelope: { source: "...", "detail-type": "...", detail: {...} }
        //   → detail is already a parsed object, no second JSON.parse needed.
        // Direct body (no envelope): the parsed outer IS the payload.
        let body: TBody;
        if (isSnsEnvelope(outer)) {
          body = JSON.parse(outer.Message) as TBody;
        } else if (isEventBridgeEnvelope(outer)) {
          body = outer.detail as TBody;
        } else {
          body = outer as TBody;
        }

        parsed.push({
          messageId: raw.MessageId,
          receiptHandle: raw.ReceiptHandle,
          body,
        });
      } catch (err) {
        console.error(
          `[${this.constructor.name}] failed to parse message ${raw.MessageId}:`,
          err,
        );
        // Unparseable messages are not pushed and therefore not processed or
        // deleted. They will exhaust their maxReceiveCount and land in the DLQ
        // where they can be inspected. Do not swallow them silently.
      }
    }

    return parsed;
  }

  private async extendVisibility(receiptHandles: string[]): Promise<void> {
    if (receiptHandles.length === 0) return;
    try {
      await this.sqs.send(
        new ChangeMessageVisibilityBatchCommand({
          QueueUrl: this.config.queueUrl,
          Entries: receiptHandles.map((handle, i) => ({
            Id: String(i),
            ReceiptHandle: handle,
            // Reset the deadline to visibilityTimeout seconds from now.
            VisibilityTimeout: this.config.visibilityTimeout,
          })),
        }),
      );
    } catch (err) {
      // Log but do not rethrow: a failed extension is not fatal for the current
      // poll cycle. The message may become visible to another consumer, but the
      // idempotency layer will handle the resulting duplicate.
      console.error(`[${this.constructor.name}] visibility extension failed:`, err);
    }
  }

  private async deleteSuccessful(
    messages: ParsedMessage<TBody>[],
    failures: BatchItemFailure[],
  ): Promise<void> {
    const failedIds = new Set(failures.map((f) => f.itemIdentifier));
    const toDelete = messages.filter((m) => !failedIds.has(m.messageId));

    if (toDelete.length === 0) return;

    const { Failed: deleteFailed } = await this.sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: this.config.queueUrl,
        Entries: toDelete.map((m) => ({
          Id: m.messageId,
          ReceiptHandle: m.receiptHandle,
        })),
      }),
    );

    if (deleteFailed && deleteFailed.length > 0) {
      console.error(
        `[${this.constructor.name}] ${deleteFailed.length} message(s) failed to delete:`,
        deleteFailed,
      );
    }
  }
}
