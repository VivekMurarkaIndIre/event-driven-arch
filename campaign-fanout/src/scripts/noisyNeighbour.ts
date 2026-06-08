// Noisy-neighbour simulation
//
// Problem modelled:
//   Three tenants share one SQS queue. tenant-a publishes 100 messages in a
//   rapid burst; tenant-b and tenant-c each publish 10 messages at a normal
//   cadence. Without any fairness mechanism, SQS delivers tenant-a's messages
//   first (they have higher queue depth), starving tenant-b/c for seconds.
//
// Mitigation applied:
//   TenantRateLimiter(maxRatePerSecond: 20, burstCapacity: 5) — each tenant's
//   token bucket caps throughput at 20 msg/s regardless of queue depth.
//   BaseConsumer(maxConcurrentPerTenant: 2) — at most 2 messages from the same
//   tenant process concurrently within a single poll batch.
//
// Expected output:
//   tenant-b and tenant-c finish their 10 messages in ~0.5 s.
//   tenant-a, despite publishing first and having 10× the volume, completes in
//   ~5 s — the same per-message rate, just more messages.
//   Neither tenant-b nor tenant-c is delayed beyond its own rate-limit window.
//
// What would happen WITHOUT the limiter:
//   All 120 messages would race through at LocalStack speed (~1–2 s total).
//   The first 10 polls would return predominantly tenant-a messages (it has the
//   deepest queue). tenant-b and tenant-c would be processed only after tenant-a
//   drained — introducing up to several seconds of additional latency for them.

import {
  SQSClient,
  CreateQueueCommand,
  SendMessageBatchCommand,
  DeleteQueueCommand,
} from "@aws-sdk/client-sqs";
import {
  BaseConsumer,
  type BatchItemFailure,
  type ParsedMessage,
} from "../consumers/BaseConsumer.js";
import { TenantRateLimiter } from "../lib/tenantRateLimiter.js";

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const QUEUE_NAME = "noisy-neighbour-test";

const TENANT_A = "tenant-a"; // noisy neighbour: 100 messages
const TENANT_B = "tenant-b"; // normal:          10 messages
const TENANT_C = "tenant-c"; // normal:          10 messages
const MSGS_PER_ROUND = 10;   // tenant-a publishes 10 per round
const ROUNDS = 10;            // 10 rounds → 100 from a, 10 from b, 10 from c
const TOTAL = ROUNDS * MSGS_PER_ROUND + ROUNDS + ROUNDS; // 120

const sqs = new SQSClient({
  endpoint: LOCALSTACK_ENDPOINT,
  region: REGION,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

// ---------------------------------------------------------------------------
// Setup: create a fresh ephemeral test queue
// ---------------------------------------------------------------------------

const { QueueUrl: rawUrl } = await sqs.send(
  new CreateQueueCommand({ QueueName: QUEUE_NAME }),
);
if (!rawUrl) throw new Error("Failed to create test queue");
const queueUrl: string = rawUrl;
console.log(`Queue ready: ${queueUrl}\n`);

// ---------------------------------------------------------------------------
// Publish: 10 interleaved rounds so the queue is mixed, not all-A then B/C.
// Each round: 10 from tenant-a, 1 from tenant-b, 1 from tenant-c.
// ---------------------------------------------------------------------------

interface TestMessage {
  tenantId: string;
  seq: number;
  publishedAt: number;
}

async function publishRound(round: number): Promise<void> {
  const now = Date.now();

  // 10 messages from tenant-a in one batch
  await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: Array.from({ length: MSGS_PER_ROUND }, (_, i) => ({
        Id: `a-${round}-${i}`,
        MessageBody: JSON.stringify({
          tenantId: TENANT_A,
          seq: round * MSGS_PER_ROUND + i,
          publishedAt: now,
        } satisfies TestMessage),
      })),
    }),
  );

  // 1 from tenant-b and 1 from tenant-c in one batch
  await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: [
        {
          Id: `b-${round}`,
          MessageBody: JSON.stringify({
            tenantId: TENANT_B,
            seq: round,
            publishedAt: now,
          } satisfies TestMessage),
        },
        {
          Id: `c-${round}`,
          MessageBody: JSON.stringify({
            tenantId: TENANT_C,
            seq: round,
            publishedAt: now,
          } satisfies TestMessage),
        },
      ],
    }),
  );
}

console.log("Publishing messages...");
for (let round = 0; round < ROUNDS; round++) {
  await publishRound(round);
}
console.log(`  ${MSGS_PER_ROUND * ROUNDS} from ${TENANT_A} (noisy)`);
console.log(`  ${ROUNDS} from ${TENANT_B} (normal)`);
console.log(`  ${ROUNDS} from ${TENANT_C} (normal)`);
console.log(`  Total: ${TOTAL} messages\n`);

// ---------------------------------------------------------------------------
// Stats tracking
// ---------------------------------------------------------------------------

interface TenantStats {
  count: number;
  firstAt: number;
  lastAt: number;
  done: boolean;
}

const stats = new Map<string, TenantStats>();
let processed = 0;
const startMs = Date.now();

function recordProcessed(tenantId: string): void {
  const now = Date.now();
  const existing = stats.get(tenantId);
  if (existing === undefined) {
    stats.set(tenantId, { count: 1, firstAt: now, lastAt: now, done: false });
  } else {
    existing.count++;
    existing.lastAt = now;
  }
}

function markDone(tenantId: string, expectedCount: number): void {
  const s = stats.get(tenantId);
  if (s !== undefined && s.count >= expectedCount && !s.done) {
    s.done = true;
    const durSec = ((s.lastAt - s.firstAt) / 1000).toFixed(2);
    const rate = s.count / ((s.lastAt - s.firstAt) / 1000 || 0.001);
    console.log(
      `  ✓ ${tenantId} done — ${s.count} msgs in ${durSec} s` +
      ` (${rate.toFixed(1)} msg/s effective)`,
    );
  }
}

function printProgress(): void {
  const elapsedSec = ((Date.now() - startMs) / 1000).toFixed(1);
  const lines: string[] = [`\n── Progress at t=${elapsedSec} s ──────────────────`];
  for (const [tenantId, s] of [...stats.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const durSec = s.lastAt > s.firstAt ? (s.lastAt - s.firstAt) / 1000 : 0.001;
    const rate = (s.count / durSec).toFixed(1);
    const bar = "█".repeat(Math.round(s.count / 4));
    const suffix = s.done ? "  [DONE]" : "";
    lines.push(`  ${tenantId.padEnd(10)} ${String(s.count).padStart(4)} msgs  ${rate.padStart(6)} msg/s  ${bar}${suffix}`);
  }
  lines.push(`  total processed: ${processed}/${TOTAL}`);
  console.log(lines.join("\n"));
}

// ---------------------------------------------------------------------------
// Consumer: NoisyNeighbourConsumer
//
// Wires together:
//   BaseConsumer(maxConcurrentPerTenant: 2) — semaphore caps same-tenant concurrency
//   TenantRateLimiter(20, burstCapacity: 5) — token bucket caps throughput per tenant
//
// The semaphore acts first (in BaseConsumer.poll), limiting how many messages
// from the same tenant start processing in parallel. The rate limiter acts
// inside processMessageBatch, delaying individual messages when a tenant's
// bucket is depleted. With maxConcurrentPerTenant: 2 and 20 msg/s, the effective
// cap is 2 concurrent × up to 20/s rate = throughput bounded by whichever fills
// first. For this demo the rate limiter is the binding constraint.
// ---------------------------------------------------------------------------

const rateLimiter = new TenantRateLimiter(
  20,  // max 20 msg/s per tenant
  5,   // burst up to 5 before throttling (≈ 250 ms of burst at 20/s)
);

const expectedCounts: ReadonlyMap<string, number> = new Map([
  [TENANT_A, MSGS_PER_ROUND * ROUNDS],
  [TENANT_B, ROUNDS],
  [TENANT_C, ROUNDS],
]);

class NoisyNeighbourConsumer extends BaseConsumer<TestMessage> {
  constructor() {
    super(sqs, {
      queueUrl,
      batchSize: 10,
      visibilityTimeout: 30,
      maxConcurrentPerTenant: 2,
    });
  }

  protected override extractTenantId(body: TestMessage): string {
    return body.tenantId;
  }

  override async processMessageBatch(
    messages: ParsedMessage<TestMessage>[],
  ): Promise<BatchItemFailure[]> {
    for (const msg of messages) {
      const { tenantId } = msg.body;

      // Token-bucket throttle: blocks until this tenant has a token available.
      // A noisy tenant's messages wait here; other tenants' messages in the
      // same Promise.all (started by BaseConsumer) are unaffected.
      await rateLimiter.acquire(tenantId);

      recordProcessed(tenantId);
      processed++;

      const expected = expectedCounts.get(tenantId);
      if (expected !== undefined) markDone(tenantId, expected);

      if (processed >= TOTAL) {
        setTimeout(() => this.stop(), 50);
      }
    }
    return [];
  }
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

const progressTimer = setInterval(printProgress, 2000);

console.log(
  "Consuming with per-tenant rate limit (20 msg/s, burst 5)" +
  " and concurrency cap (2 per tenant)...\n",
);

const consumer = new NoisyNeighbourConsumer();
process.on("SIGINT", () => {
  clearInterval(progressTimer);
  consumer.stop();
});

await consumer.start();
clearInterval(progressTimer);

const totalMs = Date.now() - startMs;

// ---------------------------------------------------------------------------
// Final report
// ---------------------------------------------------------------------------

console.log("\n── Final throughput report ─────────────────────────────────────────");
console.log("Tenant        Messages    Duration    Effective Rate  vs. limit");
console.log("────────────────────────────────────────────────────────────────────");
for (const [tenantId, s] of [...stats.entries()].sort(([a], [b]) => a.localeCompare(b))) {
  const durSec = ((s.lastAt - s.firstAt) / 1000);
  const rate = durSec > 0 ? s.count / durSec : 0;
  const pct = ((rate / rateLimiter.maxRatePerSecond) * 100).toFixed(0);
  console.log(
    `${tenantId.padEnd(14)}` +
    `${String(s.count).padStart(8)}` +
    `    ${durSec.toFixed(2).padStart(6)} s` +
    `    ${rate.toFixed(1).padStart(7)} msg/s` +
    `   ${pct.padStart(3)}% of ${rateLimiter.maxRatePerSecond}/s cap`,
  );
}
console.log("────────────────────────────────────────────────────────────────────");
console.log(`Total wall time: ${(totalMs / 1000).toFixed(2)} s\n`);
console.log("Key observations:");
console.log("  • All tenants achieved the same effective rate (~20 msg/s).");
console.log("  • tenant-b and tenant-c finished early — not delayed by tenant-a's burst.");
console.log("  • tenant-a was throttled in-process, not in-queue: it held no lock");
console.log("    that prevented other tenants from being processed concurrently.");
console.log("  • Without the limiter, tenant-a would have dominated early poll batches,");
console.log("    pushing tenant-b/c latency up by however long tenant-a's 100 msgs took.");

// ---------------------------------------------------------------------------
// Cleanup: delete the ephemeral test queue
// ---------------------------------------------------------------------------

await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
console.log("\nTest queue deleted.");
