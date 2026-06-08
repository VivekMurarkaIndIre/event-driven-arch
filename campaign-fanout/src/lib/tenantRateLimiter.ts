// ---------------------------------------------------------------------------
// THREE ISOLATION STRATEGIES FOR MULTI-TENANT EVENT PROCESSING
//
// When multiple tenants share the same SQS queue, a single tenant publishing
// a large burst can degrade latency and throughput for every other tenant in
// that queue — the "noisy neighbour" problem. There are three architectural
// approaches to prevent it, each enforced at a different layer:
//
// ── STRATEGY 1: PER-TENANT QUEUES ───────────────────────────────────────────
//
//   One SQS queue per tenant. Messages are physically isolated from the start.
//
//   Topology:
//     producer → SNS → tenant-a queue → ConsumerA
//                    → tenant-b queue → ConsumerB
//                    → tenant-c queue → ConsumerC
//
//   ✓ Perfect isolation: tenant-a's burst never delays tenant-b/c at all —
//     the queues are independent, have independent depths, and can have
//     independent VisibilityTimeout, maxReceiveCount, and DLQ policies.
//   ✓ Per-tenant SLA: set retention, DLQ thresholds, and alerting individually
//     per tenant or per tier without affecting the rest.
//   ✓ Observability: CloudWatch ApproximateNumberOfMessages and
//     ApproximateAgeOfOldestMessage are already scoped per tenant. No
//     attribution math at query time.
//   ✗ Queue sprawl: 1 000 tenants = 1 000+ queues (+DLQs = 2 000+). IAM
//     policies, CloudFormation stacks, and monitoring dashboards become
//     unmanageable. AWS default limit is 100 000 standard queues per account.
//   ✗ Consumer fleet size: each queue needs at least one polling consumer.
//     Scaling becomes O(tenants), not O(message-rate).
//   ✗ Idle cost: even an inactive tenant's queue incurs CloudWatch metric charges
//     and requires a consumer polling on its behalf.
//
//   Best fit: tens to low-hundreds of tenants; clear SLA differentiation;
//   strict data-isolation requirements (e.g. GDPR per-tenant residency).
//
// ── STRATEGY 2: PER-TIER QUEUES ─────────────────────────────────────────────
//
//   Segment tenants into a small number of tiers (free, pro, enterprise) and
//   route each tier to its own queue via SNS filter policies. This project
//   already uses this for the notifier consumer: the campaign-notifier SNS
//   subscription has a filter { tenantTier: ["pro", "enterprise"] } so free-tier
//   events never enter the notifier queue.
//
//   Topology:
//     SNS → campaign-notifier (filter: tenantTier ∈ {pro, enterprise}) → NotificationConsumer
//         → campaign-analytics (no filter)                             → AnalyticsConsumer
//
//   ✓ Queue count stays constant (2–5) regardless of tenant count.
//   ✓ Free-tier events never enter paid queues: enterprise tenants never
//     contend with free-tier volume for processing slots.
//   ✓ Independent scaling: run a larger consumer fleet for the enterprise tier
//     without scaling the free-tier fleet unnecessarily.
//   ✗ Noisy neighbour within a tier still exists. A large enterprise tenant can
//     delay a small enterprise tenant — the isolation is tier-level, not tenant-level.
//   ✗ SNS filter policies operate only on MessageAttributes (flat key-value),
//     not the message body. Any routing dimension must be published as an attribute
//     at produce time — a coupling between producer and routing layer.
//   ✗ Adding a new tier (e.g. "growth") requires new queues, new subscriptions,
//     new consumer deployments, and updated filter policies — infra and code
//     must change together.
//
//   Best fit: 2–5 tiers with clear SLA boundaries; tier-level isolation is the
//   primary requirement; high tenant count makes per-tenant queues unviable.
//
// ── STRATEGY 3: WEIGHTED FAIR QUEUING IN THE CONSUMER ───────────────────────
//
//   Keep a single shared queue. Move the isolation logic into the consumer:
//   throttle per-tenant throughput with a token-bucket rate limiter (this file)
//   and optionally cap per-tenant in-flight concurrency with a Semaphore
//   (BaseConsumer.maxConcurrentPerTenant). This is the approach implemented here.
//
//   Topology:
//     SNS → campaign-events (single queue) → FairConsumer
//                                             ├─ TenantRateLimiter (token bucket)
//                                             └─ Semaphore (maxConcurrentPerTenant)
//
//   How it works:
//     1. SQS delivers a batch of up to 10 messages, mixed across tenants.
//     2. BaseConsumer fans the batch out concurrently — each message is processed
//        in its own async chain, gated by that tenant's Semaphore.
//     3. Inside processMessageBatch, acquire() checks the token bucket for that
//        tenant. If tokens are available (tenant is within rate), it proceeds
//        immediately. If the bucket is empty, it sleeps until the next token
//        refills, then retries. The noisy tenant is delayed in-process; other
//        tenants' messages in the same batch are unaffected.
//
//   ✓ No queue sprawl: one queue and one consumer fleet regardless of tenant count.
//     New tenants are handled automatically — the bucket is created on first message.
//   ✓ Dynamic configuration: change maxRatePerSecond without infrastructure changes.
//   ✓ Minimal AWS cost: one queue, one set of DLQ alarms, one consumer auto-scaling
//     group — not multiplied by tenant or tier count.
//   ✗ Messages from a noisy tenant still enter the queue and are received by the
//     consumer before being throttled. SQS ApproximateAgeOfOldestMessage does not
//     distinguish "received but throttle-delayed" from "not yet delivered" —
//     age-based alerting becomes noisy.
//   ✗ Rate limiting is per-consumer-instance, not global. With N consumer replicas
//     each enforcing maxRatePerSecond independently, a tenant can effectively issue
//     N × maxRatePerSecond messages/s across the fleet. True global rate limiting
//     requires externalising the token bucket state (e.g. Redis INCR + EXPIRE with
//     a Lua script for atomicity).
//   ✗ In-process throttle delays hold a visibility timeout slot open. Very
//     aggressive throttling (e.g. 1 msg/s) with a 30 s visibility timeout means
//     a message waits up to 29 s inside the consumer before processing — nearly
//     exhausting the visibility window before work even begins.
//
//   Best fit: high tenant count; broadly homogeneous SLA requirements; single-
//   region / single-replica deployment or acceptable tolerance for fleet-wide
//   rate being N × per-replica rate.
// ---------------------------------------------------------------------------

interface TokenBucket {
  tokens: number;
  lastRefillAt: number; // Date.now() milliseconds
}

export class TenantRateLimiter {
  private readonly buckets = new Map<string, TokenBucket>();
  private readonly processedCounts = new Map<string, number>();

  constructor(
    // Maximum messages per second for any individual tenant.
    readonly maxRatePerSecond: number,
    // Maximum tokens a tenant can accumulate (burst capacity).
    // Defaults to maxRatePerSecond — one second of burst before throttling starts.
    // Set lower to reduce the burst window; set higher to allow longer burst tolerance.
    readonly burstCapacity: number = maxRatePerSecond,
  ) {
    if (maxRatePerSecond <= 0) throw new RangeError("maxRatePerSecond must be > 0");
    if (burstCapacity < 1) throw new RangeError("burstCapacity must be >= 1");
  }

  // Acquire a processing token for the given tenant.
  //
  // Returns immediately if the tenant's bucket has tokens available.
  // Otherwise calculates the exact delay until the next token refills and sleeps.
  // Loops after sleeping because a concurrent caller may have consumed the
  // refilled token in the microtask gap — the loop ensures we only proceed
  // when a token is actually ours.
  async acquire(tenantId: string): Promise<void> {
    for (;;) {
      const delayMs = this.tryConsume(tenantId);
      if (delayMs === 0) {
        this.processedCounts.set(
          tenantId,
          (this.processedCounts.get(tenantId) ?? 0) + 1,
        );
        return;
      }
      await new Promise<void>((resolve) => { setTimeout(resolve, delayMs); });
    }
  }

  // Attempt to consume one token from the tenant's bucket.
  // Returns 0 if successful; otherwise returns the milliseconds to wait before
  // the next token refills.
  //
  // JavaScript is single-threaded: this function executes atomically within a
  // single event-loop tick, so concurrent async callers cannot interleave
  // inside tryConsume — they execute sequentially, each seeing the state that
  // the previous caller left behind.
  private tryConsume(tenantId: string): number {
    const now = Date.now();
    let bucket = this.buckets.get(tenantId);

    if (bucket === undefined) {
      // First message from this tenant: start with burstCapacity - 1 tokens
      // (one is consumed by this call), bucket fully recharged from this moment.
      bucket = { tokens: this.burstCapacity - 1, lastRefillAt: now };
      this.buckets.set(tenantId, bucket);
      return 0;
    }

    // Refill tokens proportional to elapsed wall time.
    const elapsedSec = (now - bucket.lastRefillAt) / 1000;
    bucket.tokens = Math.min(
      this.burstCapacity,
      bucket.tokens + elapsedSec * this.maxRatePerSecond,
    );
    bucket.lastRefillAt = now;

    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      return 0;
    }

    // Compute how long until the bucket accumulates one full token.
    const waitSec = (1 - bucket.tokens) / this.maxRatePerSecond;
    return Math.ceil(waitSec * 1000);
  }

  // Per-tenant message counts recorded since the last resetMetrics() call.
  getMetrics(): ReadonlyMap<string, number> {
    return this.processedCounts;
  }

  resetMetrics(): void {
    this.processedCounts.clear();
  }
}
