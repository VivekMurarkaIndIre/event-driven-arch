// A counting semaphore for limiting concurrent access to a shared resource.
//
// acquire() blocks until a permit is available, then claims one.
// release() returns a permit, unblocking the longest-waiting acquire() caller.
//
// Unlike a mutex (binary semaphore with 1 permit), Semaphore(N) allows up to N
// concurrent holders. BaseConsumer uses one semaphore per tenant per consumer
// instance: Semaphore(2) means at most 2 messages from the same tenant can
// be in-flight simultaneously, regardless of how many messages that tenant
// has in the SQS batch.
//
// FIFO ordering: waiters are resolved in the order they called acquire(), so
// a tenant's messages are processed in queue-delivery order within the cap.
export class Semaphore {
  private permits: number;
  private readonly waiters: Array<() => void> = [];

  constructor(permits: number) {
    if (!Number.isInteger(permits) || permits < 1) {
      throw new RangeError(`permits must be a positive integer, got ${permits}`);
    }
    this.permits = permits;
  }

  // Claim one permit. Resolves immediately if a permit is available;
  // otherwise queues the caller until release() is called by a current holder.
  acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  // Return one permit. If callers are waiting, the first in queue is unblocked
  // directly — the permit is never "returned to the pool" and immediately taken
  // again, which would require an extra microtask tick.
  release(): void {
    const next = this.waiters.shift();
    if (next !== undefined) {
      next();
    } else {
      this.permits++;
    }
  }

  // Number of permits not currently held.
  get available(): number {
    return this.permits;
  }

  // Number of callers currently blocked in acquire().
  get queueDepth(): number {
    return this.waiters.length;
  }
}
