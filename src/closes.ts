import { FanoutClose } from './fanoutClose.js';
import { FanoutTombstone } from './fanoutTombstone.js';

// A gateway connection lives at most 2h.
export const CLOSE_TTL_MS = 2 * 60 * 60 * 1000;
const REPULL_MS = 60 * 1000;
const MAX_IN_FLIGHT = 16;
const RETRY_BASE_MS = 1000;
const RETRY_MAX_MS = 60 * 1000;

type Closing = { endpoint: string; connectionId: string; firstAttemptAt: number };

export type CloseStore = {
  load: () => Promise<Closing[]>;
  save: (closings: Closing[]) => Promise<unknown>;
  remove: (closing: Pick<Closing, 'endpoint' | 'connectionId'>) => Promise<unknown>;
};

export const closeStore: CloseStore = {
  load: async () =>
    (await FanoutClose.find().lean()).map(({ endpoint, connectionId, firstAttemptAt }) => ({
      endpoint,
      connectionId,
      firstAttemptAt: new Date(firstAttemptAt).getTime(),
    })),
  save: closings =>
    FanoutClose.bulkWrite(
      closings.map(({ endpoint, connectionId, firstAttemptAt }) => ({
        updateOne: {
          filter: { endpoint, connectionId },
          update: { $setOnInsert: { firstAttemptAt: new Date(firstAttemptAt) } },
          upsert: true,
        },
      })),
      { ordered: false }
    ),
  remove: ({ endpoint, connectionId }) => FanoutClose.deleteOne({ endpoint, connectionId }),
};

export type CloseHandlers = {
  delete: (connectionId: string, endpoint: string) => Promise<unknown>;
  // The gateway confirmed the close or reported the connection gone, or 2h have passed.
  closed: (endpoint: string, connectionId: string) => void;
  // A DELETE answered 403.
  forbidden?: (endpoint: string, connectionId: string) => void;
  // The gateway credentials expired.
  expired?: (error: unknown) => void;
};

type Tombstone = { endpoint: string; connectionId: string; reason: string; expiresAt: number };

export type TombstoneStore = {
  load: () => Promise<Tombstone[]>;
  save: (tombstone: Tombstone) => Promise<unknown>;
};

export const tombstoneStore: TombstoneStore = {
  load: async () =>
    (await FanoutTombstone.find({ expiresAt: { $gt: new Date() } }).lean()).map(({ endpoint, connectionId, reason, expiresAt }) => ({
      endpoint,
      connectionId,
      reason,
      expiresAt: new Date(expiresAt).getTime(),
    })),
  save: ({ endpoint, connectionId, reason, expiresAt }) =>
    FanoutTombstone.updateOne(
      { _id: `${endpoint} ${connectionId}` },
      { $set: { endpoint, connectionId, reason, expiresAt: new Date(expiresAt) } },
      { upsert: true }
    ),
};

// Every connection the fanout has closed, found gone or removed in the last 2h, persisted so a restart keeps
// them. Held apart from the pending closes, which the close pump walks.
export class Tombstones {
  private expiry: Map<string, number> = new Map();
  private pulledAt: Map<string, number> = new Map();
  private sweptAt = 0;

  constructor(private store: TombstoneStore = tombstoneStore) {}

  get size() {
    return this.expiry.size;
  }

  async load() {
    for (const { endpoint, connectionId, expiresAt } of await this.store.load()) {
      this.expiry.set(`${endpoint} ${connectionId}`, expiresAt);
    }
  }

  // Resolves to whether the tombstone was stored.
  add(endpoint: string, connectionId: string, reason: string, expiresAt = Date.now() + CLOSE_TTL_MS) {
    this.sweep();
    const key = `${endpoint} ${connectionId}`;
    if ((this.expiry.get(key) ?? 0) < expiresAt) this.expiry.set(key, expiresAt);
    return this.store.save({ endpoint, connectionId, reason, expiresAt }).then(
      () => true,
      error => {
        console.error(`failed to record the removal of connection ${connectionId}: ${error}`);
        return false;
      }
    );
  }

  has(key: string) {
    const expiresAt = this.expiry.get(key);
    return expiresAt !== undefined && expiresAt > Date.now();
  }

  // Whether a list that still names this connection should pull it again: at most once a minute per tombstone.
  repull(key: string) {
    const now = Date.now();
    if (now - (this.pulledAt.get(key) ?? -Infinity) < REPULL_MS) return false;
    this.pulledAt.set(key, now);
    return true;
  }

  private sweep() {
    const now = Date.now();
    if (now - this.sweptAt < REPULL_MS) return;
    this.sweptAt = now;
    for (const [key, expiresAt] of this.expiry) {
      if (expiresAt > now) continue;
      this.expiry.delete(key);
      this.pulledAt.delete(key);
    }
    for (const [key, at] of this.pulledAt) if (now - at >= REPULL_MS) this.pulledAt.delete(key);
  }
}

type Pending = Closing & { attempts: number; nextAt: number; inFlight: boolean };

// Closes connections through DeleteConnection, retrying until the gateway confirms or 2h have passed, and
// keeps them blocked meanwhile. Persisted, so a restart carries them over.
export class PendingCloses {
  private pending: Map<string, Pending> = new Map();
  // By next attempt. An entry is pushed again when its attempt is rescheduled, and a copy that no longer
  // matches is skipped when popped.
  private heap: Array<{ at: number; pending: Pending }> = [];
  private inFlight = 0;
  private timer?: NodeJS.Timeout;
  private timerAt = Infinity;
  private stopped = false;

  constructor(
    private handlers: CloseHandlers,
    private tombstones: Tombstones,
    private store: CloseStore = closeStore
  ) {}

  get size() {
    return this.pending.size;
  }

  blocked(key: string) {
    return this.pending.has(key);
  }

  async load() {
    const now = Date.now();
    for (const closing of await this.store.load()) {
      const key = `${closing.endpoint} ${closing.connectionId}`;
      if (this.pending.has(key)) continue;
      const pending = { ...closing, attempts: 0, nextAt: now, inFlight: false };
      this.pending.set(key, pending);
      this.push(pending);
    }
    this.pump();
  }

  add(closings: Array<{ endpoint: string; connectionId: string }>) {
    const now = Date.now();
    const added: Pending[] = [];
    for (const { endpoint, connectionId } of closings) {
      const key = `${endpoint} ${connectionId}`;
      if (this.pending.has(key)) continue;
      const pending = { endpoint, connectionId, firstAttemptAt: now, attempts: 0, nextAt: now, inFlight: false };
      this.pending.set(key, pending);
      this.push(pending);
      added.push(pending);
    }
    if (added.length === 0) return;
    this.store.save(added).catch(error => console.error(`failed to record the close of ${added.length} connections: ${error}`));
    this.pump();
  }

  // Stores closes that are still to be made, so a restart makes them if this run does not.
  record(closings: Array<{ endpoint: string; connectionId: string }>) {
    const firstAttemptAt = Date.now();
    this.store
      .save(closings.map(({ endpoint, connectionId }) => ({ endpoint, connectionId, firstAttemptAt })))
      .catch(error => console.error(`failed to record the close of ${closings.length} connections: ${error}`));
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.timer);
    this.timer = undefined;
  }

  private push(pending: Pending) {
    const heap = this.heap;
    const entry = { at: pending.nextAt, pending };
    let i = heap.push(entry) - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[parent].at <= entry.at) break;
      heap[i] = heap[parent];
      i = parent;
    }
    heap[i] = entry;
  }

  private pop() {
    const heap = this.heap;
    const top = heap[0];
    const last = heap.pop()!;
    if (heap.length > 0) {
      let i = 0;
      for (;;) {
        let child = 2 * i + 1;
        if (child >= heap.length) break;
        if (child + 1 < heap.length && heap[child + 1].at < heap[child].at) child++;
        if (heap[child].at >= last.at) break;
        heap[i] = heap[child];
        i = child;
      }
      heap[i] = last;
    }
    return top;
  }

  // Starts the due closes while fewer than MAX_IN_FLIGHT are in flight; a finishing attempt pumps again.
  private pump() {
    if (this.stopped) return;
    const now = Date.now();
    while (this.heap.length > 0 && this.heap[0].at <= now && this.inFlight < MAX_IN_FLIGHT) {
      const { at, pending } = this.pop();
      const key = `${pending.endpoint} ${pending.connectionId}`;
      if (this.pending.get(key) !== pending || pending.inFlight || pending.nextAt !== at) continue;
      if (now >= pending.firstAttemptAt + CLOSE_TTL_MS) this.finish(pending);
      else this.attempt(pending);
    }
    const next = this.heap[0]?.at;
    if (next === undefined || next >= this.timerAt || (next <= now && this.inFlight >= MAX_IN_FLIGHT)) return;
    clearTimeout(this.timer);
    this.timerAt = next;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.timerAt = Infinity;
      this.pump();
    }, Math.max(0, next - now));
    this.timer.unref();
  }

  private attempt(pending: Pending) {
    pending.inFlight = true;
    this.inFlight++;
    this.handlers
      .delete(pending.connectionId, pending.endpoint)
      .then(
        () => 'closed' as const,
        error => {
          if ((error as { name?: string }).name === 'ExpiredTokenException' || String(error).startsWith('ExpiredTokenException')) {
            this.handlers.expired?.(error);
            return 'retry' as const;
          }
          const status = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
          if (status === 410) return 'closed' as const;
          if (status === 403) this.handlers.forbidden?.(pending.endpoint, pending.connectionId);
          else if (status !== undefined && status >= 400 && status < 500 && status !== 429) {
            console.error(`the gateway refused to close connection ${pending.connectionId}, keeping it blocked: ${error}`);
            return 'refused' as const;
          }
          if (pending.attempts === 0) console.warn(`failed to close connection ${pending.connectionId}, retrying: ${error}`);
          return 'retry' as const;
        }
      )
      .then(outcome => {
        pending.inFlight = false;
        this.inFlight--;
        if (this.pending.get(`${pending.endpoint} ${pending.connectionId}`) !== pending) return this.pump();
        const expiresAt = pending.firstAttemptAt + CLOSE_TTL_MS;
        if (outcome === 'closed') this.finish(pending, true);
        else {
          const delay = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** pending.attempts++);
          pending.nextAt = outcome === 'refused' ? expiresAt : Math.min(expiresAt, Date.now() + delay * (0.75 + Math.random() * 0.5));
          this.push(pending);
        }
        this.pump();
      });
  }

  // A confirmed close keeps its record until its tombstone is stored, so a restart before then sends it again.
  private finish(pending: Pending, confirmed = false) {
    const { endpoint, connectionId, firstAttemptAt } = pending;
    this.pending.delete(`${endpoint} ${connectionId}`);
    const stored = confirmed ? this.tombstones.add(endpoint, connectionId, 'closed', firstAttemptAt + CLOSE_TTL_MS) : Promise.resolve(true);
    void stored
      .then(ok => ok && this.store.remove(pending))
      .catch(error => console.error(`failed to clear the close of connection ${connectionId}: ${error}`));
    this.handlers.closed(endpoint, connectionId);
  }
}
