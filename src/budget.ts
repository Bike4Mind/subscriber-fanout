const MB = 1024 * 1024;
export const BUDGET_FLOOR = 16 * MB;
export const BUDGET_CEILING = 256 * MB;
// C, the line the read gate admits events under, as a multiple of B.
const OVERSHOOT = 1.25;

export type HeapUse = {
  heapLimit: number;
  // Used heap after a GC once the startup load finished, and the subscription count then.
  base: number;
  startCount: number;
  perSubscription: number;
  count: number;
};

// A third of the heap left after what the subscriptions hold; the other two thirds cover in-flight frames,
// overshoot, driver replies and GC headroom. free is that third before the floor and ceiling apply.
export function sizeBudget({ heapLimit, base, startCount, perSubscription, count }: HeapUse) {
  const used = base + perSubscription * (count - startCount);
  const free = (heapLimit - used) / 3;
  return { limit: Math.max(BUDGET_FLOOR, Math.min(BUDGET_CEILING, free)), free, used };
}

// How long a pause lasts, about: each of k connections must receive the D bytes the resume waits for, c/m bytes
// per POST at L each, with at most `slots` connections served at once.
export function pauseEstimate(p: { stored: number; limit: number; charge: number; posts: number; medianMs: number; connections: number; slots: number }) {
  const drain = p.stored - p.limit + Math.max(p.limit / 8, p.charge);
  const perPost = Math.max(1, p.charge / p.posts);
  const ms = (drain / perPost) * p.medianMs * Math.max(1, p.connections / p.slots);
  return { ms, drain, perPost };
}

function addTo(totals: Map<object, number>, key: object, bytes: number) {
  const total = (totals.get(key) ?? 0) + bytes;
  if (total > 0) totals.set(key, total);
  else totals.delete(key);
}

// What deliveries hold against the budget B: E, the heap (events, looked-up documents, per-delivery and per-queue
// overhead), and X, the built message bodies. Also the bytes of POST frames in flight, against B/4, and of
// released requests. An event is charged once while anything holds it.
export class Budget {
  heap = 0;
  // Called whenever the heap falls, with the stream it was charged to, if any.
  onFall?: (stream?: object) => void;
  bodies = 0;
  frames = 0;
  released = 0;
  private events: Map<object, { bytes: number; holds: number; stream?: object; shared?: object; onFree?: () => void }> = new Map();
  private byStream: Map<object, number> = new Map();
  // O_s: per stream, the per-delivery overhead its matches charged and not yet released.
  private overheadByStream: Map<object, number> = new Map();

  constructor(public limit: number) {}

  get stored() {
    return this.heap + this.bodies;
  }

  get frameLimit() {
    return this.limit / 4;
  }

  get ceiling() {
    return this.limit * OVERSHOOT;
  }

  hold(event: object, bytes: number, stream?: object) {
    const held = this.events.get(event);
    if (held) {
      held.holds++;
      return;
    }
    this.events.set(event, { bytes, holds: 1, stream });
    this.add(bytes, stream);
  }

  // Returns the bytes freed, which is the event's size once its last holder lets go.
  release(event: object): number {
    const held = this.events.get(event);
    if (!held || --held.holds > 0) return 0;
    this.events.delete(event);
    this.add(-held.bytes, held.stream);
    held.onFree?.();
    return held.bytes + (held.shared ? this.release(held.shared) : 0);
  }

  // Charges an object that several held events share once, against the first one's stream, until the last of
  // them is released. False if the event is not held.
  attach(event: object, shared: object, bytes: number, onFree?: () => void) {
    const held = this.events.get(event);
    if (!held || (held.shared && held.shared !== shared)) return false;
    if (held.shared) return true;
    held.shared = shared;
    const entry = this.events.get(shared);
    if (entry) entry.holds++;
    else {
      this.events.set(shared, { bytes, holds: 1, stream: held.stream, onFree });
      this.add(bytes, held.stream);
    }
    return true;
  }

  sharedOf(event: object) {
    return this.events.get(event)?.shared;
  }

  freedBy(event: object) {
    const shared = this.sharedOf(event);
    return this.sizeOf(event) + (shared && this.holds(shared) === 1 ? this.sizeOf(shared) : 0);
  }

  holds(event: object) {
    return this.events.get(event)?.holds ?? 0;
  }

  sizeOf(event: object) {
    return this.events.get(event)?.bytes ?? 0;
  }

  add(bytes: number, stream?: object) {
    this.heap += bytes;
    if (stream) addTo(this.byStream, stream, bytes);
    if (bytes < 0) this.onFall?.(stream);
  }

  addOverhead(bytes: number, stream?: object) {
    if (stream) addTo(this.overheadByStream, stream, bytes);
    this.add(bytes, stream);
  }

  streamBytes(stream: object) {
    return this.byStream.get(stream) ?? 0;
  }

  overheadOf(stream: object) {
    return this.overheadByStream.get(stream) ?? 0;
  }

  // The stream holding the most heap, if it holds at least max(limit/8, the largest event it read): pausing one
  // that holds less frees less than its resume line asks for.
  heaviest<S extends object>(streams: S[], largestEvent: (stream: S) => number) {
    let heaviest: S | undefined;
    for (const stream of streams) {
      const bytes = this.streamBytes(stream);
      if (bytes < Math.max(this.limit / 8, largestEvent(stream))) continue;
      if (!heaviest || bytes > this.streamBytes(heaviest)) heaviest = stream;
    }
    return heaviest;
  }
}
