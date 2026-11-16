import { Budget } from './budget.js';

export type Body = { tail: Buffer; refs: number };

type Waiter = { waiter: object; since: number; json: number; wake: () => void };

// Message bodies, one per owner (an event, or a document looked up for every stream that read the change) and
// projection, kept while a delivery of it is unsettled. Over B, builders wait their turn for X + json <= B.
export class Bodies {
  private bodies: Map<object, Map<string, Body>> = new Map();
  private waiters: Waiter[] = [];

  constructor(private budget: Budget) {}

  // The body, built by make if the gate lets it; 'wait' if the builder must wait, with wake called once it may
  // try again; undefined if there is nothing to send, as when the projection threw.
  take(owner: object, key: string, json: number, waiter: object, wake: () => void, make: () => Buffer | undefined): Body | 'wait' | undefined {
    const built = this.bodies.get(owner)?.get(key);
    if (built) {
      built.refs++;
      return built;
    }
    if (!this.mayBuild(waiter, json, wake)) return 'wait';
    const tail = make();
    let body: Body | undefined;
    if (tail) {
      body = { tail, refs: 1 };
      let byKey = this.bodies.get(owner);
      if (!byKey) this.bodies.set(owner, (byKey = new Map()));
      byKey.set(key, body);
      this.budget.bodies += tail.length;
    }
    this.wake();
    return body;
  }

  release(owner: object, key: string, body: Body) {
    if (--body.refs > 0) return;
    const byKey = this.bodies.get(owner);
    if (byKey?.get(key) === body) byKey.delete(key);
    if (byKey?.size === 0) this.bodies.delete(owner);
    this.budget.bodies -= body.tail.length;
  }

  has(owner: object) {
    return this.bodies.has(owner);
  }

  waitMs(now = Date.now()) {
    return this.waiters.length > 0 ? now - this.waiters[0].since : 0;
  }

  // Takes a builder out of line once the try that queued it has ended, and lets the next one go.
  cancel(waiter: object) {
    const at = this.waiters.findIndex(w => w.waiter === waiter);
    if (at < 0) return;
    this.waiters.splice(at, 1);
    this.wake();
  }

  // Lets waiting builders try again once their turn has room: every one of them under B, the first over it.
  wake() {
    if (this.waiters.length === 0) return;
    if (this.budget.stored <= this.budget.limit) {
      for (const w of this.waiters) w.wake();
    } else if (this.budget.bodies + this.waiters[0].json <= this.budget.limit) this.waiters[0].wake();
  }

  private mayBuild(waiter: object, json: number, wake: () => void) {
    const at = this.waiters.findIndex(w => w.waiter === waiter);
    const first = this.waiters.length === 0 || at === 0;
    if (this.budget.stored <= this.budget.limit || (first && this.budget.bodies + json <= this.budget.limit)) {
      if (at >= 0) this.waiters.splice(at, 1);
      return true;
    }
    if (at < 0) this.waiters.push({ waiter, since: Date.now(), json, wake });
    else Object.assign(this.waiters[at], { json, wake });
    return false;
  }
}
