import { Budget } from './budget.js';

// Resolved in place of a send that was discarded because its connection was closed.
export const DROPPED = Symbol('dropped');

export type Target = { endpoint: string; connectionId: string };

// One send of a message to one connection: a POST per frame, retried from the frame it stopped at.
export type Message = {
  posts: number;
  frameBytes: (index: number) => number;
  // probation is true for a new or slow connection's POST. connected is called once the request has a
  // connected socket, which starts the request clock.
  post: (index: number, signal: AbortSignal, probation: boolean, connected: () => void) => Promise<unknown>;
};

// One message to several connections.
export type Job = {
  // Jobs built from the same event share its charge; a job without one is its own.
  event?: object;
  bytes: number;
  stream?: object;
  targets: readonly Target[];
  // Targets not yet settled.
  pending: number;
  // POSTs one send is expected to take, for the drain estimate.
  posts: number;
  ping?: boolean;
};

export type Closed = { endpoint: string; connectionId: string; reason: string; detail?: string };

export type Handlers<J extends Job> = {
  // The message for one target, or undefined when there is nothing to send.
  open: (job: J, index: number) => Message | undefined;
  // Once per target, with DROPPED as the value when its connection was closed first.
  settle: (job: J, index: number, result: PromiseSettledResult<unknown>) => void;
  // Connections the scheduler closed, together; their queues are already dropped.
  close: (closed: Closed[]) => void;
  // A connection the gateway reported gone (410).
  gone: (endpoint: string, connectionId: string) => void;
  // Bytes a job frees beyond its event and overhead when it settles: its body, if it is the last holder.
  extra: (job: J) => number;
  // Runs on every check, after the drain closes.
  check: () => void;
  // How long the first message body still waiting for room has waited.
  roomWaitMs: () => number;
  // A connection being closed or removed, by its `endpoint connectionId` key.
  blocked: (key: string) => boolean;
  // Sends tried once and settled whatever the outcome, as a ping is.
  once: (job: J, index: number) => boolean;
  // The gateway credentials expired.
  expired: (error: unknown) => void;
};

export type SchedulerLimits = {
  slots: number;
  // From the connected socket to the answer.
  requestTimeoutMs: number;
  maxDrainMs: number;
  probationSlots?: number;
};

export type Failure = 'expired' | 'gone' | 'forbidden' | 'gateway' | 'reset' | 'rejected' | 'timeout';

export function classifyFailure(error: unknown): Failure {
  const { name, message, code, $metadata } = (error ?? {}) as {
    name?: string;
    message?: string;
    code?: string;
    $metadata?: { httpStatusCode?: number };
  };
  if (name === 'ExpiredTokenException' || String(error).startsWith('ExpiredTokenException')) return 'expired';
  const status = $metadata?.httpStatusCode;
  if (status === 410) return 'gone';
  if (status === 403) return 'forbidden';
  if (status === 429 || (status !== undefined && status >= 500)) return 'gateway';
  if (status !== undefined) return 'rejected';
  // The SDK renames ECONNRESET, EPIPE and ETIMEDOUT socket errors TimeoutError, so the code decides first.
  if (code === 'ECONNRESET' || code === 'EPIPE') return 'reset';
  if (code !== undefined) return 'gateway';
  if (name === 'TimeoutError' && !message?.includes('did not establish a connection')) return 'timeout';
  // Network, DNS, TLS and credential errors: nothing this connection did.
  return 'gateway';
}

// Heap held beyond the event, measured through the delivery path: per job, per send it queues, and per
// connection with sends queued.
export const JOB_BYTES = 150;
export const ENTRY_BYTES = 56;
export const QUEUE_BYTES = 320;

const PROBATION_SLOTS = 16;
// Slots the slow ring leaves free beyond its share, so a new connection's first POST never waits behind slow
// ones; also the fewest slots the predicted healthy rotation assumes healthy connections hold.
const SLOT_RESERVE = 8;
// Slots the slow ring may hold while it has an entry waiting, however few are free.
const SLOW_SHARE = 4;
const SLOW_MIN_MS = 1000;
const ALPHA = 0.2;
const SMALL_FRAME_BYTES = 32 * 1024;
const FLOOR_MIN_MS = 250;
const FLOOR_MAX_MS = 4000;
// Completed attempts per connection and frame size class that its own threshold is taken from.
const P99_RING = 16;
// The healthy rotation the abort cap holds to; the cap rises only once the smoothed rotation is under 70% of it.
const ROTATION_TARGET_MS = 3500;
const WINDOW_MS = 60 * 1000;
// Ref counts connections whose latest POST completed within this, or twice the healthy rotation if longer.
const REF_WINDOW_MIN_MS = 10 * 1000;
const MAX_SAMPLES = 8192;
const STATS_TTL_MS = 2 * 60 * 1000;
const CONNECT_TIMEOUT_MS = 3000;
// A whole message cut past the release cap is resent after this, so a late copy the gateway still delivers
// lands before the next message.
const ABORT_HOLD_MS = 1000;
// Cut whole messages keep running outside the slots, up to this many per slot and these bytes in all.
export const RELEASES_PER_SLOT = 20;
const RELEASE_BYTES = 32 * 1024 * 1024;
const FRAME_BLOCK_MS = 1000;
// A suspect attempt unanswered this long after its socket connected, or twice Ref_3s if longer, counts as hung.
const HUNG_MIN_MS = 2000;
const REF3_WINDOW_MS = 3000;
// A budget close takes a connection slow by L for this long, or at once while a body has waited STALL_MS for room.
const SLOW_SUSTAIN_MS = 10 * 1000;
const STALL_MS = 1000;
// The client drops a partial message whose next chunk is over 30s late.
const MID_MESSAGE_MS = 10 * 1000;
// A partly sent message whose next chunk would start past this after the previous one landed closes its
// connection: that attempt can run to the 5s request timeout, and the client drops the message at 30s.
const CHUNK_GAP_MS = 20 * 1000;
const CHUNK_LATE = `the next chunk of a partly sent message would go out over ${CHUNK_GAP_MS / 1000}s after the one before it`;
// One slow POST times a deep queue overstates the drain, as on a cold socket or credential fetch, so a
// connection is closed only once its drain has stayed over the limit this long.
const DRAIN_GRACE_MS = 10 * 1000;
// A connection whose head message has spent this long connecting, in flight or held for a retry is closed.
const OWN_TIME_MS = 60 * 1000;
// Holds after each gateway fault in a row; the last repeats.
const HOLDS_MS = [100, 400, 1000, 2000, 4000, 5000];
// A 403 no other connection on the endpoint got in this window is a strike; this many in a row close.
const SHARED_403_MS = 1000;
const STRIKES = 3;
const CHECK_MS = 250;
const TURN_WARN_MS = 5000;
const TURN_LOG_MS = 60 * 1000;
const NOTE_MS = 10 * 1000;

const CUT = Symbol('cut');
const dropped: PromiseSettledResult<unknown> = { status: 'fulfilled', value: DROPPED };
const sent: PromiseSettledResult<unknown> = { status: 'fulfilled', value: undefined };

type Class = 'healthy' | 'probation';

type Task<J> = { job: J; index: number };

type Current<J> = {
  task: Task<J>;
  message: Message;
  next: number;
  // In-flight time of the current POST over its cut attempts.
  u: number;
  // Time spent connecting, in flight or held, over every POST of the message.
  own: number;
  // Gateway faults since the last POST that landed.
  faults: number;
  // performance.now() when the message was opened or its latest POST landed, so a wall clock step cannot move it.
  lastProgressAt: number;
  once: boolean;
};

type Flight = {
  startedAt: number;
  connectedAt?: number;
  controller: AbortController;
  timer?: NodeJS.Timeout;
  abortTimer?: NodeJS.Timeout;
  bytes: number;
  cls: Class;
  // Cut once this long after the socket connected.
  T: number;
  slow: boolean;
  suspect: boolean;
  // Cut whole and left running without a slot.
  released?: boolean;
  cut?: 'abort' | 'connect' | 'timeout' | 'close';
};

type Conn<J> = {
  key: string;
  // Whether any POST has started, which orders new connections ahead of retried ones.
  tried?: boolean;
  endpoint: string;
  connectionId: string;
  tasks: Task<J>[];
  head: number;
  queuedPosts: number;
  current?: Current<J>;
  flight?: Flight;
  state: 'idle' | 'ready' | 'frame' | 'flight' | 'released' | 'held';
  // Invalidates ring entries pushed before the last change of state.
  token: number;
  front: boolean;
  slow: boolean;
  // Slow only through the time its current POST has been in flight.
  suspect?: boolean;
  closed: boolean;
  holdUntil: number;
  heldSince?: number;
  // Held for a gateway fault, which takes a retry token at the end of the hold.
  needsToken?: boolean;
  // 403s in a row no other connection on the endpoint shared.
  strikes: number;
  readySince?: number;
  frameSince?: number;
  timer?: NodeJS.Timeout;
  L?: number;
  // The latest completed POST's duration.
  lastMs?: number;
  // The last P99_RING attempt durations in each frame size class, and how many were recorded in each.
  durations?: Uint16Array;
  recorded?: [number, number];
  completedAt?: number;
  drainOverSince?: number;
  slowSince?: number;
};

type Entry<J> = { c: Conn<J>; token: number };

// A FIFO that also takes entries at its front.
class Ring<T> {
  private items: Array<T | undefined> = new Array(16);
  private head = 0;
  size = 0;

  push(item: T) {
    this.grow();
    this.items[(this.head + this.size++) & (this.items.length - 1)] = item;
  }

  unshift(item: T) {
    this.grow();
    this.head = (this.head - 1) & (this.items.length - 1);
    this.items[this.head] = item;
    this.size++;
  }

  shift() {
    if (this.size === 0) return undefined;
    const item = this.items[this.head];
    this.items[this.head] = undefined;
    this.head = (this.head + 1) & (this.items.length - 1);
    this.size--;
    return item;
  }

  peek() {
    return this.size === 0 ? undefined : this.items[this.head];
  }

  at(index: number) {
    return this.items[(this.head + index) & (this.items.length - 1)]!;
  }

  removeAt(index: number) {
    const mask = this.items.length - 1;
    for (let i = index; i < this.size - 1; i++) this.items[(this.head + i) & mask] = this.items[(this.head + i + 1) & mask];
    this.items[(this.head + --this.size) & mask] = undefined;
  }

  private grow() {
    if (this.size < this.items.length) return;
    const items: Array<T | undefined> = new Array(this.items.length * 2);
    for (let i = 0; i < this.size; i++) items[i] = this.items[(this.head + i) & (this.items.length - 1)];
    this.items = items;
    this.head = 0;
  }
}

function percentile(values: number[], p: number) {
  values.sort((a, b) => a - b);
  return values[Math.min(values.length - 1, Math.floor(values.length * p))];
}

// Sends each connection's messages in order, one POST in flight per connection, within S slots shared by
// a ring of healthy connections and a probation ring of new and slow ones. See DESIGN.md.
export class Scheduler<J extends Job> {
  private conns: Map<string, Conn<J>> = new Map();
  // Connections with work queued or in flight.
  private active: Set<Conn<J>> = new Set();
  private healthy = new Ring<Entry<J>>();
  private untried = new Ring<Entry<J>>();
  private fresh = new Ring<Entry<J>>();
  private slow = new Ring<Entry<J>>();
  private frameWaits: Record<Class, Ring<Entry<J>>> = { healthy: new Ring(), probation: new Ring() };
  private inFlight = 0;
  private healthyInFlight = 0;
  private probationInFlight = 0;
  private slowInFlight = 0;
  private releases = 0;
  // A connection completed a POST in the last WINDOW_MS and is not slow.
  private healthyExists = false;
  private probationFrames = 0;
  private samples: Array<{ at: number; ms: number; large: boolean }> = [];
  private samplesHead = 0;
  private ref = 0;
  // Abort thresholds for connections with no attempt of their own in each frame size class.
  private global = [FLOOR_MIN_MS, FLOOR_MIN_MS];
  // The healthy rotation, smoothed, and the abort cap on healthy attempts it sets.
  private rotation?: number;
  private cap = Infinity;
  // Over the last second: the longest healthy wait for a turn, healthy slot holds, healthy attempt durations
  // and the largest uncapped healthy threshold.
  private periodWait = 0;
  private periodHoldSum = 0;
  private periodHolds = 0;
  private periodDurations: number[] = [];
  private periodMaxT = 0;
  // Per endpoint, attempts that completed and started in the last REF3_WINDOW_MS, and the Ref_3s they give.
  private recent: Map<string, Array<{ start: number; ms: number }>> = new Map();
  private ref3: Map<string, number> = new Map();
  // Per endpoint, the latest 403s of the two connections that got one last.
  private denials: Map<string, Array<{ key: string; at: number }>> = new Map();
  private statsAt = 0;
  private retryTokens: number;
  private retryTokensAt = Date.now();
  private tokenWaiters = new Ring<Entry<J>>();
  private tokenTimer?: NodeJS.Timeout;
  private turnLoggedAt = 0;
  private notes: Map<string, { count: number }> = new Map();
  private timer?: NodeJS.Timeout;
  private victims: Array<Conn<J>> = [];
  private victimsAt = -Infinity;
  private closing: Closed[] = [];
  private batching = false;
  private stopped = false;
  private ended = false;
  private pumping = false;
  private repump = false;
  private readonly probationSlots: number;

  constructor(
    private budget: Budget,
    private limits: SchedulerLimits,
    private handlers: Handlers<J>
  ) {
    this.probationSlots = limits.probationSlots ?? PROBATION_SLOTS;
    this.retryTokens = 2 * limits.slots;
  }

  add(job: J) {
    const now = Date.now();
    job.pending = job.targets.length;
    this.budget.hold(job.event ?? job, job.bytes, job.stream);
    this.budget.addOverhead(JOB_BYTES + ENTRY_BYTES * job.targets.length, job.stream);
    let blocked: number[] | undefined;
    for (let index = 0; index < job.targets.length; index++) {
      const { endpoint, connectionId } = job.targets[index];
      const key = `${endpoint} ${connectionId}`;
      if (this.handlers.blocked(key)) {
        (blocked ??= []).push(index);
        continue;
      }
      const c = this.connection(key, endpoint, connectionId);
      if (!this.active.has(c)) {
        this.active.add(c);
        this.budget.add(QUEUE_BYTES);
      }
      c.tasks.push({ job, index });
      if (!job.ping) c.queuedPosts += job.posts;
      if (c.state === 'idle') this.ready(c, now);
    }
    for (const index of blocked ?? []) this.settleTask({ job, index }, dropped);
    this.watch();
    this.pump();
  }

  medianMs() {
    const recent = this.samples.slice(this.samplesHead).filter(s => Date.now() - s.at <= WINDOW_MS);
    if (recent.length === 0) return undefined;
    return recent.map(s => s.ms).sort((a, b) => a - b)[recent.length >> 1];
  }

  get healthySlots() {
    return Math.max(8, this.limits.slots - this.probationSlots);
  }

  close(endpoint: string, connectionId: string) {
    const c = this.conns.get(`${endpoint} ${connectionId}`);
    if (c) this.closeConnection(c);
  }

  busy(endpoint: string, connectionId: string) {
    const c = this.conns.get(`${endpoint} ${connectionId}`);
    return c !== undefined && this.active.has(c);
  }

  // A 403 to another request to the endpoint, such as a DELETE, which keeps a connection's own 403 from
  // counting against it.
  forbidden(endpoint: string, connectionId: string) {
    this.noteDenial(endpoint, `${endpoint} ${connectionId}`, Date.now());
  }

  // Retry tokens are still granted after stop(), so held sends go out during the shutdown drain, until end().
  stop() {
    this.stopped = true;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  end() {
    this.ended = true;
    clearTimeout(this.tokenTimer);
    this.tokenTimer = undefined;
  }

  // While over limit, closes connections slow by L for SLOW_SUSTAIN_MS whose close alone frees a real share of
  // the excess, the one that frees most first; if none does, all of them together when that would. own() walks
  // a connection's queue and this runs on every read, so the ranking is reused for CHECK_MS.
  closeSlow(limit: number) {
    if (this.budget.stored <= limit) return;
    const now = Date.now();
    if (now - this.victimsAt >= CHECK_MS) {
      const minFree = Math.max(limit / 1024, (this.budget.stored - limit) / 64);
      // Room is shared, so a stall on any stream waives the wait.
      const stalled = this.handlers.roomWaitMs() >= STALL_MS;
      const eligible: Conn<J>[] = [];
      for (const c of this.active) {
        if (!this.slowByL(c)) continue;
        if (stalled || (c.slowSince !== undefined && now - c.slowSince >= SLOW_SUSTAIN_MS)) eligible.push(c);
      }
      const ranked: Array<[number, Conn<J>]> = [];
      for (const c of eligible) {
        const own = this.own(c);
        if (own >= minFree) ranked.push([own, c]);
      }
      ranked.sort((a, b) => a[0] - b[0]);
      this.victims = ranked.map(([, c]) => c);
      if (ranked.length === 0 && eligible.length > 1 && this.groupOwn(eligible) >= minFree) this.victims = eligible;
      this.victimsAt = now;
    }
    this.batch(() => {
      while (this.budget.stored > limit && this.victims.length > 0) {
        const c = this.victims.pop()!;
        if (c.closed || !this.active.has(c) || !this.slowByL(c)) continue;
        this.closeConnection(c, `it is slow and the queues are over ${limit} bytes`);
      }
    });
  }

  // The bytes closing this connection alone would free now.
  own(c: Conn<J>) {
    let bytes = QUEUE_BYTES;
    const count = ({ job }: Task<J>) => {
      bytes += ENTRY_BYTES;
      if (job.pending !== 1) return;
      bytes += JOB_BYTES + this.handlers.extra(job);
      const event = job.event ?? job;
      if (this.budget.holds(event) === 1) bytes += this.budget.freedBy(event);
    };
    if (c.current) count(c.current.task);
    for (let i = c.head; i < c.tasks.length; i++) count(c.tasks[i]);
    return bytes;
  }

  // The bytes closing all of these connections together would free now: a job when all its unsettled targets
  // are among them, its event when no job outside them holds it.
  private groupOwn(group: Conn<J>[]) {
    let bytes = 0;
    const counts: Map<J, number> = new Map();
    const visit = ({ job }: Task<J>) => {
      bytes += ENTRY_BYTES;
      counts.set(job, (counts.get(job) ?? 0) + 1);
    };
    for (const c of group) {
      bytes += QUEUE_BYTES;
      if (c.current) visit(c.current.task);
      for (let i = c.head; i < c.tasks.length; i++) visit(c.tasks[i]);
    }
    const events: Map<object, number> = new Map();
    for (const [job, n] of counts) {
      if (n !== job.pending) continue;
      bytes += JOB_BYTES + this.handlers.extra(job);
      const event = job.event ?? job;
      events.set(event, (events.get(event) ?? 0) + 1);
    }
    const shares: Map<object, number> = new Map();
    for (const [event, jobs] of events) {
      if (this.budget.holds(event) !== jobs) continue;
      bytes += this.budget.sizeOf(event);
      const shared = this.budget.sharedOf(event);
      if (shared) shares.set(shared, (shares.get(shared) ?? 0) + 1);
    }
    for (const [shared, holders] of shares) if (this.budget.holds(shared) === holders) bytes += this.budget.sizeOf(shared);
    return bytes;
  }

  // The time to send what this connection alone still holds at its current latency; everything it holds, if it
  // is slow by L.
  drainMs(c: Conn<J>, now: number) {
    const byL = this.slowByL(c);
    const latency = !byL && this.isSlow(c, now) ? (c.L ?? 0) : this.latency(c, now);
    if (latency === 0) return 0;
    const job = c.current?.task.job;
    let posts = job && (byL || job.pending === 1) && !job.ping ? c.current!.message.posts - c.current!.next : 0;
    if (byL) return (posts + c.queuedPosts) * latency;
    if ((posts + c.queuedPosts) * latency <= this.limits.maxDrainMs) return (posts + c.queuedPosts) * latency;
    for (let i = c.head; i < c.tasks.length; i++) {
      const { job } = c.tasks[i];
      if (job.pending === 1 && !job.ping) posts += job.posts;
    }
    return posts * latency;
  }

  private connection(key: string, endpoint: string, connectionId: string) {
    let c = this.conns.get(key);
    if (!c) {
      c = {
        key,
        endpoint,
        connectionId,
        tasks: [],
        head: 0,
        queuedPosts: 0,
        state: 'idle',
        token: 0,
        front: false,
        slow: false,
        closed: false,
        holdUntil: 0,
        strikes: 0,
      };
      this.conns.set(key, c);
    }
    return c;
  }

  // L*: the larger of the connection's latency and the time its current POST has been in flight. After a
  // slowdown L lags for several POSTs, so a latest POST under the slow line bounds it.
  private latency(c: Conn<J>, now: number) {
    const connectedAt = c.flight?.connectedAt;
    const u = (c.current?.u ?? 0) + (connectedAt !== undefined ? now - connectedAt : 0);
    const L = c.L === undefined ? 0 : c.lastMs! > this.slowLine() ? c.L : Math.min(c.L, c.lastMs!);
    return Math.max(L, u);
  }

  private slowLine() {
    return Math.max(SLOW_MIN_MS, 2 * this.ref);
  }

  private isSlow(c: Conn<J>, now: number) {
    return this.latency(c, now) > this.slowLine();
  }

  private slowByL(c: Conn<J>) {
    const line = this.slowLine();
    return (c.L ?? 0) > line && (c.lastMs ?? 0) > line;
  }

  // The head message's time connecting, in flight, released or held; time ready in a ring or waiting for frame
  // room does not count.
  private ownMs(c: Conn<J>, now: number) {
    if (!c.current) return 0;
    return c.current.own + (c.flight ? now - c.flight.startedAt : 0) + (c.heldSince !== undefined ? now - c.heldSince : 0);
  }

  private chunkLate(current: Current<J> | undefined, inMs = 0) {
    return current !== undefined && current.next > 0 && performance.now() + inMs - current.lastProgressAt > CHUNK_GAP_MS;
  }

  private ready(c: Conn<J>, now: number, front = false) {
    if (c.closed) return;
    if (!c.current && c.head >= c.tasks.length) return this.retire(c);
    c.token++;
    if (c.holdUntil > now || c.needsToken) {
      if (c.heldSince === undefined) c.heldSince = now;
      c.state = 'held';
      if (c.holdUntil > now) this.holdFor(c, c.holdUntil - now);
      else this.waitToken(c, now);
      return;
    }
    c.slow = this.isSlow(c, now);
    c.suspect = c.slow && !this.slowByL(c);
    const ring = this.ringOf(c);
    const aged = c.current !== undefined && c.current.next > 0 && performance.now() - c.current.lastProgressAt >= MID_MESSAGE_MS;
    c.state = 'ready';
    c.front = front || aged;
    c.readySince ??= now;
    if (c.front) ring.unshift({ c, token: c.token });
    else ring.push({ c, token: c.token });
  }

  private holdFor(c: Conn<J>, ms: number) {
    clearTimeout(c.timer);
    c.timer = setTimeout(() => {
      c.timer = undefined;
      if (c.state !== 'held' || c.closed) return;
      const now = Date.now();
      if (c.holdUntil > now) return this.holdFor(c, c.holdUntil - now);
      if (c.needsToken) this.waitToken(c, now);
      else this.resume(c, now);
      this.pump();
    }, ms);
    c.timer.unref();
  }

  private resume(c: Conn<J>, now: number) {
    if (c.current && c.heldSince !== undefined) c.current.own += now - c.heldSince;
    c.heldSince = undefined;
    this.ready(c, now);
  }

  // Retry tokens go to the connections waiting for one in the order they asked, as many as each refill adds.
  private waitToken(c: Conn<J>, now: number) {
    if (!this.firstTokenWaiter() && this.takeRetry(now)) {
      c.needsToken = false;
      return this.resume(c, now);
    }
    this.tokenWaiters.push({ c, token: c.token });
    this.armTokens(now);
  }

  private grantTokens(now: number) {
    this.refill(now);
    for (let entry = this.firstTokenWaiter(); entry && this.retryTokens >= 1; entry = this.firstTokenWaiter()) {
      this.tokenWaiters.shift();
      this.retryTokens--;
      entry.c.needsToken = false;
      this.resume(entry.c, now);
    }
    this.armTokens(now);
  }

  private armTokens(now: number) {
    if (this.ended || this.tokenTimer || !this.firstTokenWaiter()) return;
    this.refill(now);
    this.tokenTimer = setTimeout(() => {
      this.tokenTimer = undefined;
      this.grantTokens(Date.now());
      this.pump();
    }, this.tokenWaitMs());
    this.tokenTimer.unref();
  }

  private firstTokenWaiter() {
    for (let entry = this.tokenWaiters.peek(); entry; entry = this.tokenWaiters.peek()) {
      if (entry.token === entry.c.token && entry.c.state === 'held' && entry.c.needsToken) return entry;
      this.tokenWaiters.shift();
    }
    return undefined;
  }

  private ringOf(c: Conn<J>) {
    return c.slow ? this.slow : c.completedAt !== undefined ? this.healthy : c.tried ? this.fresh : this.untried;
  }

  // Moves slow-ring entries whose connection is no longer slow, as when the slow line rose, to the back of the
  // ring they now belong to, keeping their ready time.
  private recheckSlow(now: number) {
    const kept = new Ring<Entry<J>>();
    for (let entry = this.slow.shift(); entry; entry = this.slow.shift()) {
      if (entry.token !== entry.c.token || entry.c.state !== 'ready') continue;
      if (this.isSlow(entry.c, now)) kept.push(entry);
      else this.reclass(entry);
    }
    this.slow = kept;
  }

  private reclass(entry: Entry<J>) {
    entry.c.slow = false;
    entry.c.suspect = false;
    this.ringOf(entry.c).push(entry);
  }

  private retire(c: Conn<J>) {
    c.state = 'idle';
    c.token++;
    c.drainOverSince = undefined;
    c.slowSince = undefined;
    c.tasks = [];
    c.head = 0;
    c.queuedPosts = 0;
    c.readySince = undefined;
    if (this.active.delete(c)) this.budget.add(-QUEUE_BYTES);
    if (c.completedAt === undefined) this.conns.delete(c.key);
  }

  private pump() {
    if (this.pumping) {
      this.repump = true;
      return;
    }
    this.pumping = true;
    try {
      do {
        this.repump = false;
        const now = Date.now();
        while (this.inFlight < this.limits.slots && this.startNext(now));
      } while (this.repump);
    } finally {
      this.pumping = false;
    }
  }

  // The slow ring starts a POST only while more than SLOT_RESERVE slots are free, or while it holds fewer than
  // SLOW_SHARE; healthy POSTs leave it that share. While a healthy connection exists, new and retried connections
  // go past probation's slots only while more than SLOT_RESERVE are free.
  private startNext(now: number) {
    const free = this.limits.slots - this.inFlight;
    const healthyWaiting = this.hasEntry(this.healthy) || this.waitsForFrame('healthy');
    const slowWaiting = this.hasEntry(this.slow) || this.waitsForFrame('probation', true);
    const share = slowWaiting && this.slowInFlight < SLOW_SHARE;
    const underCap = this.probationInFlight < (healthyWaiting ? this.probationSlots : this.limits.slots);
    if (underCap || share) {
      const freshOk = underCap && (free > SLOT_RESERVE || !this.healthyExists || this.probationInFlight < this.probationSlots);
      const slowOk = (underCap && free > SLOT_RESERVE) || share;
      const c = this.take('probation', now, freshOk, slowOk);
      if (c) {
        this.begin(c, 'probation', now);
        return true;
      }
    }
    if (share && free <= SLOW_SHARE - this.slowInFlight) return false;
    const c = this.take('healthy', now);
    if (!c) return false;
    this.begin(c, 'healthy', now);
    return true;
  }

  private hasEntry(ring: Ring<Entry<J>>) {
    for (let entry = ring.peek(); entry; entry = ring.peek()) {
      if (entry.token === entry.c.token && entry.c.state === 'ready') return true;
      ring.shift();
    }
    return false;
  }

  // Valid frame waits of a class in order, dropping stale entries on the way.
  private *frameWaiters(cls: Class) {
    const waits = this.frameWaits[cls];
    for (let i = 0; i < waits.size; i++) {
      const entry = waits.at(i);
      if (entry.token !== entry.c.token || entry.c.state !== 'frame') waits.removeAt(i--);
      else yield { c: entry.c, index: i };
    }
  }

  private waitsForFrame(cls: Class, slowOnly = false) {
    for (const { c } of this.frameWaiters(cls)) if (!slowOnly || c.slow) return true;
    return false;
  }

  private take(cls: Class, now: number, freshOk = true, slowOk = true) {
    let first = true;
    for (const { c, index } of this.frameWaiters(cls)) {
      if (cls === 'probation' && !(c.slow ? slowOk : freshOk)) continue;
      if (this.fits(cls, c.current!.message.frameBytes(c.current!.next))) {
        this.frameWaits[cls].removeAt(index);
        return c;
      }
      // Once a frame has waited this long, nothing smaller of its class starts ahead of it.
      if (first && now - c.frameSince! >= FRAME_BLOCK_MS) return undefined;
      first = false;
    }
    const rings = cls === 'healthy' ? [this.healthy] : [...(freshOk ? [this.untried, this.fresh] : []), ...(slowOk ? [this.slow] : [])];
    for (const ring of rings) {
      while (this.hasEntry(ring)) {
        const entry = ring.shift()!;
        if (ring !== this.slow || this.isSlow(entry.c, now)) return entry.c;
        this.reclass(entry);
      }
    }
    return undefined;
  }

  private fits(cls: Class, bytes: number) {
    if (cls === 'healthy') return this.healthyInFlight === 0 || this.budget.frames + bytes <= this.budget.frameLimit;
    return this.probationInFlight === 0 || this.probationFrames + bytes <= this.budget.frameLimit / 4;
  }

  // Opens the connection's next message if it has none, then starts its next POST if the frame fits.
  private begin(c: Conn<J>, cls: Class, now: number) {
    c.token++;
    while (!c.current) {
      if (c.head >= c.tasks.length) return this.retire(c);
      const task = c.tasks[c.head];
      let message: Message | undefined;
      try {
        message = this.handlers.open(task.job, task.index);
      } catch (error) {
        this.shift(c);
        this.settleTask(task, { status: 'rejected', reason: error });
        continue;
      }
      this.shift(c);
      if (message === undefined) {
        this.settleTask(task, sent);
        continue;
      }
      const once = this.handlers.once(task.job, task.index);
      c.current = { task, message, next: 0, u: 0, own: 0, faults: 0, lastProgressAt: performance.now(), once };
    }
    if (this.chunkLate(c.current)) return this.closeConnection(c, CHUNK_LATE);
    const bytes = c.current.message.frameBytes(c.current.next);
    if (!this.fits(cls, bytes)) {
      c.state = 'frame';
      c.frameSince = now;
      this.frameWaits[cls].push({ c, token: c.token });
      return;
    }
    this.post(c, cls, bytes, now);
  }

  private shift(c: Conn<J>) {
    const task = c.tasks[c.head++];
    if (!task.job.ping) c.queuedPosts -= task.job.posts;
    if (c.head > 64 && c.head * 2 > c.tasks.length) {
      c.tasks = c.tasks.slice(c.head);
      c.head = 0;
    }
  }

  private post(c: Conn<J>, cls: Class, bytes: number, now: number) {
    const current = c.current!;
    if (cls === 'healthy' && c.readySince !== undefined) {
      const waited = now - c.readySince;
      if (waited > this.periodWait) this.periodWait = waited;
      if (waited > TURN_WARN_MS) this.logTurn(waited, now);
    }
    c.readySince = undefined;
    c.frameSince = undefined;
    c.tried = true;
    // A slow connection's attempt runs to the request timeout; anyone else's is cut once it runs well past
    // its usual time.
    const T = c.slow ? Infinity : this.threshold(c, cls, bytes, now);
    const flight: Flight = { startedAt: now, controller: new AbortController(), bytes, cls, T, slow: c.slow, suspect: !!c.suspect };
    flight.timer = setTimeout(() => this.cut(c, flight, 'connect'), CONNECT_TIMEOUT_MS);
    flight.timer.unref();
    c.flight = flight;
    c.state = 'flight';
    this.inFlight++;
    this.budget.frames += bytes;
    if (cls === 'probation') {
      this.probationInFlight++;
      this.probationFrames += bytes;
    } else this.healthyInFlight++;
    if (flight.slow) this.slowInFlight++;

    const connected = () => {
      if (c.flight !== flight || flight.connectedAt !== undefined) return;
      flight.connectedAt = Date.now();
      clearTimeout(flight.timer);
      flight.timer = setTimeout(() => this.cut(c, flight, 'timeout'), this.limits.requestTimeoutMs);
      flight.timer.unref();
      this.armAbort(c, flight);
    };
    let posting: Promise<unknown>;
    try {
      posting = Promise.resolve(current.message.post(current.next, flight.controller.signal, cls === 'probation', connected));
    } catch (error) {
      posting = Promise.reject(error);
    }
    const cut = new Promise<typeof CUT>(resolve =>
      flight.controller.signal.addEventListener('abort', () => resolve(CUT), { once: true })
    );
    Promise.race([posting, cut]).then(
      () => this.landed(c, flight),
      error => this.landed(c, flight, { error })
    );
  }

  // Deferred past pending I/O, so a response that arrived while the event loop was busy lands first. A whole
  // message cut at T keeps running without its slot while the release caps allow.
  private cut(c: Conn<J>, flight: Flight, cut: 'abort' | 'connect' | 'timeout') {
    setImmediate(() => {
      if (c.flight !== flight || flight.controller.signal.aborted) return;
      if (cut === 'abort' && this.release(c, flight)) return;
      flight.cut = cut;
      flight.controller.abort();
    });
  }

  private release(c: Conn<J>, flight: Flight) {
    const current = c.current!;
    if (flight.released || current.message.posts !== 1 || current.once) return false;
    if (this.releases >= RELEASES_PER_SLOT * this.limits.slots || this.budget.released + flight.bytes > RELEASE_BYTES) return false;
    clearTimeout(flight.abortTimer);
    this.freeSlot(flight, Date.now());
    flight.released = true;
    this.releases++;
    this.budget.released += flight.bytes;
    c.state = 'released';
    c.token++;
    this.pump();
    return true;
  }

  private freeSlot(flight: Flight, now: number) {
    this.inFlight--;
    this.budget.frames -= flight.bytes;
    if (flight.cls === 'probation') {
      this.probationInFlight--;
      this.probationFrames -= flight.bytes;
    } else {
      this.healthyInFlight--;
      this.periodHoldSum += now - flight.startedAt;
      this.periodHolds++;
    }
    if (flight.slow) this.slowInFlight--;
  }

  // The abort clock runs from the connected socket, so lookup, TCP and TLS setup never count toward a cut.
  private armAbort(c: Conn<J>, flight: Flight) {
    clearTimeout(flight.abortTimer);
    if (!Number.isFinite(flight.T) || flight.connectedAt === undefined || flight.released) return;
    const ms = Math.max(0, flight.connectedAt + flight.T - Date.now());
    flight.abortTimer = setTimeout(() => this.cut(c, flight, 'abort'), ms);
    flight.abortTimer.unref();
  }

  // Twice the connection's own p99 in the frame's size class, or the global value with no history there; for
  // a healthy attempt at most the cap. Never under twice Ref.
  private threshold(c: Conn<J>, cls: Class, bytes: number, now: number) {
    if (now - this.statsAt >= 1000) this.updateStats(now);
    const k = bytes <= SMALL_FRAME_BYTES ? 0 : 1;
    const own = this.ownP99(c, k);
    const base = own !== undefined ? Math.max(FLOOR_MIN_MS, 2 * own) : this.global[k];
    let t = base;
    if (cls === 'healthy') {
      if (base > this.periodMaxT) this.periodMaxT = base;
      t = Math.min(base, this.cap);
    }
    return Math.max(FLOOR_MIN_MS, 2 * this.ref, t);
  }

  private ownP99(c: Conn<J>, k: number) {
    const n = Math.min(c.recorded?.[k] ?? 0, P99_RING);
    if (n === 0) return undefined;
    let max = 0;
    for (let i = 0; i < n; i++) max = Math.max(max, c.durations![k * P99_RING + i]);
    return max;
  }

  // Lowers the threshold of healthy attempts in flight to a new cap; one already past it is cut at once.
  private rearm() {
    for (const c of this.active) {
      const flight = c.flight;
      if (!flight || flight.released || flight.cls !== 'healthy' || !Number.isFinite(flight.T)) continue;
      const T = Math.max(FLOOR_MIN_MS, 2 * this.ref, Math.min(flight.T, this.cap));
      if (T >= flight.T) continue;
      flight.T = T;
      this.armAbort(c, flight);
    }
  }

  private landed(c: Conn<J>, flight: Flight, failure?: { error: unknown }) {
    clearTimeout(flight.timer);
    clearTimeout(flight.abortTimer);
    const now = Date.now();
    if (flight.released) {
      this.releases--;
      this.budget.released -= flight.bytes;
    } else this.freeSlot(flight, now);
    if (c.flight !== flight) return this.pump();
    c.flight = undefined;
    const ms = now - (flight.connectedAt ?? flight.startedAt);
    const current = c.current!;
    current.own += now - flight.startedAt;
    this.batch(() => {
      if (flight.cut === 'abort') this.aborted(c, current, ms, now);
      else if (flight.cut === 'timeout') {
        if (current.once) this.giveUp(c, current, new Error(`no answer within ${ms}ms`), now);
        else this.closeConnection(c, 'a send went unanswered past the request timeout');
      } else if (flight.cut === 'connect') {
        this.failed(c, current, new Error(`no connection within ${CONNECT_TIMEOUT_MS}ms`), now, flight);
      } else if (!failure) this.succeeded(c, current, flight, ms, now);
      else this.failed(c, current, failure.error, now, flight);
    });
    this.pump();
  }

  private aborted(c: Conn<J>, current: Current<J>, ms: number, now: number) {
    if (current.once) return this.giveUp(c, current, new Error(`cut after ${ms}ms`), now);
    current.u += ms;
    if (current.message.posts === 1) {
      c.holdUntil = now + ABORT_HOLD_MS;
      this.note('whole messages were cut past the release cap and sent again', `a whole message to connection ${c.connectionId} was cut past the release cap and is sent again`);
    }
    this.ready(c, now);
  }

  private succeeded(c: Conn<J>, current: Current<J>, flight: Flight, ms: number, now: number) {
    if (flight.cls === 'healthy') this.periodDurations.push(ms);
    let recent = this.recent.get(c.endpoint);
    if (!recent) this.recent.set(c.endpoint, (recent = []));
    recent.push({ start: flight.connectedAt ?? flight.startedAt, ms });
    this.record(c, ms, flight.bytes, now);
    this.refill(now);
    this.retryTokens = Math.min(2 * this.limits.slots, this.retryTokens + 1);
    if (this.firstTokenWaiter()) this.grantTokens(now);
    c.strikes = 0;
    current.next++;
    current.u = 0;
    current.faults = 0;
    current.lastProgressAt = performance.now();
    if (current.next >= current.message.posts) {
      c.current = undefined;
      this.settleTask(current.task, sent);
    }
    this.ready(c, now);
  }

  private failed(c: Conn<J>, current: Current<J>, error: unknown, now: number, flight: Flight) {
    const failure = classifyFailure(error);
    if (failure === 'expired') {
      this.handlers.expired(error);
      return this.giveUp(c, current, error, now);
    }
    if (failure === 'gone') {
      this.handlers.gone(c.endpoint, c.connectionId);
      return this.closeConnection(c);
    }
    if (failure === 'forbidden') {
      const shared = this.sharedDenial(c, now);
      this.noteDenial(c.endpoint, c.key, now);
      if (!current.once) {
        if (shared) c.strikes = 0;
        else if (++c.strikes >= STRIKES) return this.closeConnection(c, '403 not shared by the endpoint', String(error));
      }
    }
    if (current.once) return this.giveUp(c, current, error, now);
    if (failure === 'rejected') return this.closeConnection(c, 'the gateway rejected a send', String(error));
    if (failure === 'timeout') return this.closeConnection(c, 'a send went unanswered past the request timeout');
    const holdUntil = now + HOLDS_MS[Math.min(current.faults++, HOLDS_MS.length - 1)];
    if (this.chunkLate(current, holdUntil - now)) return this.closeConnection(c, CHUNK_LATE, String(error));
    if (failure === 'reset' && flight.connectedAt !== undefined && current.message.posts === 1) {
      this.note('sends were reset after connecting and sent again', `a send to connection ${c.connectionId} was reset after connecting and is sent again; the gateway may still deliver the first copy`);
    } else this.note('sends failed and wait to be sent again', `a send to connection ${c.connectionId} failed and waits to be sent again: ${error}`);
    c.holdUntil = holdUntil;
    c.needsToken = true;
    this.ready(c, now);
  }

  // Settles a send that is tried once as failed.
  private giveUp(c: Conn<J>, current: Current<J>, error: unknown, now: number) {
    c.current = undefined;
    this.settleTask(current.task, { status: 'rejected', reason: error });
    this.ready(c, now);
  }

  private sharedDenial(c: Conn<J>, now: number) {
    return this.denials.get(c.endpoint)?.some(d => d.key !== c.key && now - d.at <= SHARED_403_MS) ?? false;
  }

  private noteDenial(endpoint: string, key: string, now: number) {
    let seen = this.denials.get(endpoint);
    if (!seen) this.denials.set(endpoint, (seen = []));
    const at = seen.findIndex(d => d.key === key);
    if (at >= 0) seen.splice(at, 1);
    seen.unshift({ key, at: now });
    if (seen.length > 2) seen.pop();
  }

  private refill(now: number) {
    this.retryTokens = Math.min(2 * this.limits.slots, this.retryTokens + ((now - this.retryTokensAt) * this.limits.slots) / 1000);
    this.retryTokensAt = now;
  }

  private takeRetry(now: number) {
    this.refill(now);
    if (this.retryTokens < 1) return false;
    this.retryTokens--;
    return true;
  }

  private tokenWaitMs() {
    return Math.max(1, ((1 - this.retryTokens) * 1000) / this.limits.slots);
  }

  private record(c: Conn<J>, ms: number, bytes: number, now: number) {
    c.L = c.L === undefined ? ms : c.L + ALPHA * (ms - c.L);
    c.lastMs = ms;
    c.completedAt = now;
    const large = bytes > SMALL_FRAME_BYTES;
    const k = large ? 1 : 0;
    c.durations ??= new Uint16Array(2 * P99_RING);
    c.recorded ??= [0, 0];
    c.durations[k * P99_RING + (c.recorded[k]++ % P99_RING)] = Math.min(ms, 0xffff);
    this.samples.push({ at: now, ms, large });
    if (this.samples.length - this.samplesHead > MAX_SAMPLES) this.samplesHead++;
  }

  private settleTask(task: Task<J>, result: PromiseSettledResult<unknown>) {
    const { job } = task;
    this.budget.addOverhead(-ENTRY_BYTES, job.stream);
    if (--job.pending === 0) {
      this.budget.addOverhead(-JOB_BYTES, job.stream);
      this.budget.release(job.event ?? job);
    }
    this.handlers.settle(job, task.index, result);
  }

  private closeConnection(c: Conn<J>, reason?: string, detail?: string) {
    if (c.closed) return;
    c.closed = true;
    c.token++;
    c.state = 'idle';
    if (this.conns.get(c.key) === c) this.conns.delete(c.key);
    if (this.active.delete(c)) this.budget.add(-QUEUE_BYTES);
    clearTimeout(c.timer);
    const flight = c.flight;
    c.flight = undefined;
    if (flight) {
      flight.cut = 'close';
      flight.controller.abort();
    }
    const tasks = c.tasks.slice(c.head);
    if (c.current) tasks.unshift(c.current.task);
    c.current = undefined;
    c.tasks = [];
    c.head = 0;
    c.queuedPosts = 0;
    if (reason) this.closing.push({ endpoint: c.endpoint, connectionId: c.connectionId, reason, detail });
    for (const task of tasks) this.settleTask(task, dropped);
    if (!this.batching) this.flushCloses();
    this.pump();
  }

  // Runs fn with the connections it closes handed over together at the end.
  private batch(fn: () => void) {
    const outer = this.batching;
    this.batching = true;
    try {
      fn();
    } finally {
      this.batching = outer;
      if (!outer) this.flushCloses();
    }
  }

  private flushCloses() {
    if (this.closing.length === 0) return;
    const closed = this.closing;
    this.closing = [];
    this.handlers.close(closed);
  }

  // Logs the first of a kind at once and how many more followed in the next NOTE_MS.
  private note(kind: string, first: string) {
    const tally = this.notes.get(kind);
    if (tally) {
      tally.count++;
      return;
    }
    console.warn(first);
    const entry = { count: 0 };
    this.notes.set(kind, entry);
    setTimeout(() => {
      this.notes.delete(kind);
      if (entry.count > 0) console.warn(`${entry.count} more ${kind} in ${NOTE_MS}ms`);
    }, NOTE_MS).unref();
  }

  private watch() {
    if (this.timer || this.stopped) return;
    this.timer = setInterval(() => this.check(), CHECK_MS);
    this.timer.unref();
  }

  private check() {
    const now = Date.now();
    if (now - this.statsAt >= 1000) this.updateStats(now);
    this.updateRef3(now);
    this.recheckSlow(now);
    this.batch(() => {
      for (const c of [...this.active]) {
        if (c.closed) continue;
        if (this.slowByL(c)) c.slowSince ??= now;
        else c.slowSince = undefined;
        const flight = c.flight;
        const ref3 = this.ref3.get(c.endpoint);
        if (flight?.suspect && !flight.released && flight.connectedAt !== undefined && ref3 !== undefined) {
          const hungAfter = Math.max(HUNG_MIN_MS, 2 * ref3);
          const ms = now - flight.connectedAt;
          if (ms > hungAfter) {
            this.closeConnection(c, 'a send went unanswered while others to its endpoint answered', `${ms}ms against ${hungAfter}ms`);
            continue;
          }
        }
        if (c.current && !c.current.once && this.ownMs(c, now) >= OWN_TIME_MS) {
          this.closeConnection(c, `its oldest message has been sending for ${OWN_TIME_MS / 1000}s`);
          continue;
        }
        const drainMs = this.drainMs(c, now);
        if (drainMs <= this.limits.maxDrainMs) c.drainOverSince = undefined;
        else if (now - (c.drainOverSince ??= now) >= DRAIN_GRACE_MS) {
          this.closeConnection(c, `what it alone holds would take over ${this.limits.maxDrainMs / 1000}s to send`, `${Math.round(drainMs / 1000)}s`);
          continue;
        }
        if (c.state === 'ready' && !c.front && c.current && c.current.next > 0 && performance.now() - c.current.lastProgressAt >= MID_MESSAGE_MS) {
          this.ready(c, now, true);
        }
      }
      this.handlers.check();
    });
    this.pump();
    if (this.active.size === 0) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  // Ref is the k-th fastest of the connections' latest POSTs, k = min(10, ceil(n/10)); the global threshold in
  // each frame size class is twice the 99th percentile of recent POSTs in it.
  private updateStats(now: number) {
    this.statsAt = now;
    const window = Math.max(REF_WINDOW_MIN_MS, 2 * (this.rotation ?? 0));
    const lasts: number[] = [];
    let healthy = false;
    for (const c of this.conns.values()) {
      if (c.completedAt === undefined) continue;
      if (now - c.completedAt <= window) lasts.push(c.lastMs!);
      if (now - c.completedAt <= WINDOW_MS && !c.slow) healthy = true;
      if (now - c.completedAt > STATS_TTL_MS && !this.active.has(c)) this.conns.delete(c.key);
    }
    this.healthyExists = healthy;
    lasts.sort((a, b) => a - b);
    this.ref = lasts.length > 0 ? lasts[Math.min(10, Math.ceil(lasts.length / 10)) - 1] : 0;
    while (this.samplesHead < this.samples.length && now - this.samples[this.samplesHead].at > WINDOW_MS) this.samplesHead++;
    if (this.samplesHead > 1024 && this.samplesHead * 2 > this.samples.length) {
      this.samples = this.samples.slice(this.samplesHead);
      this.samplesHead = 0;
    }
    const recent: [number[], number[]] = [[], []];
    for (let i = this.samplesHead; i < this.samples.length; i++) recent[this.samples[i].large ? 1 : 0].push(this.samples[i].ms);
    this.global = recent.map(v => (v.length > 0 ? Math.min(FLOOR_MAX_MS, Math.max(FLOOR_MIN_MS, 2 * percentile(v, 0.99))) : FLOOR_MIN_MS));
    for (const [endpoint, seen] of this.denials) if (seen.every(d => now - d.at > SHARED_403_MS)) this.denials.delete(endpoint);
    this.updateCap(now);
  }

  // Closed loop on the healthy rotation, evaluated once a second: steps down at once on the larger of the raw and
  // smoothed rotation, and rises by 25% a second once the smoothed one is well under target. See DESIGN.md.
  private updateCap(now: number) {
    let ready = 0;
    for (const _ of this.frameWaiters('healthy')) ready++;
    for (const c of this.active) if (c.state === 'ready' && !c.slow && c.completedAt !== undefined) ready++;
    const head = this.hasEntry(this.healthy) ? this.healthy.peek()!.c : undefined;
    const oldest = head?.readySince !== undefined ? now - head.readySince : 0;
    const hold = this.periodHolds > 0 ? this.periodHoldSum / this.periodHolds : 0;
    const predicted = ((ready + this.healthyInFlight) * hold) / Math.max(SLOT_RESERVE, this.limits.slots - this.probationInFlight);
    const raw = Math.max(this.periodWait, oldest, predicted);
    this.rotation = this.rotation === undefined ? raw : 0.5 * this.rotation + 0.5 * raw;
    const over = Math.max(this.rotation, raw);
    if (over > ROTATION_TARGET_MS) {
      // Hung attempts never complete, so they never hold the cap up; a live ring's own durations do.
      const guard = this.periodDurations.length > 0 ? 2 * percentile(this.periodDurations, 0.9) : 0;
      const from = Number.isFinite(this.cap) ? this.cap : this.periodMaxT || this.global[0];
      const next = Math.max(FLOOR_MIN_MS, guard, (from * ROTATION_TARGET_MS) / over);
      if (next < this.cap) {
        this.cap = next;
        this.rearm();
      }
    } else if (this.rotation < 0.7 * ROTATION_TARGET_MS && Number.isFinite(this.cap)) {
      this.cap *= 1.25;
      if (this.cap > FLOOR_MAX_MS) this.cap = Infinity;
    }
    this.periodWait = 0;
    this.periodHoldSum = 0;
    this.periodHolds = 0;
    this.periodDurations = [];
    this.periodMaxT = 0;
  }

  // Ref_3s: the k-th fastest of an endpoint's attempts that both started and completed in the last 3s, k being
  // min(10, ceil(n/10)). Attempts that started before a slowdown drop out within 3s of it.
  private updateRef3(now: number) {
    this.ref3.clear();
    for (const [endpoint, entries] of this.recent) {
      const kept = entries.filter(e => now - e.start <= REF3_WINDOW_MS);
      if (kept.length === 0) {
        this.recent.delete(endpoint);
        continue;
      }
      this.recent.set(endpoint, kept);
      const ms = kept.map(e => e.ms).sort((a, b) => a - b);
      this.ref3.set(endpoint, ms[Math.min(10, Math.ceil(ms.length / 10)) - 1]);
    }
  }

  private logTurn(waitedMs: number, now: number) {
    if (now - this.turnLoggedAt < TURN_LOG_MS) return;
    this.turnLoggedAt = now;
    console.warn(
      `a healthy connection waited ${(waitedMs / 1000).toFixed(1)}s for its turn to send (${this.active.size} connections busy on ${this.limits.slots} slots)`
    );
  }
}
