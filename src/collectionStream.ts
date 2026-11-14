import mongoose, { AnyObject } from 'mongoose';
import { Budget } from './budget.js';
import { FanoutCursor } from './fanoutCursor.js';
import { Matcher, Projector, normalizeBson } from './matcher.js';
import { sizeEvent } from './sizing.js';

export type ChangeEvent = mongoose.mongo.ChangeStreamDocument<AnyObject>;
// bytes estimates the heap the event holds, its normalized copy included; json bounds its message body.
export type StreamEvent = { change: ChangeEvent; bytes: number; json: number; stream: CollectionStream };
// Queues the event to the subscription and calls settled once, when every send of it has settled. False, with
// nothing queued, while the event's message body must wait for room; the stream is woken to try again.
export type DeliverFn = (subscriptionId: string, event: StreamEvent, projector: Projector, settled: () => void) => boolean;

export type Registration = {
  matcher: Matcher;
  projector: Projector;
  updatedAt?: Date;
  // Left out of matching until its replay has been sent.
  walking?: boolean;
};

type StartOptions = { startAfter: unknown } | { startAtOperationTime: mongoose.mongo.Timestamp } | Record<string, never>;

type Unsettled = { seq: number; tokenBefore: unknown; matching: boolean };

type BufferedEvent = StreamEvent & {
  normalized: unknown;
  receivedAt: number;
  seq: number;
  tokenBefore: unknown;
  // Created at receipt, so the persisted token never passes an event read but not yet matched.
  entry: Unsettled;
  matched?: boolean;
  // Replays still to send it, which keep it in the buffer.
  pins: number;
};

type Matching = {
  event: BufferedEvent;
  entry: Unsettled;
  registrations: Iterator<[string, Registration]>;
  // The registration whose message body waits for room.
  retry?: [string, Registration];
  prepared: boolean;
  pending: number;
  settled: () => void;
  ms: number;
};

// The buffered events a joiner missed, sent to it ahead of the next event matched.
type Walk = {
  subscriptionId: string;
  registration: Registration;
  events: BufferedEvent[];
  next: number;
  deliver: DeliverFn;
  entry: Unsettled;
  pending: number;
  settled: () => void;
  done?: () => void;
};

export type WindowLost = 'evicted' | 'trimmed';

// After an event over 16MB fails the stream, 'narrow' reads one event per batch to pin the failure
// to it, and 'skip' reads only that event's metadata to step past it.
type StreamMode = 'normal' | 'narrow' | 'skip';

function clusterTime(change: ChangeEvent) {
  return (change as { clusterTime?: mongoose.mongo.Timestamp }).clusterTime;
}

function latest(through: number | undefined, seconds: number | undefined) {
  return seconds === undefined ? through : Math.max(through ?? seconds, seconds);
}

const BUFFER_MAX_EVENTS = 500;
const BUFFER_MAX_AGE_MS = 30 * 1000;
const PERSIST_INTERVAL_MS = 5 * 1000;
// A quiet stream saves the server's resume position this often, drawn per stream so streams opened together
// do not write together.
const QUIET_SAVE_MIN_MS = 45 * 1000;
const QUIET_SAVE_SPREAD_MS = 30 * 1000;
// A getMore reply holds up to batchSize events; this sizes it to about 1MB from the events seen lately.
const BATCH_TARGET_BYTES = 1024 * 1024;
const BATCH_HISTORY = 100;
const INITIAL_BATCH_SIZE = 16;
const MAX_BATCH_SIZE = 1000;
// An open holds its turn until its first batch, or this long.
const OPEN_TURN_MS = 2000;
const REOPEN_DELAY_MS = 1000;
const MAX_REOPEN_DELAY_MS = 60 * 1000;
const BSON_OBJECT_TOO_LARGE = 10334;
// Server errors that a reopen can outlast: network, failover and shutdown codes.
const TRANSIENT_CODES = new Set([6, 7, 43, 63, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 11602, 13388, 13435, 13436]);
// The matcher yields after this long, so POSTs and their callbacks run between slices of a long event.
const SLICE_MS = 5;
// A stream stops reading while more events than this wait to be matched, or more matching time than this.
const MATCH_QUEUE_EVENTS = 1000;
const MATCH_QUEUE_MS = 20 * 1000;
const MATCH_ALPHA = 0.2;

class MatchLoop {
  private streams: Set<CollectionStream> = new Set();
  private scheduled = false;

  add(stream: CollectionStream) {
    this.streams.add(stream);
    this.schedule();
  }

  remove(stream: CollectionStream) {
    this.streams.delete(stream);
  }

  private schedule() {
    if (this.scheduled) return;
    this.scheduled = true;
    setImmediate(() => this.run());
  }

  private run() {
    this.scheduled = false;
    const deadline = performance.now() + SLICE_MS;
    while (this.streams.size > 0 && performance.now() < deadline) {
      const stream: CollectionStream = this.streams.values().next().value!;
      const outcome = stream.matchSome(deadline);
      if (outcome === 'sliced') break;
      this.streams.delete(stream);
      if (outcome === 'more') this.streams.add(stream);
    }
    if (this.streams.size > 0) this.schedule();
  }
}

const matchLoop = new MatchLoop();

export function reopenDelay(retries: number, baseMs = REOPEN_DELAY_MS, maxMs = MAX_REOPEN_DELAY_MS) {
  return Math.min(baseMs * 2 ** (retries - 1), maxMs);
}

export function isResumeExpiredError(error: { message?: string }) {
  return (
    error.message?.includes('Resume of change stream was not possible') ||
    error.message?.includes('resume point may no longer be in the oplog') ||
    error.message?.includes('the resume token was not found')
  );
}

// Errors without a server code are the driver's own, such as a lost connection.
function isTransientError(error: Error) {
  const { code, errorLabels } = error as { code?: unknown; errorLabels?: string[] };
  return typeof code !== 'number' || TRANSIENT_CODES.has(code) || !!errorLabels?.includes('ResumableChangeStreamError');
}

// Resolves once the server has answered the initial aggregate, so callers know
// events from this point on will be seen. The driver does not forward 'init' to
// the ChangeStream, but the initial batch always sets a resume token.
// Resolves true once initialised, false on an error or timeout.
export function waitForStreamInit(stream: mongoose.mongo.ChangeStream<AnyObject>, timeoutMs = 10 * 1000) {
  return new Promise<boolean>(resolve => {
    const finish = (initialised: boolean) => {
      clearTimeout(timer);
      stream.off('resumeTokenChanged', onInit);
      stream.off('error', onFailure);
      resolve(initialised);
    };
    const onInit = () => finish(true);
    const onFailure = () => finish(false);
    const timer = setTimeout(onFailure, timeoutMs);
    stream.once('resumeTokenChanged', onInit);
    stream.once('error', onFailure);
  });
}

// Opens, reopens and resumes take turns, a few at a time.
export class OpenGate {
  private running = 0;
  private waiting: Array<() => void> = [];

  constructor(private width = 4) {}

  acquire() {
    return new Promise<() => void>(resolve => {
      const grant = () => {
        let released = false;
        resolve(() => {
          if (released) return;
          released = true;
          const next = this.waiting.shift();
          if (next) next();
          else this.running--;
        });
      };
      if (this.running < this.width) {
        this.running++;
        grant();
      } else this.waiting.push(grant);
    });
  }
}

// Where a stream keeps its resume token between runs.
export type TokenStore = {
  load: () => Promise<string | undefined>;
  save: (resumeToken: string) => Promise<unknown>;
  clear: () => Promise<unknown>;
};

function cursorStore(collectionName: string): TokenStore {
  return {
    load: async () => (await FanoutCursor.findOne({ collectionName }).lean())?.resumeToken ?? undefined,
    save: async resumeToken => FanoutCursor.updateOne({ collectionName }, { $set: { resumeToken } }, { upsert: true }),
    clear: async () => FanoutCursor.deleteOne({ collectionName }),
  };
}

export type CollectionStreamOptions = {
  reopenDelayMs?: number;
  maxReopenDelayMs?: number;
  bufferMaxAgeMs?: number;
  matchQueueEvents?: number;
  // Charged with each event from read until it leaves the replay buffer; events that cannot fit are skipped.
  budget?: Budget;
  // The read gate: whether an event of this heap may be charged now. Refused, the stream stops reading and reads
  // it again once resumed.
  admit?: (stream: CollectionStream, heap: number) => boolean;
  // The match gate: whether the next change or replay step may start matching. Refused, the stream is woken later.
  mayMatch?: (stream: CollectionStream) => boolean;
  // Readies an event before it is matched, as a lookup does; false until then, and the stream is woken.
  prepare?: (event: StreamEvent) => boolean;
  // Called after each event is matched.
  onRead?: (stream: CollectionStream) => void;
  // Called when the resume position has left the oplog, before the stream reopens live.
  onHistoryLost?: (stream: CollectionStream) => void;
  // Called when a delivery that waited for room is tried again and does not wait again, however that try ends,
  // or when the stream closes while one waits.
  waitEnded?: (stream: CollectionStream) => void;
  gate?: OpenGate;
  // Stages after the stream's own projection, such as a dedicated subscription's server-side filter.
  pipeline?: Array<Record<string, unknown>>;
  tokens?: TokenStore;
  label?: string;
};

export class CollectionStream {
  private registrations: Map<string, Registration> = new Map();
  private buffer: BufferedEvent[] = [];
  private stream?: mongoose.mongo.ChangeStream<AnyObject>;
  private mode: StreamMode = 'normal';
  private state: 'idle' | 'opening' | 'open' | 'closed' = 'idle';
  private initialStart: StartOptions = {};
  // Where the current stream was opened from.
  private openedFrom: StartOptions = {};
  private lastToken?: unknown;
  // The server's position when the stream was closed with nothing left to match.
  private closedToken?: unknown;
  // The token before each event whose delivery has not settled. The persisted token never passes
  // the earliest of them, so a restart redelivers whatever was in flight.
  private unsettled: Set<Unsettled> = new Set();
  // Events read and not yet matched, oldest first, and the event being matched.
  private queue: BufferedEvent[] = [];
  private queueHead = 0;
  private current?: Matching;
  private walks: Walk[] = [];
  // The seq of the latest event whose matching started.
  private matchedSeq = 0;
  private matchMs?: number;
  // Stopped reading while too much waits to be matched, apart from a pause for memory.
  matchHold = false;
  private seq = 0;
  private onSettled?: () => void;
  private persistedToken?: string;
  private persistTimer?: NodeJS.Timeout;
  private quietTimer?: NodeJS.Timeout;
  private reopenTimer?: NodeJS.Timeout;
  private trimTimer?: NodeJS.Timeout;
  private retries = 0;
  private pendingPersist?: Promise<void>;
  paused = false;
  pausedSince?: number;
  // When a stream resumed first after a long pause was paused, until what it was refused is admitted.
  priority?: number;
  // From a refused lookup until the change it was for is ready to match.
  private lookupRefused = false;
  // h: how far under the budget the stored heap must fall for the stream to resume.
  pauseCharge = 0;
  // The largest event heap read since the last resume, and that event's json size.
  largestCharge = 0;
  largestJson = 0;
  // Where to reopen on resume, once a stream or a reopen was stopped by the pause.
  private pausedAt?: { start: StartOptions; mode: StreamMode };
  // clusterTime seconds of the newest event dropped from the replay buffer by age or count, and for memory.
  private trimmedThroughSeconds?: number;
  private evictedThroughSeconds?: number;
  private readonly reopenDelayMs: number;
  private readonly maxReopenDelayMs: number;
  private readonly bufferMaxAgeMs: number;
  private readonly matchQueueEvents: number;
  private readonly quietSaveMs = QUIET_SAVE_MIN_MS + Math.random() * QUIET_SAVE_SPREAD_MS;
  private readonly budget?: Budget;
  private readonly admit?: (stream: CollectionStream, heap: number) => boolean;
  private readonly mayMatch?: (stream: CollectionStream) => boolean;
  private readonly prepare?: (event: StreamEvent) => boolean;
  private readonly onRead?: (stream: CollectionStream) => void;
  private readonly onHistoryLost?: (stream: CollectionStream) => void;
  private readonly waitEnded?: (stream: CollectionStream) => void;
  // Whether the delivery to try next waits for room.
  private waiting = false;
  private readonly gate: OpenGate;
  private opens = 0;
  // BSON sizes of the latest events, which set the batch size at each open.
  private recentSizes: number[] = [];
  private readonly pipeline: Array<Record<string, unknown>>;
  private readonly tokens: TokenStore;
  readonly label: string;

  constructor(
    readonly collectionName: string,
    private deliver: DeliverFn,
    {
      reopenDelayMs = REOPEN_DELAY_MS,
      maxReopenDelayMs = MAX_REOPEN_DELAY_MS,
      bufferMaxAgeMs = BUFFER_MAX_AGE_MS,
      matchQueueEvents = MATCH_QUEUE_EVENTS,
      budget,
      admit,
      mayMatch,
      prepare,
      onRead,
      onHistoryLost,
      waitEnded,
      gate = new OpenGate(),
      pipeline = [],
      tokens = cursorStore(collectionName),
      label = collectionName,
    }: CollectionStreamOptions = {}
  ) {
    this.reopenDelayMs = reopenDelayMs;
    this.maxReopenDelayMs = maxReopenDelayMs;
    this.bufferMaxAgeMs = bufferMaxAgeMs;
    this.matchQueueEvents = matchQueueEvents;
    this.budget = budget;
    this.admit = admit;
    this.mayMatch = mayMatch;
    this.prepare = prepare;
    this.onRead = onRead;
    this.onHistoryLost = onHistoryLost;
    this.waitEnded = waitEnded;
    this.gate = gate;
    this.pipeline = pipeline;
    this.tokens = tokens;
    this.label = label;
  }

  get size() {
    return this.registrations.size;
  }

  get isOpen() {
    return this.state === 'opening' || this.state === 'open';
  }

  has(subscriptionId: string) {
    return this.registrations.has(subscriptionId);
  }

  // Whether a subscription's replay at registration is still to be sent.
  replaying(subscriptionId: string) {
    return this.registrations.get(subscriptionId)?.walking === true;
  }

  register(subscriptionId: string, registration: Registration) {
    this.registrations.set(subscriptionId, registration);
    return registration.updatedAt ? this.replay(subscriptionId, registration.updatedAt) : undefined;
  }

  unregister(subscriptionId: string) {
    this.registrations.delete(subscriptionId);
    return this.registrations.size;
  }

  async open() {
    if (this.state !== 'idle') return;
    this.state = 'opening';

    let resumeToken;
    try {
      resumeToken = await this.tokens.load();
    } catch (error) {
      if (this.state !== 'opening') return;
      this.state = 'idle';
      const delayMs = this.nextDelay();
      console.error(`failed to read the resume token for ${this.label}, retrying in ${delayMs}ms: ${error}`);
      this.reopenTimer = setTimeout(() => void this.open(), delayMs);
      return;
    }
    if (this.state !== 'opening') return;

    if (resumeToken) {
      this.persistedToken = resumeToken;
      this.initialStart = { startAfter: JSON.parse(resumeToken) };
    } else {
      const oldest = Math.min(
        ...[...this.registrations.values()].map(r => r.updatedAt?.getTime() ?? Infinity)
      );
      this.initialStart = Number.isFinite(oldest)
        ? { startAtOperationTime: new mongoose.mongo.Timestamp({ t: Math.floor(oldest / 1000), i: 1 }) }
        : {};
    }

    this.state = 'open';
    this.scheduleQuietSave();
    await this.openStream(this.initialStart);
  }

  // On shutdown, deliveries still in flight get until drainUntil to settle before the token is saved.
  async close(reason: 'idle' | 'shutdown', drainUntil = Date.now()) {
    if (this.state === 'closed') return;
    const wasOpen = this.state !== 'idle';
    this.state = 'closed';
    clearTimeout(this.reopenTimer);
    clearTimeout(this.persistTimer);
    clearTimeout(this.quietTimer);
    clearTimeout(this.trimTimer);
    this.persistTimer = undefined;
    this.trimTimer = undefined;

    const stream = this.stream;
    this.stream = undefined;
    if (this.mode === 'normal') this.closedToken = stream?.resumeToken;
    const closing = stream?.close().catch(() => null);
    matchLoop.remove(this);
    if (this.current) this.current.entry.matching = true;
    this.current = undefined;
    this.walks = [];
    this.endWait(false);
    this.queue = [];
    this.queueHead = 0;
    for (const event of this.buffer.splice(0)) this.budget?.release(event);

    if (reason === 'shutdown') {
      await this.settled(drainUntil);
      await this.pendingPersist?.catch(() => null);
      await this.persistToken();
    } else {
      await closing;
      // A token write already in flight must land before the cursor is deleted,
      // or it would bring the cursor back.
      await this.pendingPersist?.catch(() => null);
      // Nobody is listening any more, so a later first subscriber should start
      // from its own updatedAt instead of replaying everything since now.
      await this.tokens.clear();
    }

    if (wasOpen) {
      console.info(`collection stream closed on ${this.label} (${this.registrations.size} subscriptions)`);
    }
  }

  // Stops reading for memory until resume(); h is how far under the budget the stored heap must fall first. A
  // stream that was resumed first keeps its place in line if it is paused again before it reads.
  pause(h: number, at?: StartOptions) {
    if (this.paused) {
      this.pauseCharge = Math.max(this.pauseCharge, h);
      return;
    }
    this.paused = true;
    this.pausedSince = this.priority ?? Date.now();
    this.priority = undefined;
    this.pauseCharge = h;
    this.stopReading(at);
  }

  // Pauses for a document looked up for the change being matched that did not fit; it is fetched again on resume.
  refuseLookup(h: number) {
    this.lookupRefused = true;
    this.pause(h);
  }

  resume(first = false) {
    if (!this.paused) return;
    this.paused = false;
    this.priority = first ? this.pausedSince : undefined;
    this.pausedSince = undefined;
    this.largestCharge = 0;
    this.largestJson = 0;
    this.startReading();
    this.wake();
  }

  wake() {
    if (this.state !== 'closed' && (this.current || this.walks.length > 0 || this.queueHead < this.queue.length)) matchLoop.add(this);
  }

  private get stopped() {
    return this.paused || this.matchHold;
  }

  private stopReading(at?: StartOptions) {
    const stream = this.stream;
    if (!stream) return;
    this.stream = undefined;
    stream.close().catch(() => null);
    this.pausedAt = { start: at ?? this.position(stream), mode: this.mode };
  }

  private startReading() {
    if (this.stopped) return;
    const at = this.pausedAt;
    this.pausedAt = undefined;
    if (at) this.scheduleReopen(at.start, 0, at.mode);
  }

  private batchSize() {
    if (this.recentSizes.length === 0) return INITIAL_BATCH_SIZE;
    const largest = Math.max(...this.recentSizes);
    return Math.max(1, Math.min(MAX_BATCH_SIZE, Math.floor(BATCH_TARGET_BYTES / largest)));
  }

  private async openStream(start: StartOptions, mode: StreamMode = 'normal') {
    if (this.stopped) {
      this.pausedAt = { start, mode };
      return;
    }
    const attempt = ++this.opens;
    const release = await this.gate.acquire();
    if (this.state === 'closed' || attempt !== this.opens) return release();
    if (this.stopped) {
      this.pausedAt = { start, mode };
      return release();
    }
    const collection = mongoose.connection.collection(this.collectionName);
    const stream =
      mode === 'skip'
        ? // A filter on the stream still needs the looked-up document to pick the event to skip.
          collection.watch<AnyObject, ChangeEvent>(
            [...this.pipeline, { $project: { operationType: 1, ns: 1, documentKey: 1, clusterTime: 1 } }],
            { ...start, batchSize: 1, ...(this.pipeline.length > 0 ? { fullDocument: 'updateLookup' } : {}) }
          )
        : // updateDescription is never delivered, and next to the looked-up document it can
          // push an update event past the 16MB BSON limit.
          collection.watch<AnyObject, ChangeEvent>([{ $project: { updateDescription: 0 } }, ...this.pipeline], {
            fullDocument: 'updateLookup',
            ...start,
            batchSize: mode === 'narrow' ? 1 : this.batchSize(),
          });
    this.stream = stream;
    this.mode = mode;
    this.openedFrom = start;
    stream.on('change', change => this.handleChange(change, stream));
    stream.on('error', error => this.handleError(stream, error));

    const startDescription =
      'startAfter' in start ? 'startAfter' : 'startAtOperationTime' in start ? 'startAtOperationTime' : 'live';
    console.info(
      `collection stream open on ${this.label} (${this.registrations.size} subscriptions, ${startDescription}${
        mode === 'normal' ? '' : `, ${mode}`
      })`
    );
    const turn = setTimeout(release, OPEN_TURN_MS);
    const initialised = await waitForStreamInit(stream);
    clearTimeout(turn);
    release();
    // A reopen that initialises cleanly resets the backoff, so errors spread over the life
    // of a quiet stream do not add up to the maximum delay.
    if (initialised && stream === this.stream) this.retries = 0;
  }

  private handleChange(change: ChangeEvent, stream: mongoose.mongo.ChangeStream<AnyObject>) {
    if (this.state === 'closed' || stream !== this.stream) return;
    this.retries = 0;
    const tokenBefore = this.lastToken;

    // A drop, rename or dropDatabase ends the stream with an invalidate event and
    // the driver closes the cursor without an error. Start a new stream right
    // after it so later subscribers on this collection keep receiving changes.
    if (change.operationType === 'invalidate') {
      this.passed(change);
      console.warn(`collection stream on ${this.label} invalidated, reopening`);
      this.stream = undefined;
      stream.close().catch(() => null);
      this.scheduleReopen({ startAfter: change._id }, this.reopenDelayMs);
      return;
    }

    if (this.mode === 'skip') {
      this.passed(change);
      this.reopenNormal(stream, change);
      const documentKey = JSON.stringify((change as { documentKey?: unknown }).documentKey).slice(0, 200);
      console.warn(`skipped a change on ${this.label} to ${documentKey}: the event is over 16MB`);
      return;
    }

    // Before normalizeBson, which would copy a Binary that still pins the reply it was read from.
    const size = sizeEvent(change);
    if (this.budget && size.heap + size.json > this.budget.limit) {
      this.passed(change);
      if (this.mode !== 'normal') this.reopenNormal(stream, change);
      const documentKey = JSON.stringify((change as { documentKey?: unknown }).documentKey).slice(0, 200);
      console.warn(
        `skipped a change on ${this.label} to ${documentKey}: it needs about ${size.heap + size.json} bytes, over the ${this.budget.limit} byte budget`
      );
      return;
    }
    if (this.admit && !this.admit(this, size.heap)) {
      this.pause(Math.max(this.budget!.limit / 8, size.heap), this.refusedAt(change));
      return;
    }
    // On resume the stream reads its refused change first; a refused lookup is admitted at match instead.
    if (!this.lookupRefused) this.priority = undefined;
    this.passed(change);
    if (this.mode !== 'normal') this.reopenNormal(stream, change);

    const normalized = normalizeBson(change);
    const now = Date.now();
    const seq = ++this.seq;
    const event: BufferedEvent = {
      change,
      normalized,
      receivedAt: now,
      bytes: size.heap,
      json: size.json,
      stream: this,
      seq,
      tokenBefore,
      entry: { seq, tokenBefore, matching: true },
      pins: 0,
    };
    if (size.heap > this.largestCharge) {
      this.largestCharge = size.heap;
      this.largestJson = size.json;
    }
    this.recentSizes.push(size.bson);
    if (this.recentSizes.length > BATCH_HISTORY) this.recentSizes.shift();
    this.buffer.push(event);
    this.budget?.hold(event, event.bytes, this);
    this.trimBuffer(now);
    this.unsettled.add(event.entry);
    this.queue.push(event);
    this.checkHold();
    matchLoop.add(this);
  }

  private passed(change: ChangeEvent) {
    this.lastToken = change._id;
    this.schedulePersist();
  }

  private reopenNormal(stream: mongoose.mongo.ChangeStream<AnyObject>, change: ChangeEvent) {
    this.stream = undefined;
    stream.close().catch(() => null);
    this.scheduleReopen({ startAfter: change._id }, 0);
  }

  // Where to read a refused event again. The driver's token is already past it, so before anything was read a
  // live stream restarts at the event's own cluster time.
  private refusedAt(change: ChangeEvent): StartOptions {
    if (this.lastToken !== undefined) return { startAfter: this.lastToken };
    if (Object.keys(this.openedFrom).length > 0) return this.openedFrom;
    const at = clusterTime(change);
    return at ? { startAtOperationTime: at } : {};
  }

  matchSome(deadline: number): 'sliced' | 'more' | 'empty' {
    if (this.state === 'closed') return 'empty';
    // A replay step that waits for room keeps its place in the body line, so only steps not yet tried are gated.
    if (!this.current && !this.waiting && (this.walks.length > 0 || this.queueHead < this.queue.length) && this.mayMatch?.(this) === false) return 'empty';
    if (!this.current && this.walks.length > 0) return this.walkSome(deadline);
    const current = this.current ?? this.startMatching();
    if (!current) return 'empty';
    const { event } = current;
    if (!current.prepared) {
      if (this.prepare && !this.prepare(event)) return 'empty';
      current.prepared = true;
      if (this.lookupRefused) {
        this.lookupRefused = false;
        this.priority = undefined;
      }
    }
    const started = performance.now();
    let visited = 0;
    for (;;) {
      let entry = current.retry;
      current.retry = undefined;
      if (!entry) {
        const next = current.registrations.next();
        if (next.done) break;
        entry = next.value;
      }
      const [subscriptionId, registration] = entry;
      let waits = false;
      try {
        if (!registration.walking && this.registrations.get(subscriptionId) === registration && this.matches(subscriptionId, registration, event)) {
          current.pending++;
          if (!this.deliver(subscriptionId, event, registration.projector, current.settled)) {
            current.pending--;
            current.retry = entry;
            current.ms += performance.now() - started;
            waits = true;
            return 'empty';
          }
        }
      } finally {
        this.endWait(waits);
      }
      if (++visited % 16 === 0 && performance.now() >= deadline) {
        current.ms += performance.now() - started;
        return 'sliced';
      }
    }
    current.ms += performance.now() - started;
    this.current = undefined;
    this.matchMs = this.matchMs === undefined ? current.ms : this.matchMs + MATCH_ALPHA * (current.ms - this.matchMs);
    event.matched = true;
    current.entry.matching = false;
    current.settled();
    this.trimBuffer(Date.now());
    this.checkHold();
    this.onRead?.(this);
    return this.walks.length > 0 || this.queueHead < this.queue.length ? 'more' : 'empty';
  }

  private startMatching(): Matching | undefined {
    if (this.queueHead >= this.queue.length) return undefined;
    const event = this.queue[this.queueHead++];
    if (this.queueHead > 1024 && this.queueHead * 2 > this.queue.length) {
      this.queue = this.queue.slice(this.queueHead);
      this.queueHead = 0;
    }
    this.matchedSeq = event.seq;
    const { entry } = event;
    const matching: Matching = {
      event,
      entry,
      registrations: this.registrations.entries(),
      prepared: false,
      // Held at one until every delivery has started, as one may settle at once.
      pending: 1,
      settled: () => {
        if (--matching.pending > 0) return;
        this.unsettled.delete(entry);
        this.checkSettled();
        this.schedulePersist();
      },
      ms: 0,
    };
    this.current = matching;
    return matching;
  }

  private walkSome(deadline: number): 'sliced' | 'more' | 'empty' {
    const walk = this.walks[0];
    const live = this.registrations.get(walk.subscriptionId) === walk.registration;
    let visited = 0;
    while (walk.next < walk.events.length) {
      const event = walk.events[walk.next];
      let waits = false;
      try {
        if (live && this.matches(walk.subscriptionId, walk.registration, event)) {
          walk.pending++;
          if (!walk.deliver(walk.subscriptionId, event, walk.registration.projector, walk.settled)) {
            walk.pending--;
            waits = true;
            return 'empty';
          }
        }
      } finally {
        this.endWait(waits);
      }
      event.pins--;
      walk.next++;
      if (++visited % 16 === 0 && performance.now() >= deadline) return 'sliced';
    }
    this.walks.shift();
    walk.registration.walking = false;
    walk.entry.matching = false;
    walk.settled();
    walk.done?.();
    this.trimBuffer(Date.now());
    return this.walks.length > 0 || this.queueHead < this.queue.length ? 'more' : 'empty';
  }

  // A delivery that waited for room leaves the line once the try after it ends without waiting again.
  private endWait(waits: boolean) {
    const waited = this.waiting;
    this.waiting = waits;
    if (waited && !waits) this.waitEnded?.(this);
  }

  get queued() {
    return this.queue.length - this.queueHead + (this.current ? 1 : 0);
  }

  // Stops reading while the match queue is over either bound, and reads again under half of both.
  private checkHold() {
    const queued = this.queued;
    const ms = queued * (this.matchMs ?? 0);
    if (!this.matchHold && (queued > this.matchQueueEvents || ms > MATCH_QUEUE_MS)) {
      console.warn(`${queued} events on ${this.label} wait to be matched, about ${Math.round(ms)}ms of matching; stopped reading`);
      this.matchHold = true;
      this.stopReading();
    } else if (this.matchHold && queued < this.matchQueueEvents / 2 && ms < MATCH_QUEUE_MS / 2) {
      console.info(`${queued} events on ${this.label} wait to be matched; reading again`);
      this.matchHold = false;
      this.startReading();
    }
  }

  // Whether no send is in flight: what is left unsettled has not been matched, and is read again after a restart.
  private get quiet() {
    for (const entry of this.unsettled) if (!entry.matching) return false;
    return true;
  }

  private checkSettled() {
    if (this.onSettled && this.quiet) this.onSettled();
  }

  private settled(until: number) {
    if (this.quiet) return Promise.resolve();
    return new Promise<void>(resolve => {
      const timer = setTimeout(resolve, Math.max(0, until - Date.now()));
      this.onSettled = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  // Never past the oldest event still to be matched or replayed; matching or replaying it trims again.
  private trimBuffer(now: number) {
    while (
      this.buffer[0]?.matched &&
      this.buffer[0].pins === 0 &&
      (this.buffer.length > BUFFER_MAX_EVENTS || now - this.buffer[0].receivedAt > this.bufferMaxAgeMs)
    ) {
      const event = this.buffer.shift()!;
      this.budget?.release(event);
      this.trimmedThroughSeconds = latest(this.trimmedThroughSeconds, clusterTime(event.change)?.getHighBits());
    }
    // Trim on a timer too, so a quiet collection does not hold its last burst of events.
    if (this.buffer[0]?.matched && this.buffer[0].pins === 0 && !this.trimTimer && this.state !== 'closed') {
      this.trimTimer = setTimeout(() => {
        this.trimTimer = undefined;
        this.trimBuffer(Date.now());
      }, this.buffer[0].receivedAt + this.bufferMaxAgeMs + 1 - now);
      this.trimTimer.unref();
    }
  }

  replayOnly() {
    return this.budget ? this.buffer.filter(event => event.matched && event.pins === 0 && this.budget!.holds(event) === 1) : [];
  }

  evict(event: StreamEvent) {
    const at = this.buffer.indexOf(event as BufferedEvent);
    if (at < 0) return;
    this.buffer.splice(at, 1);
    this.budget?.release(event);
    this.evictedThroughSeconds = latest(this.evictedThroughSeconds, clusterTime(event.change)?.getHighBits());
  }

  get subscriptionIds() {
    return [...this.registrations.keys()];
  }

  // Queues the matched events since a time to go out before the next match; without deliver the subscription is
  // left out of matching until then. Returns 'queued', why part of the window is gone, or nothing before it opens.
  replay(subscriptionId: string, since: Date, deliver?: DeliverFn, done?: () => void): WindowLost | 'queued' | undefined {
    const registration = this.registrations.get(subscriptionId);
    if (!registration || this.state !== 'open') return;
    const fromSeconds = Math.floor((new Date(since).getTime() - 1000) / 1000);
    this.trimBuffer(Date.now());
    if ((this.evictedThroughSeconds ?? -Infinity) >= fromSeconds) return 'evicted';
    if ((this.trimmedThroughSeconds ?? -Infinity) >= fromSeconds) return 'trimmed';
    const events = this.buffer.filter(event => event.seq <= this.matchedSeq && (clusterTime(event.change)?.getHighBits() ?? -Infinity) >= fromSeconds);
    if (events.length === 0) return;
    for (const event of events) event.pins++;
    const entry = { seq: events[0].seq, tokenBefore: events[0].tokenBefore, matching: true };
    const walk: Walk = {
      subscriptionId,
      registration,
      events,
      next: 0,
      deliver: deliver ?? this.deliver,
      entry,
      pending: 1,
      settled: () => {
        if (--walk.pending > 0) return;
        this.unsettled.delete(entry);
        this.checkSettled();
        this.schedulePersist();
      },
      done,
    };
    if (!deliver) registration.walking = true;
    this.unsettled.add(entry);
    this.walks.push(walk);
    matchLoop.add(this);
    return 'queued';
  }

  // One subscription's bad filter must not stop the event reaching the rest.
  private matches(subscriptionId: string, registration: Registration, { normalized }: BufferedEvent) {
    try {
      return registration.matcher(normalized);
    } catch (error) {
      console.warn(`filter failed for subscription ${subscriptionId} on ${this.label}: ${error}`);
      return false;
    }
  }

  // Before the first change, the stream's own token still marks where it got to.
  private position(stream: mongoose.mongo.ChangeStream<AnyObject>): StartOptions {
    const token = this.lastToken ?? stream.resumeToken;
    return token ? { startAfter: token } : this.initialStart;
  }

  private handleError(stream: mongoose.mongo.ChangeStream<AnyObject>, error: Error) {
    if (this.state === 'closed' || stream !== this.stream) return;
    this.stream = undefined;
    stream.close().catch(() => null);
    const start = this.position(stream);

    if ((error as { code?: unknown }).code === BSON_OBJECT_TOO_LARGE && this.mode !== 'skip') {
      const next = this.mode === 'normal' ? 'narrow' : 'skip';
      console.warn(`changeStream error on ${this.label}, reopening ${next}: ${error}`);
      this.scheduleReopen(start, 0, next);
      return;
    }

    if (isResumeExpiredError(error)) {
      console.error(`changeStream error on ${this.label}: ${error}`);
      console.log(`Resume token expired for ${this.label}, clearing and reopening live...`);
      this.onHistoryLost?.(this);
      this.lastToken = undefined;
      this.persistedToken = undefined;
      // The expired position must not be retried by a later reopen either.
      this.initialStart = {};
      clearTimeout(this.persistTimer);
      this.persistTimer = undefined;
      this.tokens
        .clear()
        .catch(err => console.error(`Failed to clear resume token for ${this.label}:`, err))
        .finally(() => this.scheduleReopen({}, this.reopenDelayMs));
      return;
    }

    // A failure that repeats (a view, a permissions problem) parks this collection on the
    // capped backoff; the other collections keep running.
    const delayMs = this.nextDelay();
    if (isTransientError(error)) console.warn(`changeStream error on ${this.label}, reopening in ${delayMs}ms: ${error}`);
    else console.error(`changeStream error on ${this.label} that a reopen is not expected to fix, reopening in ${delayMs}ms: ${error}`);
    this.scheduleReopen(start, delayMs);
  }

  private nextDelay() {
    return reopenDelay(++this.retries, this.reopenDelayMs, this.maxReopenDelayMs);
  }

  private scheduleReopen(start: StartOptions, delayMs: number, mode: StreamMode = 'normal') {
    if (this.state === 'closed') return;
    clearTimeout(this.reopenTimer);
    this.reopenTimer = setTimeout(() => {
      if (this.state === 'closed') return;
      this.openStream(start, mode).catch(err => {
        console.error(`Failed to reopen collection stream on ${this.label}:`, err);
        this.scheduleReopen(start, this.nextDelay());
      });
    }, delayMs);
  }

  private schedulePersist() {
    if (this.persistTimer || this.state === 'closed') return;
    this.persistTimer = setTimeout(() => {
      this.persistTimer = undefined;
      if (this.state === 'closed') return;
      this.persistToken().catch(err => console.error(`Failed to persist resume token for ${this.label}:`, err));
    }, PERSIST_INTERVAL_MS);
    this.persistTimer.unref();
  }

  // A stream whose events have all settled saves the server's position, which keeps moving while the collection
  // is quiet, so a restart does not resume from a token the oplog has since dropped.
  private scheduleQuietSave() {
    this.quietTimer = setTimeout(() => {
      if (this.state === 'closed') return;
      if (this.readingQuietly) {
        this.persistToken().catch(err => console.error(`Failed to persist resume token for ${this.label}:`, err));
      }
      this.scheduleQuietSave();
    }, this.quietSaveMs);
    this.quietTimer.unref();
  }

  private get readingQuietly() {
    return this.stream !== undefined && this.mode === 'normal' && this.unsettled.size === 0 && this.queued === 0 && this.walks.length === 0;
  }

  private persistToken() {
    let earliest: { seq: number; tokenBefore: unknown } | undefined;
    for (const entry of this.unsettled) if (!earliest || entry.seq < earliest.seq) earliest = entry;
    const token = earliest
      ? earliest.tokenBefore
      : ((this.readingQuietly ? this.stream!.resumeToken : this.closedToken) ?? this.lastToken);
    if (token === undefined) return Promise.resolve();
    const resumeToken = JSON.stringify(token);
    if (resumeToken === this.persistedToken) return Promise.resolve();
    const write = this.tokens
      .save(resumeToken)
      .then(() => {
        this.persistedToken = resumeToken;
      })
      .finally(() => {
        if (this.pendingPersist === write) this.pendingPersist = undefined;
      });
    this.pendingPersist = write;
    return write;
  }
}
