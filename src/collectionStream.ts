import mongoose, { AnyObject } from 'mongoose';
import { FanoutCursor } from './fanoutCursor.js';
import { Matcher, Projector, normalizeBson } from './matcher.js';
import { IQuerySubscriptionDocument } from './querySubscription.js';

export type ChangeEvent = mongoose.mongo.ChangeStreamDocument<AnyObject>;
export type DeliverFn = (subscriptionId: string, change: ChangeEvent) => unknown;

export type Registration = {
  matcher: Matcher;
  projector: Projector;
  subscription: IQuerySubscriptionDocument;
};

type StartOptions = { startAfter: unknown } | { startAtOperationTime: mongoose.mongo.Timestamp } | Record<string, never>;

type BufferedEvent = { change: ChangeEvent; normalized: unknown; receivedAt: number };

function clusterSeconds(change: ChangeEvent) {
  return (change as { clusterTime?: mongoose.mongo.Timestamp }).clusterTime?.getHighBits();
}

const BUFFER_MAX_EVENTS = 500;
const BUFFER_MAX_AGE_MS = 30 * 1000;
const PERSIST_INTERVAL_MS = 5 * 1000;
const REOPEN_DELAY_MS = 1000;
const MAX_REOPEN_RETRIES = 5;

export function isResumeExpiredError(error: { message?: string }) {
  return (
    error.message?.includes('Resume of change stream was not possible') ||
    error.message?.includes('resume point may no longer be in the oplog') ||
    error.message?.includes('the resume token was not found')
  );
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

export type CollectionStreamOptions = {
  onFatal?: (error: unknown) => void;
  reopenDelayMs?: number;
};

export class CollectionStream {
  private registrations: Map<string, Registration> = new Map();
  private buffer: BufferedEvent[] = [];
  private stream?: mongoose.mongo.ChangeStream<AnyObject>;
  private state: 'idle' | 'opening' | 'open' | 'closed' = 'idle';
  private initialStart: StartOptions = {};
  private lastToken?: unknown;
  private persistedToken?: string;
  private persistTimer?: NodeJS.Timeout;
  private reopenTimer?: NodeJS.Timeout;
  private retries = 0;
  private pendingPersist?: Promise<void>;
  // clusterTime seconds of the newest event dropped from the replay buffer.
  private evictedThroughSeconds?: number;
  private readonly onFatal: (error: unknown) => void;
  private readonly reopenDelayMs: number;

  constructor(
    readonly collectionName: string,
    private deliver: DeliverFn,
    { onFatal = () => process.exit(1), reopenDelayMs = REOPEN_DELAY_MS }: CollectionStreamOptions = {}
  ) {
    this.onFatal = onFatal;
    this.reopenDelayMs = reopenDelayMs;
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

  register(subscriptionId: string, registration: Registration) {
    this.registrations.set(subscriptionId, registration);
    if (this.state === 'open') {
      this.replay(subscriptionId, registration);
    }
  }

  unregister(subscriptionId: string) {
    this.registrations.delete(subscriptionId);
    return this.registrations.size;
  }

  async open() {
    if (this.state !== 'idle') return;
    this.state = 'opening';

    const cursor = await FanoutCursor.findOne({ collectionName: this.collectionName }).lean();
    if (this.state !== 'opening') return;

    if (cursor?.resumeToken) {
      this.persistedToken = cursor.resumeToken;
      this.initialStart = { startAfter: JSON.parse(cursor.resumeToken) };
    } else {
      const oldest = Math.min(
        ...[...this.registrations.values()].map(r => r.subscription.updatedAt?.getTime() ?? Infinity)
      );
      this.initialStart = Number.isFinite(oldest)
        ? { startAtOperationTime: new mongoose.mongo.Timestamp({ t: Math.floor(oldest / 1000), i: 1 }) }
        : {};
    }

    this.state = 'open';
    await this.openStream(this.initialStart);
  }

  async close(reason: 'idle' | 'shutdown') {
    if (this.state === 'closed') return;
    const wasOpen = this.state !== 'idle';
    this.state = 'closed';
    clearTimeout(this.reopenTimer);
    clearTimeout(this.persistTimer);
    this.persistTimer = undefined;

    const stream = this.stream;
    this.stream = undefined;
    await stream?.close().catch(() => null);
    this.buffer = [];
    // A token write already in flight must land before the cursor is deleted,
    // or it would bring the cursor back.
    await this.pendingPersist?.catch(() => null);

    if (reason === 'shutdown') {
      await this.persistToken();
    } else {
      // Nobody is listening any more, so a later first subscriber should start
      // from its own updatedAt instead of replaying everything since now.
      await FanoutCursor.deleteOne({ collectionName: this.collectionName });
    }

    if (wasOpen) {
      console.info(`collection stream closed on ${this.collectionName} (${this.registrations.size} subscriptions)`);
    }
  }

  private async openStream(start: StartOptions) {
    const collection = mongoose.connection.collection(this.collectionName);
    const stream = collection.watch<AnyObject, ChangeEvent>([], { fullDocument: 'updateLookup', ...start });
    this.stream = stream;
    stream.on('change', change => this.handleChange(change));
    stream.on('error', error => this.handleError(stream, error));

    const startDescription =
      'startAfter' in start ? 'startAfter' : 'startAtOperationTime' in start ? 'startAtOperationTime' : 'live';
    console.info(
      `collection stream open on ${this.collectionName} (${this.registrations.size} subscriptions, ${startDescription})`
    );
    // A reopen that initialises cleanly starts a fresh retry budget, so errors
    // spread over the life of a quiet stream do not add up to a fatal exit.
    if ((await waitForStreamInit(stream)) && stream === this.stream) {
      this.retries = 0;
    }
  }

  private handleChange(change: ChangeEvent) {
    if (this.state === 'closed') return;
    this.retries = 0;
    this.lastToken = change._id;
    this.schedulePersist();

    // A drop, rename or dropDatabase ends the stream with an invalidate event and
    // the driver closes the cursor without an error. Start a new stream right
    // after it so later subscribers on this collection keep receiving changes.
    if (change.operationType === 'invalidate') {
      console.warn(`collection stream on ${this.collectionName} invalidated, reopening`);
      const stream = this.stream;
      this.stream = undefined;
      stream?.close().catch(() => null);
      this.scheduleReopen({ startAfter: change._id }, this.reopenDelayMs);
      return;
    }

    const normalized = normalizeBson(change);
    const now = Date.now();
    this.buffer.push({ change, normalized, receivedAt: now });
    this.trimBuffer(now);

    for (const [subscriptionId, registration] of this.registrations) {
      this.evaluate(subscriptionId, registration, change, normalized);
    }
  }

  private trimBuffer(now: number) {
    while (this.buffer.length > BUFFER_MAX_EVENTS || (this.buffer[0] && now - this.buffer[0].receivedAt > BUFFER_MAX_AGE_MS)) {
      const evicted = this.buffer.shift();
      const seconds = evicted && clusterSeconds(evicted.change);
      if (seconds !== undefined) this.evictedThroughSeconds = seconds;
    }
  }

  private replay(subscriptionId: string, registration: Registration) {
    const updatedAt = registration.subscription.updatedAt?.getTime();
    if (updatedAt === undefined) return;
    const fromSeconds = Math.floor((updatedAt - 1000) / 1000);
    this.trimBuffer(Date.now());

    if (this.evictedThroughSeconds !== undefined && this.evictedThroughSeconds >= fromSeconds) {
      console.warn(
        `subscription ${subscriptionId} on ${this.collectionName} joined after its replay window was evicted; changes since its updatedAt may be missing`
      );
    }

    for (const { change, normalized } of this.buffer) {
      const seconds = clusterSeconds(change);
      if (seconds !== undefined && seconds >= fromSeconds) {
        this.evaluate(subscriptionId, registration, change, normalized);
      }
    }
  }

  // One subscription's bad filter or projection must not stop the event reaching the rest.
  private evaluate(subscriptionId: string, registration: Registration, change: ChangeEvent, normalized: unknown) {
    let projected: ChangeEvent;
    try {
      if (!registration.matcher(normalized)) return;
      const fullDocument = (change as { fullDocument?: AnyObject }).fullDocument;
      projected = (fullDocument ? { ...change, fullDocument: registration.projector(fullDocument) } : change) as ChangeEvent;
    } catch (error) {
      console.warn(`filter failed for subscription ${subscriptionId} on ${this.collectionName}: ${error}`);
      return;
    }
    this.deliver(subscriptionId, projected);
  }

  private handleError(stream: mongoose.mongo.ChangeStream<AnyObject>, error: Error) {
    if (this.state === 'closed' || stream !== this.stream) return;
    console.error(`changeStream error on collection ${this.collectionName}: ${error}`);
    this.stream = undefined;
    stream.close().catch(() => null);

    if (isResumeExpiredError(error)) {
      console.log(`Resume token expired for collection ${this.collectionName}, clearing and reopening live...`);
      this.lastToken = undefined;
      this.persistedToken = undefined;
      // The expired position must not be retried by a later reopen either.
      this.initialStart = {};
      clearTimeout(this.persistTimer);
      this.persistTimer = undefined;
      FanoutCursor.deleteOne({ collectionName: this.collectionName })
        .catch(err => console.error(`Failed to clear resume token for ${this.collectionName}:`, err))
        .finally(() => this.scheduleReopen({}, this.reopenDelayMs));
      return;
    }

    this.retries++;
    if (this.retries > MAX_REOPEN_RETRIES) {
      console.error(`collection stream on ${this.collectionName} failed ${MAX_REOPEN_RETRIES} reopen attempts, exiting`);
      this.onFatal(error);
      return;
    }
    const start = this.lastToken ? { startAfter: this.lastToken } : this.initialStart;
    this.scheduleReopen(start, this.reopenDelayMs * 2 ** (this.retries - 1));
  }

  private scheduleReopen(start: StartOptions, delayMs: number) {
    if (this.state === 'closed') return;
    this.reopenTimer = setTimeout(() => {
      if (this.state === 'closed') return;
      this.openStream(start).catch(err => {
        console.error(`Failed to reopen collection stream on ${this.collectionName}:`, err);
      });
    }, delayMs);
  }

  private schedulePersist() {
    if (this.persistTimer || this.state === 'closed') return;
    this.persistTimer = setTimeout(() => {
      this.persistTimer = undefined;
      if (this.state === 'closed') return;
      this.persistToken().catch(err => console.error(`Failed to persist resume token for ${this.collectionName}:`, err));
    }, PERSIST_INTERVAL_MS);
    this.persistTimer.unref();
  }

  private persistToken() {
    if (this.lastToken === undefined) return Promise.resolve();
    const resumeToken = JSON.stringify(this.lastToken);
    if (resumeToken === this.persistedToken) return Promise.resolve();
    const write = FanoutCursor.updateOne({ collectionName: this.collectionName }, { $set: { resumeToken } }, { upsert: true })
      .exec()
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
