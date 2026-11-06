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
export function waitForStreamInit(stream: mongoose.mongo.ChangeStream<AnyObject>, timeoutMs = 10 * 1000) {
  return new Promise<void>(resolve => {
    const done = () => {
      clearTimeout(timer);
      stream.off('resumeTokenChanged', done);
      stream.off('error', done);
      resolve();
    };
    const timer = setTimeout(done, timeoutMs);
    stream.once('resumeTokenChanged', done);
    stream.once('error', done);
  });
}

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

  constructor(
    readonly collectionName: string,
    private deliver: DeliverFn,
    private onFatal: (error: unknown) => void = () => process.exit(1)
  ) {}

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
    await waitForStreamInit(stream);
  }

  private handleChange(change: ChangeEvent) {
    this.retries = 0;
    this.lastToken = change._id;
    this.schedulePersist();

    const normalized = normalizeBson(change);
    const now = Date.now();
    this.buffer.push({ change, normalized, receivedAt: now });
    while (this.buffer.length > BUFFER_MAX_EVENTS || (this.buffer[0] && now - this.buffer[0].receivedAt > BUFFER_MAX_AGE_MS)) {
      this.buffer.shift();
    }

    for (const [subscriptionId, registration] of this.registrations) {
      this.evaluate(subscriptionId, registration, change, normalized);
    }
  }

  private replay(subscriptionId: string, registration: Registration) {
    const updatedAt = registration.subscription.updatedAt?.getTime();
    if (updatedAt === undefined) return;
    const fromSeconds = Math.floor((updatedAt - 1000) / 1000);

    for (const { change, normalized } of this.buffer) {
      const clusterTime = (change as { clusterTime?: mongoose.mongo.Timestamp }).clusterTime;
      if (clusterTime && clusterTime.getHighBits() >= fromSeconds) {
        this.evaluate(subscriptionId, registration, change, normalized);
      }
    }
  }

  private evaluate(subscriptionId: string, registration: Registration, change: ChangeEvent, normalized: unknown) {
    let matched: boolean;
    try {
      matched = registration.matcher(normalized);
    } catch (error) {
      console.warn(`matcher failed for subscription ${subscriptionId} on ${this.collectionName}: ${error}`);
      return;
    }
    if (!matched) return;

    const fullDocument = (change as { fullDocument?: AnyObject }).fullDocument;
    const projected = fullDocument ? { ...change, fullDocument: registration.projector(fullDocument) } : change;
    this.deliver(subscriptionId, projected as ChangeEvent);
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
      clearTimeout(this.persistTimer);
      this.persistTimer = undefined;
      FanoutCursor.deleteOne({ collectionName: this.collectionName })
        .catch(err => console.error(`Failed to clear resume token for ${this.collectionName}:`, err))
        .finally(() => this.scheduleReopen({}, REOPEN_DELAY_MS));
      return;
    }

    this.retries++;
    if (this.retries > MAX_REOPEN_RETRIES) {
      console.error(`collection stream on ${this.collectionName} failed ${MAX_REOPEN_RETRIES} reopen attempts, exiting`);
      this.onFatal(error);
      return;
    }
    const start = this.lastToken ? { startAfter: this.lastToken } : this.initialStart;
    this.scheduleReopen(start, REOPEN_DELAY_MS * 2 ** (this.retries - 1));
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
    if (this.persistTimer) return;
    this.persistTimer = setTimeout(() => {
      this.persistTimer = undefined;
      this.persistToken().catch(err => console.error(`Failed to persist resume token for ${this.collectionName}:`, err));
    }, PERSIST_INTERVAL_MS);
    this.persistTimer.unref();
  }

  private async persistToken() {
    if (this.lastToken === undefined) return;
    const resumeToken = JSON.stringify(this.lastToken);
    if (resumeToken === this.persistedToken) return;
    await FanoutCursor.updateOne({ collectionName: this.collectionName }, { $set: { resumeToken } }, { upsert: true });
    this.persistedToken = resumeToken;
  }
}
