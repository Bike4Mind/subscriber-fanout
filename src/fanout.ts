import { describeConnectError, describeMongoUri } from './mongoUri.js';
import mongoose, { AnyObject } from 'mongoose';
import { IQuerySubscriptionDocument, QuerySubscription } from './querySubscription.js';
import { credentialProviderChain } from './websocket.js';
import { IMessageDataToClient } from './wireTypes.js';
import { flatten } from 'flat';
import { pickBy } from 'lodash-es';
import {
  ApiGatewayManagementApiClient,
  DeleteConnectionCommand,
  PostToConnectionCommand,
} from '@aws-sdk/client-apigatewaymanagementapi';
import { createHash } from 'node:crypto';
import http from 'node:http';
import https from 'node:https';
import v8 from 'node:v8';
import vm from 'node:vm';
import {
  ChangeEvent,
  CollectionStream,
  OpenGate,
  StreamEvent,
  WindowLost,
  isResumeExpiredError,
  reopenDelay,
  waitForStreamInit,
} from './collectionStream.js';
import { BUDGET_FLOOR, Budget, HeapUse, pauseEstimate, sizeBudget } from './budget.js';
import { PendingCloses, Tombstones } from './closes.js';
import { Bodies, Body } from './bodies.js';
import { Closed, DROPPED, Job, Message, RELEASES_PER_SLOT, Scheduler } from './scheduler.js';
import { sizeEvent } from './sizing.js';
import { cachingLookup, onConnected } from './connect.js';
import { CHUNK_DATA_SIZE, Frames, MAX_WS_PAYLOAD_BYTES, REQUEST_BYTES, messagePrefix, messageTail } from './wire.js';
import { FanoutCursor } from './fanoutCursor.js';
import {
  InvalidFilterError,
  NO_MATCH_FILTER,
  Projector,
  SubscriptionFilter,
  buildProjector,
  buildSubscriptionFilter,
  transformAndPrefixQuery,
} from './matcher.js';

const SEND_SLOTS = 64;
// A connection whose own backlog would take longer than this to send is closed, so its client reconnects
// and refetches.
const MAX_DRAIN_MS = 60 * 1000;
// From the connected socket to the answer. The SDK's own timeouts sit past the scheduler's, as backstops.
const SEND_TIMEOUT_MS = 5 * 1000;
const SDK_CONNECT_TIMEOUT_MS = 4 * 1000;
const SDK_TIMEOUT_MARGIN_MS = 4 * 1000;
const DELETE_SOCKETS = 16;
const DROP_LOG_MS = 10 * 1000;
const PROJECT_AFTER = 1000;
// Closes of joiners whose replay window is gone: a burst of this many, then one a second.
const WINDOW_CLOSE_BURST = 10;
// Paused streams are checked for resume this often.
const RESUME_CHECK_MS = 250;
// Once a stream has been paused this long, it resumes first and the others read under its line.
const HEAD_PAUSE_MS = 10 * 1000;
// A lookup gets maxTimeMS on the server and a slightly longer timer here, and is tried again after each of
// these; failing still, the change goes out with its ids only.
const LOOKUP_MAX_TIME_MS = 5 * 1000;
const LOOKUP_TIMEOUT_MS = 6 * 1000;
const LOOKUP_RETRIES_MS = [1000, 2000, 4000];
// removeSubscription goes on without a stream's close past this.
const IDLE_CLOSE_WAIT_MS = 10 * 1000;
// A direct WebSocket gateway (e.g. the self-host stack) has no frame cap and forwards frames whole; set
// WEBSOCKET_DISABLE_CHUNKING=true there so clients receive un-chunked frames.
const DISABLE_CHUNKING = process.env.WEBSOCKET_DISABLE_CHUNKING === 'true';
const PONG = Buffer.from(JSON.stringify({ action: 'pong' }));
// Leaves room to exit within 8s, under Docker's default 10s stop timeout, the shortest any consumer runs with.
export const SHUTDOWN_DEADLINE_MS = 7.5 * 1000;
// Every open change stream holds a pooled connection through each awaitData getMore, so the
// pool needs room above the stream count or every other query waits behind them.
const MAX_POOL_SIZE = 100;
const POOL_HEADROOM = 10;

type SubscriberType = IQuerySubscriptionDocument['subscribers'][0];

type DocumentChange = Extract<ChangeEvent, { operationType: 'insert' | 'update' | 'replace' | 'delete' }>;

// One change or ping to a subscription's connections.
type Delivery = Job & {
  targets: SubscriberType[];
  subscriptionId: string;
  // Absent for a ping.
  change?: DocumentChange;
  source?: StreamEvent;
  projector?: Projector;
  owner?: object;
  bodyKey?: string;
  body?: Body;
  done: () => void;
};

// One per change a dedicated stream looks up, shared by every dedicated stream that read the same oplog entry.
type Lookup = {
  key?: string;
  fetching?: Promise<void>;
  // Once fetched: the document, or null for the change's ids-only update.
  document?: AnyObject | null;
  heap: number;
  json: number;
  charged: boolean;
};

type Gateway = { post: ApiGatewayManagementApiClient; probation: ApiGatewayManagementApiClient; delete: ApiGatewayManagementApiClient };

export type SendFn = (connectionId: string, endpoint: string, message: IMessageDataToClient) => Promise<unknown>;

export type SubscriberFanoutOptions = {
  mongoUri: string;
  stage: string;
  managementEndpoint: string;
  send?: SendFn;
  // Set to 0 to disable the periodic ping (tests call pingAllSubscribers directly).
  pingIntervalMs?: number;
  buildFilter?: (subscription: IQuerySubscriptionDocument) => SubscriptionFilter;
  disconnect?: (connectionId: string, endpoint: string) => Promise<unknown>;
  // S, the POSTs in flight at once.
  sendConcurrency?: number;
  maxDrainMs?: number;
  maxQueuedBytes?: number;
  sendTimeoutMs?: number;
  maxPoolSize?: number;
};

export class SubscriberFanout {
  private openGate = new OpenGate();
  private heapUse?: HeapUse;
  private shortfallLogged = false;
  // The clients for each API Gateway endpoint; there is normally one.
  private gateways: Map<string, Gateway> = new Map();
  private lookup = cachingLookup();

  // Map of query ID (hash of query) to Subscriber info (ISubscription.subscribers)
  subscribers: Map<string, Array<SubscriberType>> = new Map();
  // One shared change stream per watched collection
  private streamsByCollection: Map<string, CollectionStream> = new Map();
  // Map of query ID to the collection whose shared stream it is registered on
  private subscriptionCollections: Map<string, string> = new Map();
  // Subscriptions whose query cannot be evaluated in process keep their own stream
  private dedicatedStreams: Map<string, CollectionStream> = new Map();
  // Emptied subscriptions whose conditional delete has gone out; no other is sent until they fill again.
  private emptied: Set<string> = new Set();
  private closes: PendingCloses;
  private tombstones = new Tombstones();
  // The subscriptions each connection is listed on, by `endpoint connectionId`.
  private byConnection: Map<string, Set<string>> = new Map();
  // Per subscription, the entries whose replay is still to be sent; they get nothing else until then.
  private walking: Map<string, Set<string>> = new Map();
  // Streams whose next change waits at the match gate.
  private matchWaiting: Set<CollectionStream> = new Set();
  private drops = 0;

  // The fields of each subscription as JSON, which with the event names its shared message body.
  private projectionKeys: Map<string, string> = new Map();
  // What each subscription matches and sends, to tell a changed subscription from a reloaded one.
  private definitions: Map<string, string> = new Map();
  private lookups: WeakMap<StreamEvent, Lookup> = new WeakMap();
  private sharedLookups: Map<string, Lookup> = new Map();
  // Collections whose lookups keep failing: each change gets one try until one succeeds.
  private lookupsFailing: Set<string> = new Set();
  // Streams paused because a document they looked up did not fit; it is fetched again on resume.
  private refetch: Set<CollectionStream> = new Set();
  private windowCloses = WINDOW_CLOSE_BURST;
  private windowClosesAt = Date.now();
  private windowQueue: Closed[] = [];
  private windowQueueHead = 0;
  private windowTimer?: NodeJS.Timeout;
  private resumeTimer?: NodeJS.Timeout;
  private subscriptionWatcher?: mongoose.mongo.ChangeStream<IQuerySubscriptionDocument>;
  private watcherToken?: unknown;
  private watcherStartTime?: mongoose.mongo.Timestamp;
  private watcherRetries = 0;
  private watcherTimer?: NodeJS.Timeout;
  private pingTimer?: NodeJS.Timeout;
  private stopping = false;
  private poolWarned = false;
  // Idle closes still deleting their cursor; stop() waits for them before disconnecting.
  private idleCloses: Set<Promise<void>> = new Set();

  // Subscribers whose stored endpoint no longer matches are stale and get dropped.
  private managementEndpoint: string;
  private disconnect: (connectionId: string, endpoint: string) => Promise<unknown>;
  private slots: number;
  private budget: Budget;
  private bodies: Bodies;
  private scheduler: Scheduler<Delivery>;
  private buildFilter: (subscription: IQuerySubscriptionDocument) => SubscriptionFilter;

  constructor(private options: SubscriberFanoutOptions) {
    this.managementEndpoint = options.managementEndpoint;
    this.disconnect =
      options.disconnect ??
      (async (connectionId, endpoint) =>
        this.gateway(endpoint).delete.send(new DeleteConnectionCommand({ ConnectionId: connectionId })));
    this.closes = new PendingCloses(
      {
        delete: (connectionId, endpoint) => this.disconnect(connectionId, endpoint),
        closed: (endpoint, connectionId) => this.forgetConnection(endpoint, connectionId),
        forbidden: (endpoint, connectionId) => this.scheduler.forbidden(endpoint, connectionId),
        expired: error => this.handleDeliveryError(error, 'closing a connection'),
      },
      this.tombstones
    );
    this.slots = options.sendConcurrency ?? SEND_SLOTS;
    this.budget = new Budget(options.maxQueuedBytes ?? BUDGET_FLOOR);
    this.budget.onFall = stream => this.wakeMatching(stream as CollectionStream | undefined);
    this.bodies = new Bodies(this.budget);
    this.scheduler = new Scheduler(
      this.budget,
      {
        slots: this.slots,
        requestTimeoutMs: options.sendTimeoutMs ?? SEND_TIMEOUT_MS,
        maxDrainMs: options.maxDrainMs ?? MAX_DRAIN_MS,
      },
      {
        open: (delivery, index) => this.openDelivery(delivery, index),
        settle: (delivery, index, result) => this.settleDelivery(delivery, index, result),
        close: closed => this.closeMany(closed),
        gone: (endpoint, connectionId) => {
          console.warn(`connection ${connectionId} is gone, removing its subscriptions`);
          void this.tombstones.add(endpoint, connectionId, 'gone');
          this.forgetConnection(endpoint, connectionId);
        },
        extra: delivery => (delivery.body?.refs === 1 ? delivery.body.tail.length : 0),
        check: () => this.enforce(),
        roomWaitMs: () => this.bodies.waitMs(),
        blocked: key => this.closes.blocked(key) || this.tombstones.has(key),
        once: (delivery, index) => !delivery.change || delivery.targets[index].endpoint !== this.managementEndpoint,
        expired: error => this.handleDeliveryError(error, 'a send'),
      }
    );
    this.buildFilter = options.buildFilter ?? buildSubscriptionFilter;
    console.info(`SubscriberFanout: ${options.stage} starting`);
  }

  async start() {
    const url = this.options.mongoUri.replace('%STAGE%', this.options.stage).replace('localhost', '127.0.0.1');
    const target = describeMongoUri(url);

    console.debug(`connecting to MongoDB at ${target}`);
    try {
      await mongoose.connect(url, { maxPoolSize: this.options.maxPoolSize ?? MAX_POOL_SIZE });
    } catch (error) {
      throw new Error(`failed to connect to MongoDB at ${target} (${describeConnectError(error, url)})`);
    }

    console.debug(`connected to MongoDB`);
    await Promise.all([this.closes.load(), this.tombstones.load()]);
    console.info(`${this.closes.size} connection closes unconfirmed, ${this.tombstones.size} connections removed in the last 2h`);
    // Credentials looked up at the first send would stall the first deliveries for as long as the lookup takes.
    if (!this.options.send) {
      const clients = Object.values(this.gateway(this.managementEndpoint));
      await Promise.all(clients.map(client => client.config.credentials())).catch(() => {});
    }

    const total = await QuerySubscription.estimatedDocumentCount();
    collectGarbage();
    const before = v8.getHeapStatistics().used_heap_size;
    await this.resync(true, loaded => loaded === PROJECT_AFTER && total > loaded && this.projectLoad(before, loaded, total));
    collectGarbage();
    const base = v8.getHeapStatistics().used_heap_size;
    const startCount = this.subscriptionCount();
    this.heapUse = {
      heapLimit: v8.getHeapStatistics().heap_size_limit,
      base,
      startCount,
      perSubscription: startCount > 0 ? Math.max(0, (base - before) / startCount) : 0,
      count: startCount,
    };
    this.sizeBudget(true);
    await FanoutCursor.deleteMany({ collectionName: { $nin: [...this.streamsByCollection.keys()] } });
    await Promise.all(this.allStreams().map(stream => stream.open()));

    await waitForStreamInit(this.watchSubscriptions());
    this.checkPoolHeadroom();

    const pingIntervalMs = this.options.pingIntervalMs ?? 60 * 1000; // minutely
    if (pingIntervalMs > 0) {
      this.pingTimer = setInterval(() => {
        this.pingAllSubscribers().catch(error => this.handleDeliveryError(error, 'ping'));
      }, pingIntervalMs);
    }
  }

  private watchSubscriptions() {
    const start = this.watcherToken
      ? { startAfter: this.watcherToken }
      : this.watcherStartTime && { startAtOperationTime: this.watcherStartTime };
    const changeStream = QuerySubscription.watch([], { fullDocument: 'updateLookup', ...start });
    changeStream.on('change', change => {
      if (this.stopping) return;
      // A drop or rename of querySubscriptions, or a dropDatabase.
      if (change.operationType === 'invalidate') {
        console.warn('querySubscriptions watcher invalidated, reloading every subscription');
        this.subscriptionWatcher = undefined;
        changeStream.close();
        void this.resyncWatcher();
        return;
      }
      this.handleSubscriptionChange(change)
        .catch(error => console.error(`failed to apply a ${change.operationType} on querySubscriptions: ${error}`))
        .finally(() => this.checkPoolHeadroom());
    });
    changeStream.on('resumeTokenChanged', token => {
      this.watcherToken = token;
    });
    changeStream.on('error', error => this.handleWatcherError(changeStream, error));
    this.subscriptionWatcher = changeStream;
    return changeStream;
  }

  private checkPoolHeadroom() {
    const maxPoolSize = this.options.maxPoolSize ?? MAX_POOL_SIZE;
    const streams = this.streamsByCollection.size + this.dedicatedStreams.size + 1;
    const tight = streams > maxPoolSize - POOL_HEADROOM;
    if (tight && !this.poolWarned) {
      console.warn(`${streams} change streams open on a pool of ${maxPoolSize}; raise MONGODB_MAX_POOL_SIZE`);
    }
    this.poolWarned = tight;
  }

  private handleWatcherError(changeStream: mongoose.mongo.ChangeStream<IQuerySubscriptionDocument>, error: Error) {
    if (this.stopping || changeStream !== this.subscriptionWatcher) return;
    changeStream.close();
    const lost = isResumeExpiredError(error);
    const delayMs = reopenDelay(++this.watcherRetries);
    console.error(`querySubscriptions watcher error, ${lost ? 'reloading every subscription' : 'reopening'} in ${delayMs}ms: ${error}`);
    this.watcherTimer = setTimeout(() => (lost ? void this.resyncWatcher() : this.reopenWatcher()), delayMs);
  }

  private reopenWatcher() {
    const reopened = this.watchSubscriptions();
    void waitForStreamInit(reopened).then(initialised => {
      if (initialised && reopened === this.subscriptionWatcher) this.watcherRetries = 0;
    });
  }

  private async resyncWatcher() {
    if (this.stopping) return;
    try {
      await this.resync();
      if (!this.stopping) this.reopenWatcher();
    } catch (error) {
      const delayMs = reopenDelay(++this.watcherRetries);
      console.error(`failed to reload subscriptions, retrying in ${delayMs}ms: ${error}`);
      this.watcherTimer = setTimeout(() => void this.resyncWatcher(), delayMs);
    }
  }

  // Loads every subscription and reconciles what is registered with it, then sets the watcher to start from
  // before the load, so a change made during it is not missed. Startup, an invalidate and a lost resume
  // position all take this path.
  private async resync(startup = false, onLoaded?: (loaded: number) => void) {
    const operationTime = (await mongoose.connection.db.command({ ping: 1 })).operationTime;
    const loaded = new Set<string>();
    // Plain documents, like the ones built from change events, so a subscriber's fields are only what is stored.
    for await (const document of QuerySubscription.find().lean().cursor()) {
      const subscription = { ...document, id: String(document._id) } as unknown as IQuerySubscriptionDocument;
      loaded.add(subscription.id);
      const registered = this.subscriptionCollections.has(subscription.id) || this.dedicatedStreams.has(subscription.id);
      if (startup || !registered || this.definitions.get(subscription.id) !== definitionOf(subscription)) {
        await this.addSubscription(subscription, { deferOpen: startup });
      } else await this.updateSubscription(subscription);
      onLoaded?.(loaded.size);
    }
    for (const id of [...this.subscriptionCollections.keys(), ...this.dedicatedStreams.keys()]) {
      if (!loaded.has(id)) await this.deleteSubscription(id);
    }
    this.watcherStartTime = operationTime;
    this.watcherToken = undefined;
  }

  // Bounded by deadlineMs whether or not Mongo answers: sends still in flight get the first
  // three quarters of it, and the resume tokens are written in what is left.
  async stop(deadlineMs = SHUTDOWN_DEADLINE_MS) {
    this.stopping = true;
    clearInterval(this.pingTimer);
    clearTimeout(this.watcherTimer);
    clearInterval(this.resumeTimer);
    clearTimeout(this.windowTimer);
    this.scheduler.stop();
    this.closes.stop();
    this.subscriptionWatcher?.close();
    const streams = this.allStreams();
    if (mongoose.connection.readyState !== mongoose.ConnectionStates.connected) {
      // Nothing can be saved without Mongo. Closing the client stops its reconnect attempts, whose
      // DNS lookups would otherwise hold the process open at exit until they time out.
      console.warn('Mongo is unreachable, stopping without saving resume tokens');
      for (const stream of streams) void stream.close('shutdown').catch(() => null);
      void mongoose.connection
        .getClient()
        .close(true)
        .catch(() => null);
      this.scheduler.end();
      this.clearState();
      return;
    }
    const drainUntil = Date.now() + deadlineMs * 0.75;
    let timedOut = false;
    const shutdown = (async () => {
      await Promise.all(
        streams.map(stream =>
          stream
            .close('shutdown', drainUntil)
            .catch(error => console.error(`failed to save the resume token for ${stream.label}: ${error}`))
        )
      );
      await Promise.all([...this.idleCloses].map(closing => closing.catch(() => null)));
      if (!timedOut) await mongoose.disconnect();
    })().then(
      () => true,
      error => {
        console.error('shutdown failed:', error);
        return true;
      }
    );
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<boolean>(resolve => (timer = setTimeout(resolve, deadlineMs, false)));
    const finished = await Promise.race([shutdown, deadline]);
    clearTimeout(timer);
    if (!finished) {
      timedOut = true;
      console.warn(`shutdown did not finish within ${deadlineMs}ms`);
    }
    this.scheduler.end();
    this.clearState();
  }

  private clearState() {
    this.streamsByCollection.clear();
    this.dedicatedStreams.clear();
    this.subscriptionCollections.clear();
    this.subscribers.clear();
    this.byConnection.clear();
    this.walking.clear();
    this.projectionKeys.clear();
    this.definitions.clear();
    this.sharedLookups.clear();
    this.refetch.clear();
    this.emptied.clear();
  }

  private allStreams() {
    return [...this.streamsByCollection.values(), ...this.dedicatedStreams.values()];
  }

  private streamOf(subscriptionId: string) {
    const collectionName = this.subscriptionCollections.get(subscriptionId);
    return this.dedicatedStreams.get(subscriptionId) ?? this.streamsByCollection.get(collectionName ?? '');
  }

  openStreamCount(collectionName?: string) {
    const streams = [...this.streamsByCollection.values()].filter(s => s.isOpen);
    return collectionName ? streams.filter(s => s.collectionName === collectionName).length : streams.length;
  }

  dedicatedStreamCount() {
    return this.dedicatedStreams.size;
  }

  async handleSubscriptionChange(change: mongoose.mongo.ChangeStreamDocument<IQuerySubscriptionDocument>) {
    switch (change.operationType) {
      case 'insert': {
        // Adding a new subscription
        console.debug(
          `subscription change: ${change.operationType} ${JSON.stringify(change.ns)} ${change.documentKey._id} ${JSON.stringify(
            change.fullDocument
          )}`
        );
        const id = change.documentKey._id.toString();
        // The watcher starts from before the startup load, which may already have read this subscription.
        if (this.subscriptionCollections.has(id) || this.dedicatedStreams.has(id)) break;
        await this.addSubscription({ ...change.fullDocument, id });
        break;
      }
      case 'update':
        console.debug(
          `subscription change: ${change.operationType} ${JSON.stringify(change.ns)} ${change.documentKey._id} ${JSON.stringify(
            change.updateDescription
          )}`
        );
        if (change.fullDocument) {
          await this.updateSubscription({ ...change.fullDocument, id: change.documentKey._id.toString() });
        }
        break;
      case 'delete':
        console.debug(
          `subscription change: ${change.operationType} ${JSON.stringify(change.ns)} ${JSON.stringify(
            change.documentKey
          )}`
        );
        await this.deleteSubscription(change.documentKey._id.toString());
        break;
    }
  }

  async addSubscription(subscription: IQuerySubscriptionDocument, { deferOpen = false } = {}) {
    console.info(
      `adding subscription for ${subscription.collectionName} query: ${JSON.stringify(
        subscription.query,
        (_key, value) => (Array.isArray(value) && value.length > 50 ? `[${value.length} items]` : value)
      )} fields ${JSON.stringify(subscription.fields)}, lastChange ${subscription.lastChange ?? 'null'}`
    );

    // Skip subscriptions with an invalid collection name.
    if (!subscription.collectionName || typeof subscription.collectionName !== 'string') {
      return;
    }

    if (this.subscriptionCollections.has(subscription.id) || this.dedicatedStreams.has(subscription.id)) {
      await this.removeSubscription(subscription.id);
    }
    this.setSubscribers(subscription.id, this.admit(subscription.id, subscription.subscribers));
    this.emptied.delete(subscription.id);
    this.projectionKeys.set(subscription.id, JSON.stringify(subscription.fields ?? {}));
    this.definitions.set(subscription.id, definitionOf(subscription));

    let filter: SubscriptionFilter;
    try {
      filter = this.buildFilter(subscription);
    } catch (error) {
      if (error instanceof InvalidFilterError) {
        console.warn(
          `subscription ${subscription.id} on ${subscription.collectionName} has a filter MongoDB rejects, delivering nothing: ${error.message}`
        );
        filter = NO_MATCH_FILTER;
      } else {
        console.warn(
          `subscription ${subscription.id} on ${subscription.collectionName} cannot be matched in process, using a dedicated stream: ${error}`
        );
        let dedicated: CollectionStream;
        try {
          dedicated = this.addDedicatedStream(subscription);
        } catch (error) {
          this.setSubscribers(subscription.id);
          this.projectionKeys.delete(subscription.id);
          console.error(`subscription ${subscription.id} on ${subscription.collectionName} skipped: ${error}`);
          return;
        }
        if (!deferOpen) await dedicated.open();
        return;
      }
    }

    let stream = this.streamsByCollection.get(subscription.collectionName);
    const isNew = !stream;
    if (!stream) {
      stream = new CollectionStream(subscription.collectionName, (...args) => this.deliverChange(...args), this.streamOptions());
      this.streamsByCollection.set(subscription.collectionName, stream);
    }
    const replay = stream.register(subscription.id, { ...filter, updatedAt: subscription.updatedAt });
    if (replay && replay !== 'queued') this.windowLost(replay, this.subscribers.get(subscription.id) ?? []);
    this.subscriptionCollections.set(subscription.id, subscription.collectionName);
    this.resizeBudget();

    if (isNew && !deferOpen) {
      await stream.open();
    }
  }

  addDedicatedStream(subscription: IQuerySubscriptionDocument) {
    const pipeline: Array<Record<string, unknown>> = [
      // Lets through the invalidate after a drop or rename, so the stream reopens past it.
      { $match: { $or: [transformAndPrefixQuery(subscription.query, 'fullDocument'), { operationType: 'invalidate' }] } },
    ];
    // Only the filter needs the server: the document is dropped there and looked up before matching.
    let projector: Projector | undefined;
    try {
      projector = buildProjector(subscription.fields);
    } catch {
      projector = undefined;
    }
    if (projector) pipeline.push({ $project: { fullDocument: 0 } });
    else if (Object.keys(subscription.fields ?? {}).length > 0) {
      const fields: Record<string, number | boolean> = flatten({ fullDocument: subscription.fields });

      // Check for and process exclusions first, removing anything uninteresting to caller
      const exclusions = pickBy(fields, v => !v);
      if (Object.keys(exclusions).length > 0) {
        pipeline.push({
          $project: { ...exclusions },
        });
      }

      // Process inclusion rules: in this case we need to specifically include our fields of interest,
      // since the caller's inclusions would normally exclude them unless we're explicit
      const inclusions = pickBy(fields, v => !!v);
      if (Object.keys(inclusions).length > 0) {
        pipeline.push({
          $project: {
            ...inclusions,
            operationType: true,
            ns: true,
            documentKey: true,
            clusterTime: true,
          },
        });
      }
    }

    // Throws here on a token that can never be resumed from, so the subscription is skipped.
    if (subscription.lastChange) JSON.parse(subscription.lastChange);
    const stream = new CollectionStream(
      subscription.collectionName,
      (...args) => this.deliverChange(...args),
      {
        ...this.streamOptions(),
        prepare: projector ? event => this.prepare(event) : undefined,
        pipeline,
        label: `${subscription.collectionName} for subscription ${subscription.id}`,
        tokens: {
          load: async () => subscription.lastChange,
          save: async lastChange => QuerySubscription.updateOne({ _id: subscription.id }, { $set: { lastChange } }),
          clear: async () => QuerySubscription.updateOne({ _id: subscription.id }, { $unset: { lastChange: '' } }),
        },
      }
    );
    // The server has already filtered and projected every event the stream reads.
    stream.register(subscription.id, { matcher: () => true, projector: projector ?? (doc => doc), updatedAt: subscription.updatedAt });
    this.dedicatedStreams.set(subscription.id, stream);
    this.resizeBudget();
    return stream;
  }

  async updateSubscription(subscription: IQuerySubscriptionDocument) {
    // In this case, we've been told by mongo that the subscription list for a query
    // has changed, so we need to update our in-memory version of the subscribers.  It's
    // possible there are now zero subscribers, in which case we need to close the
    // active query.
    if (subscription.subscribers.length > 0) {
      if (!this.subscriptionCollections.has(subscription.id) && !this.dedicatedStreams.has(subscription.id)) {
        // Its last subscriber left and a new one arrived before the doc was deleted.
        await this.addSubscription(subscription);
        return;
      }
      const listed = this.admit(subscription.id, subscription.subscribers);
      const previous = this.subscribers.get(subscription.id) ?? [];
      if (previous.length !== listed.length) {
        console.info(`updating subscription ${subscription.id} (now ${listed.length} subscribers)`);
      }
      this.setSubscribers(subscription.id, listed);
      this.emptied.delete(subscription.id);
      // A subscriber pushed onto an existing doc fetched its initial results just before.
      const stream = this.streamOf(subscription.id);
      let walking = this.walking.get(subscription.id);
      if (walking) {
        // A joiner that has left gets no more of its replay.
        const keys = new Set(listed.map(entryKey));
        for (const key of walking) if (!keys.has(key)) walking.delete(key);
        if (walking.size === 0) {
          this.walking.delete(subscription.id);
          walking = undefined;
        }
      }
      const lost: Record<WindowLost, SubscriberType[]> = { evicted: [], trimmed: [] };
      const known = new Set(previous.map(entryKey));
      for (const joiner of listed.filter(s => !known.has(entryKey(s)))) {
        const key = entryKey(joiner);
        const set = walking ?? new Set<string>();
        const replay = stream?.replay(
          subscription.id,
          joiner.createdAt ?? subscription.updatedAt,
          (id, event, projector, settled) => {
            if (set.has(key)) return this.deliverChange(id, event, projector, settled, [joiner]);
            settled();
            return true;
          },
          () => {
            set.delete(key);
            if (set.size === 0 && this.walking.get(subscription.id) === set) this.walking.delete(subscription.id);
          }
        );
        if (replay === 'queued') {
          set.add(key);
          if (!walking) this.walking.set(subscription.id, (walking = set));
        } else if (replay) lost[replay].push(joiner);
      }
      this.windowLost('evicted', lost.evicted);
      this.windowLost('trimmed', lost.trimmed);
      return;
    }

    // else subscribers is empty, so we need to close the query
    const registered = this.subscriptionCollections.has(subscription.id) || this.dedicatedStreams.has(subscription.id);
    // Every pull that emptied the doc reports it empty, as the lookup reads the doc as it is now.
    if (!registered && this.emptied.has(subscription.id)) return;
    if (!registered) console.error(`no change stream for subscription ${subscription.id}`);
    this.emptied.add(subscription.id);
    await this.removeSubscription(subscription.id);
    // A subscriber added since keeps the doc, and its update re-registers the subscription.
    await this.deleteIfEmpty(subscription.id);
  }

  async deleteSubscription(subscriptionId: string) {
    console.info(`deleted subscription ${subscriptionId}`);
    this.emptied.delete(subscriptionId);
    await this.removeSubscription(subscriptionId);
  }

  private async deleteIfEmpty(subscriptionId: string) {
    await QuerySubscription.deleteOne({ _id: subscriptionId, subscribers: { $size: 0 } }).catch(error => {
      this.emptied.delete(subscriptionId);
      throw error;
    });
  }

  private async removeSubscription(subscriptionId: string) {
    this.setSubscribers(subscriptionId);
    this.walking.delete(subscriptionId);
    this.projectionKeys.delete(subscriptionId);
    this.definitions.delete(subscriptionId);

    const dedicated = this.dedicatedStreams.get(subscriptionId);
    if (dedicated) {
      this.dedicatedStreams.delete(subscriptionId);
      this.resizeBudget();
      await this.closeIdle(dedicated);
    }

    const collectionName = this.subscriptionCollections.get(subscriptionId);
    if (collectionName === undefined) return;
    this.subscriptionCollections.delete(subscriptionId);
    this.resizeBudget();

    const stream = this.streamsByCollection.get(collectionName);
    if (stream && stream.unregister(subscriptionId) === 0) {
      this.streamsByCollection.delete(collectionName);
      await this.closeIdle(stream);
    }
  }

  // Waits at most IDLE_CLOSE_WAIT_MS for the cursor write, which stop() still waits for.
  private async closeIdle(stream: CollectionStream) {
    this.refetch.delete(stream);
    for (const [key, lookup] of this.sharedLookups) if (!lookup.charged && !lookup.fetching) this.sharedLookups.delete(key);
    const closing = stream.close('idle').finally(() => this.idleCloses.delete(closing));
    this.idleCloses.add(closing);
    let timer: NodeJS.Timeout | undefined;
    const late = new Promise<boolean>(resolve => (timer = setTimeout(resolve, IDLE_CLOSE_WAIT_MS, true)));
    const slow = await Promise.race([closing.then(() => false), late]).finally(() => clearTimeout(timer));
    if (slow) console.warn(`closing the stream on ${stream.label} has taken over ${IDLE_CLOSE_WAIT_MS}ms, going on without it`);
  }

  private handleDeliveryError(error: unknown, context: string) {
    if (String(error).startsWith('ExpiredTokenException') || (error as { name?: string })?.name === 'ExpiredTokenException') {
      // Take a coordinated exit and let the service manager restart us with fresh credentials.
      console.error(`${context} failed with expired credentials, exiting: ${error}`);
      process.exit(1);
    }
    console.error(`${context} failed: ${error}`);
  }

  // Closes connections so their clients reconnect and refetch, with one log line per reason. Their subscriptions
  // go once the gateway confirms, so a client is never left connected but unsubscribed.
  private closeMany(closed: Closed[]) {
    const fresh: Closed[] = [];
    const seen = new Set<string>();
    for (const close of closed) {
      this.scheduler.close(close.endpoint, close.connectionId);
      const key = `${close.endpoint} ${close.connectionId}`;
      if (seen.has(key) || this.closes.blocked(key) || this.tombstones.has(key)) continue;
      seen.add(key);
      fresh.push(close);
    }
    if (fresh.length === 0) return;
    const byReason: Map<string, Closed[]> = new Map();
    for (const close of fresh) {
      const list = byReason.get(close.reason);
      if (list) list.push(close);
      else byReason.set(close.reason, [close]);
    }
    for (const [reason, [first, ...rest]] of byReason) {
      const detail = first.detail ? ` (${first.detail})` : '';
      const who = rest.length === 0 ? `connection ${first.connectionId}` : `${rest.length + 1} connections, ${first.connectionId} first`;
      console.warn(`closing ${who}: ${reason}${detail}`);
    }
    this.closes.add(fresh);
    this.dropConnections(fresh);
  }

  private closeConnection(endpoint: string, connectionId: string, reason: string) {
    this.closeMany([{ endpoint, connectionId, reason }]);
  }

  private forgetConnection(endpoint: string, connectionId: string) {
    this.dropConnections([{ endpoint, connectionId }]);
    this.pullConnection(endpoint, connectionId);
  }

  private pullConnection(endpoint: string, connectionId: string) {
    QuerySubscription.updateMany(
      { subscribers: { $elemMatch: { connectionId, endpoint } } },
      { $pull: { subscribers: { connectionId, endpoint } } }
    ).catch(error => console.error(`failed to remove the subscriptions of connection ${connectionId}: ${error}`));
  }

  // Takes connections off every in-memory list in one pass per subscription they are on.
  private dropConnections(connections: Array<{ endpoint: string; connectionId: string }>) {
    const keys = new Set(connections.map(({ endpoint, connectionId }) => `${endpoint} ${connectionId}`));
    const affected = new Set<string>();
    for (const key of keys) {
      const ids = this.byConnection.get(key);
      if (!ids) continue;
      this.byConnection.delete(key);
      for (const id of ids) affected.add(id);
    }
    for (const subscriptionId of affected) {
      const subscribers = this.subscribers.get(subscriptionId) ?? [];
      this.subscribers.set(subscriptionId, subscribers.filter(s => !keys.has(`${s.endpoint} ${s.connectionId}`)));
    }
  }

  private setSubscribers(subscriptionId: string, subscribers?: SubscriberType[]) {
    const before = this.subscribers.get(subscriptionId) ?? [];
    if (subscribers) this.subscribers.set(subscriptionId, subscribers);
    else this.subscribers.delete(subscriptionId);
    const keys = new Set(subscribers?.map(s => `${s.endpoint} ${s.connectionId}`));
    for (const s of before) {
      const key = `${s.endpoint} ${s.connectionId}`;
      if (keys.has(key)) continue;
      const ids = this.byConnection.get(key);
      if (ids?.delete(subscriptionId) && ids.size === 0) this.byConnection.delete(key);
    }
    for (const key of keys) {
      const ids = this.byConnection.get(key);
      if (ids) ids.add(subscriptionId);
      else this.byConnection.set(key, new Set([subscriptionId]));
    }
  }

  // An incoming list without the connections being closed or removed. One still naming a removed connection
  // has it pulled again, at most once a minute.
  private admit(subscriptionId: string, subscribers: SubscriberType[]) {
    if (this.closes.size === 0 && this.tombstones.size === 0) return subscribers;
    return subscribers.filter(({ endpoint, connectionId }) => {
      const key = `${endpoint} ${connectionId}`;
      if (this.closes.blocked(key)) return false;
      if (!this.tombstones.has(key)) return true;
      if (this.tombstones.repull(key)) {
        console.info(`subscription ${subscriptionId} still lists removed connection ${connectionId}, removing it again`);
        this.pullConnection(endpoint, connectionId);
      }
      return false;
    });
  }

  // The subscribers that get live messages: all but those whose replay is still to be sent.
  private live(subscriptionId: string, subscribers: SubscriberType[]) {
    const walking = this.walking.get(subscriptionId);
    return walking ? subscribers.filter(s => !walking.has(entryKey(s))) : subscribers;
  }

  private deliverChange(subscriptionId: string, event: StreamEvent, projector: Projector, settled: () => void, only?: SubscriberType[]) {
    const { change } = event;
    if (
      change.operationType !== 'update' &&
      change.operationType !== 'replace' &&
      change.operationType !== 'insert' &&
      change.operationType !== 'delete'
    ) {
      console.warn(`Unexpected operationType ${change.operationType ?? 'null'}: ${JSON.stringify(change)}`);
      settled();
      return true;
    }
    return this.enqueue(subscriptionId, { event, source: event, bytes: event.bytes, stream: event.stream, change, projector, done: settled }, only);
  }

  // Queues a message to a subscription's live connections, or only to some of them, with its body built now;
  // done runs once every send has settled. False, with nothing queued, while the body waits for room.
  private enqueue(
    subscriptionId: string,
    message: Pick<Delivery, 'event' | 'source' | 'bytes' | 'stream' | 'change' | 'projector' | 'ping' | 'done'>,
    only?: SubscriberType[]
  ) {
    const listed = this.subscribers.get(subscriptionId);
    const subscribers = only ?? (listed && this.live(subscriptionId, listed));
    if (!subscribers?.length) {
      // Registered without subscribers. Only the watcher unregisters, on the delete event, so
      // a subscriber that arrives meanwhile keeps both the doc and the registration.
      if (!only && !listed?.length && !this.emptied.has(subscriptionId)) {
        this.emptied.add(subscriptionId);
        this.deleteIfEmpty(subscriptionId).catch(error => console.error(`failed to delete empty subscription ${subscriptionId}: ${error}`));
      }
      message.done();
      return true;
    }
    const delivery: Delivery = { ...message, subscriptionId, targets: subscribers, pending: 0, posts: 1 };
    const event = message.source;
    if (event) {
      delivery.posts = this.estimatePosts(event.json);
      delivery.bodyKey = this.projectionKeys.get(subscriptionId) ?? '';
      delivery.owner = (message.change?.operationType !== 'delete' && this.lookups.get(event)) || event;
      const body = this.bodies.take(delivery.owner, delivery.bodyKey, event.json, event.stream, () => event.stream.wake(), () =>
        this.buildBody(delivery)
      );
      if (body === 'wait') return false;
      if (!body) {
        message.done();
        return true;
      }
      delivery.body = body;
    }
    this.scheduler.add(delivery);
    return true;
  }

  private estimatePosts(json: number) {
    if (DISABLE_CHUNKING || this.options.send || json + 100 <= MAX_WS_PAYLOAD_BYTES) return 1;
    return Math.ceil((json + 100) / CHUNK_DATA_SIZE);
  }

  private openDelivery(delivery: Delivery, index: number): Message {
    const subscriber = delivery.targets[index];
    return delivery.body ? this.message(subscriber, delivery.body.tail) : this.message(subscriber, PONG, true);
  }

  private lookupOf(event: StreamEvent) {
    let lookup = this.lookups.get(event);
    if (lookup) return lookup;
    const { ns, documentKey, clusterTime } = event.change as DocumentChange & { clusterTime?: unknown };
    const key = clusterTime ? `${ns.coll}|${mongoose.mongo.BSON.EJSON.stringify(documentKey._id)}|${clusterTime}` : undefined;
    lookup = key ? this.sharedLookups.get(key) : undefined;
    if (!lookup) {
      lookup = { key, heap: 0, json: 0, charged: false };
      if (key) this.sharedLookups.set(key, lookup);
    }
    this.lookups.set(event, lookup);
    return lookup;
  }

  // Gives a dedicated stream's head event its document before it is matched: one fetch per oplog entry, admitted
  // through the read gate like an event.
  private prepare(event: StreamEvent) {
    const change = event.change as DocumentChange;
    if (change.operationType === 'delete') return true;
    const stream = event.stream;
    if (this.refetch.has(stream)) return false;
    const lookup = this.lookupOf(event);
    if (lookup.document === undefined) {
      lookup.fetching ??= this.fetch(lookup, change).finally(() => (lookup.fetching = undefined));
      void lookup.fetching.then(() => stream.wake());
      return false;
    }
    const { document } = lookup;
    if (document && !lookup.charged) {
      if (!this.admitRead(stream, lookup.heap)) {
        this.forget(lookup);
        this.refetch.add(stream);
        stream.refuseLookup(Math.max(this.budget.limit / 8, lookup.heap));
        return false;
      }
      lookup.charged = this.budget.attach(event, lookup, lookup.heap, () => this.forget(lookup));
    } else if (document) this.budget.attach(event, lookup, lookup.heap);
    (event.change as { fullDocument?: AnyObject | null }).fullDocument = document;
    if (document) event.json = lookup.json;
    return true;
  }

  // Tries after 1s, 2s and 4s, then sends the ids only; while a collection's lookups keep failing, each change
  // gets one try.
  private async fetch(lookup: Lookup, { ns, documentKey }: DocumentChange) {
    const retries = this.lookupsFailing.has(ns.coll) ? [] : LOOKUP_RETRIES_MS;
    const described = `a change on ${ns.coll} to ${documentKey._id}`;
    for (let attempt = 0; ; attempt++) {
      try {
        const document = await this.findOne(ns.coll, documentKey._id);
        this.lookupsFailing.delete(ns.coll);
        const size = document ? sizeEvent(document) : undefined;
        if (document && size!.heap + size!.json > this.budget.limit) {
          console.warn(
            `sending the ids of ${described} without its document: it needs about ${size!.heap + size!.json} bytes, over the ${this.budget.limit} byte budget`
          );
        }
        if (!document || size!.heap + size!.json > this.budget.limit) break;
        Object.assign(lookup, { document, heap: size!.heap, json: size!.json });
        return;
      } catch (error) {
        if (attempt >= retries.length) {
          console.warn(`failed to look up ${described}, sending its ids only: ${error}`);
          this.lookupsFailing.add(ns.coll);
          break;
        }
        if (attempt === 0) console.warn(`failed to look up ${described}, retrying: ${error}`);
        await new Promise(resolve => setTimeout(resolve, retries[attempt]));
      }
    }
    lookup.document = null;
    if (lookup.key && this.sharedLookups.get(lookup.key) === lookup) this.sharedLookups.delete(lookup.key);
  }

  private findOne(collection: string, id: unknown) {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>(
      (_, reject) => (timer = setTimeout(() => reject(new Error(`no answer within ${LOOKUP_TIMEOUT_MS}ms`)), LOOKUP_TIMEOUT_MS))
    );
    const found = mongoose.connection
      .collection(collection)
      .findOne({ _id: id as mongoose.mongo.BSON.ObjectId }, { readConcern: { level: 'majority' }, maxTimeMS: LOOKUP_MAX_TIME_MS });
    return Promise.race([found, timeout]).finally(() => clearTimeout(timer));
  }

  private forget(lookup: Lookup) {
    if (lookup.key && this.sharedLookups.get(lookup.key) === lookup) this.sharedLookups.delete(lookup.key);
    lookup.charged = false;
    lookup.document = undefined;
  }

  private buildBody({ change, projector, subscriptionId }: Delivery) {
    if (!change) return undefined;
    let fullDocument: AnyObject | null;
    try {
      const document = (change as { fullDocument?: AnyObject }).fullDocument;
      fullDocument = document ? projector!(document) : null;
    } catch (error) {
      console.warn(`projection failed for subscription ${subscriptionId} on ${change.ns.coll}: ${error}`);
      return undefined;
    }
    // Deleted, or soft-deleted.
    const isDeleteish = change.operationType === 'delete' || fullDocument?.deletedAt;
    return messageTail({
      collectionName: change.ns.coll,
      operationType: isDeleteish ? 'delete' : change.operationType,
      data: {
        ...(isDeleteish ? {} : fullDocument),
        id: change.documentKey._id.toString(),
        _id: change.documentKey._id.toString(),
      },
    });
  }

  private message(subscriber: SubscriberType, tail: Buffer, whole = false): Message {
    const { connectionId, endpoint } = subscriber;
    const prefix = whole ? Buffer.alloc(0) : messagePrefix(subscriber.clientId);
    const send = this.options.send;
    if (send) {
      return {
        posts: 1,
        frameBytes: () => prefix.length + tail.length + REQUEST_BYTES,
        post: (_index, _signal, _probation, connected) => {
          connected();
          return send(connectionId, endpoint, JSON.parse(Buffer.concat([prefix, tail]).toString()) as IMessageDataToClient);
        },
      };
    }
    const frames = new Frames(prefix, tail, !DISABLE_CHUNKING);
    return {
      posts: frames.count,
      frameBytes: index => frames.bytes(index),
      post: (index, abortSignal, probation, connected) => {
        const unwatch = onConnected(endpoint, connectionId, connected);
        return this.gateway(endpoint)
          [probation ? 'probation' : 'post'].send(
            new PostToConnectionCommand({ ConnectionId: connectionId, Data: frames.frame(index) }),
            { abortSignal }
          )
          .finally(unwatch);
      },
    };
  }

  private settleDelivery(delivery: Delivery, index: number, result: PromiseSettledResult<unknown>) {
    const subscriber = delivery.targets[index];
    if (result.status === 'rejected') this.noteDrop(subscriber.connectionId, result.reason);
    // Registered through another gateway: sent its first message, then removed.
    if (subscriber.endpoint !== this.managementEndpoint && !(result.status === 'fulfilled' && result.value === DROPPED)) {
      this.removeForeign(subscriber);
    }
    if (delivery.pending > 0) return;
    if (delivery.body) this.bodies.release(delivery.owner!, delivery.bodyKey!, delivery.body);
    delivery.done();
    this.bodies.wake();
  }

  // Pings and sends to another gateway's connections are tried once, so they fail alike in a gateway fault and
  // are logged once per burst.
  private noteDrop(connectionId: string, reason: unknown) {
    if (this.drops++ > 0) return;
    console.warn(`dropped a message to connection ${connectionId}: ${reason}`);
    setTimeout(() => {
      if (this.drops > 1) console.warn(`dropped ${this.drops - 1} more messages in ${DROP_LOG_MS}ms`);
      this.drops = 0;
    }, DROP_LOG_MS).unref();
  }

  private removeForeign({ endpoint, connectionId }: SubscriberType) {
    if (!this.byConnection.has(`${endpoint} ${connectionId}`)) return;
    console.warn(`removing connection ${connectionId} from its subscriptions: its endpoint ${endpoint} is not this gateway's`);
    void this.tombstones.add(endpoint, connectionId, 'another gateway');
    this.scheduler.close(endpoint, connectionId);
    this.forgetConnection(endpoint, connectionId);
  }

  // One POST client and one DELETE client per endpoint, each with its own sockets and one attempt per request.
  private gateway(endpoint: string) {
    let gateway = this.gateways.get(endpoint);
    if (gateway) return gateway;
    let logged = false;
    let resolving: ReturnType<typeof credentialProviderChain> | undefined;
    // Each client caches what this returns and calls it again before the credentials expire; the clients
    // share one lookup.
    const credentials = async () => {
      resolving ??= credentialProviderChain().finally(() => (resolving = undefined));
      const { source, credentials } = await resolving;
      if (!logged) console.debug(`gateway client for ${endpoint} uses credentials from ${source}`);
      logged = true;
      return credentials;
    };
    const client = (sockets: number) => {
      const agent = { keepAlive: true, maxSockets: sockets, lookup: this.lookup };
      return new ApiGatewayManagementApiClient({
        endpoint,
        credentials,
        maxAttempts: 1,
        // Without throwOnRequestTimeout the SDK only logs a warning and the request never settles.
        requestHandler: {
          httpAgent: new http.Agent(agent),
          httpsAgent: new https.Agent(agent),
          connectionTimeout: SDK_CONNECT_TIMEOUT_MS,
          requestTimeout: (this.options.sendTimeoutMs ?? SEND_TIMEOUT_MS) + SDK_TIMEOUT_MARGIN_MS,
          throwOnRequestTimeout: true,
        },
      });
    };
    // Room above the slots and the released requests, so a POST never queues for a socket. New and slow
    // connections post through their own agent, so the sockets their cut attempts drop are never a healthy
    // connection's.
    const sockets = this.slots + 8 + RELEASES_PER_SLOT * this.slots;
    gateway = { post: client(sockets), probation: client(sockets), delete: client(DELETE_SOCKETS) };
    this.gateways.set(endpoint, gateway);
    return gateway;
  }

  // To each connection with nothing already queued.
  async pingAllSubscribers() {
    const subscriptions = [...this.subscriptionCollections.keys(), ...this.dedicatedStreams.keys()];
    const idle = subscriptions.map(subscriptionId => {
      if (this.streamOf(subscriptionId)?.replaying(subscriptionId)) return [];
      const subscribers = this.live(subscriptionId, this.subscribers.get(subscriptionId) ?? []);
      return subscribers.length > 0 ? subscribers.filter(s => !this.scheduler.busy(s.endpoint, s.connectionId)) : undefined;
    });
    let pending = subscriptions.length;
    if (pending === 0) return;
    await new Promise<void>(resolve => {
      const done = () => --pending === 0 && resolve();
      subscriptions.forEach((subscriptionId, i) => this.enqueue(subscriptionId, { bytes: 0, ping: true, done }, idle[i]));
    });
  }

  private subscriptionCount() {
    return this.subscriptionCollections.size + this.dedicatedStreams.size;
  }

  // Recomputed as the subscriptions, and the heap they hold, change by a tenth.
  private resizeBudget() {
    if (!this.heapUse) return;
    const count = this.subscriptionCount();
    if (Math.abs(count - this.heapUse.count) >= Math.max(1, this.heapUse.count / 10)) this.sizeBudget();
  }

  // A load that runs out of heap never reaches the check after it, so the first subscriptions price the rest.
  private projectLoad(before: number, loaded: number, total: number) {
    collectGarbage();
    const { heap_size_limit: heapLimit, used_heap_size: now } = v8.getHeapStatistics();
    const perSubscription = Math.max(0, (now - before) / loaded);
    const { limit, used } = sizeBudget({ heapLimit, base: before, startCount: 0, perSubscription, count: total });
    const budget = this.options.maxQueuedBytes ?? limit;
    const needed = used + 3 * budget;
    if (needed <= heapLimit) return;
    console.error(
      `loading ${total} subscriptions at about ${Math.round(perSubscription / 1024)}KB each, as the first ${loaded} took, with a ${mb(budget)} delivery budget needs a heap limit of at least ${mb(needed)}, ${mb(needed - heapLimit)} over the ${mb(heapLimit)} heap`
    );
  }

  // The heap must hold what the subscriptions use plus three times the budget.
  private sizeBudget(startup = false) {
    const heapUse = this.heapUse!;
    heapUse.count = this.subscriptionCount();
    const { limit, used } = sizeBudget(heapUse);
    this.budget.limit = this.options.maxQueuedBytes ?? limit;
    this.wakeMatching();
    if (startup) {
      console.info(
        `delivery budget ${mb(this.budget.limit)} of a ${mb(heapUse.heapLimit)} heap, ${mb(used)} in use after loading ${heapUse.count} subscriptions at about ${Math.round(heapUse.perSubscription / 1024)}KB each; documents up to about ${mb(this.budget.limit / 3)} are delivered, ${this.slots} POSTs in flight at once`
      );
    }
    const needed = used + 3 * this.budget.limit;
    const short = needed > heapUse.heapLimit;
    if (short && (startup || !this.shortfallLogged)) {
      console.error(
        `the ${mb(heapUse.heapLimit)} heap is ${mb(needed - heapUse.heapLimit)} short of what ${heapUse.count} subscriptions and a ${mb(this.budget.limit)} delivery budget need; raise the heap limit to at least ${mb(needed)}`
      );
    }
    this.shortfallLogged = short;
  }

  private streamOptions() {
    return {
      budget: this.budget,
      gate: this.openGate,
      admit: (stream: CollectionStream, heap: number) => this.admitRead(stream, heap),
      mayMatch: (stream: CollectionStream) => this.mayMatch(stream),
      onRead: () => this.enforce(),
      onHistoryLost: (stream: CollectionStream) => this.onHistoryLost(stream),
      waitEnded: (stream: CollectionStream) => this.bodies.cancel(stream),
    };
  }

  // The changes its subscribers missed are gone, so they reconnect and refetch.
  private onHistoryLost(stream: CollectionStream) {
    const reason = `the change history of ${stream.label} it was reading is gone`;
    const closed: Closed[] = [];
    for (const subscriptionId of stream.subscriptionIds) {
      for (const { endpoint, connectionId } of this.subscribers.get(subscriptionId) ?? []) closed.push({ endpoint, connectionId, reason });
    }
    this.closeMany(closed);
  }

  // The read gate. Whether a stream may take an event or document of this heap: under C, or once a stream has been
  // paused HEAD_PAUSE_MS, under that stream's resume line. A refused stream pauses and reads it again later.
  private admitRead(stream: CollectionStream, heap: number) {
    const limit = this.budget.limit;
    if (this.budget.stored > limit) this.evictReplay(() => this.budget.stored > limit);
    const head = this.headStream(Date.now());
    const line = head && head !== stream ? limit - head.pauseCharge : this.budget.ceiling;
    if (this.budget.heap + heap <= line) return true;
    console.warn(
      `deliveries hold ${this.budget.heap} bytes of heap and ${stream.label} read ${heap} more, over the ${Math.round(line)} byte line; pausing it; ${this.describePause(stream)}`
    );
    this.watchPaused();
    return false;
  }

  // The match gate. A stream's next change is matched while E is within C or none of its own delivery overhead is
  // held: only its settled deliveries and closed connections free that, and unmatched changes never hold any.
  private mayMatch(stream: CollectionStream) {
    if (this.budget.heap <= this.budget.ceiling || this.budget.overheadOf(stream) <= 0) return true;
    this.matchWaiting.add(stream);
    return false;
  }

  // After E or C changed, or `stream`'s own heap fell, which is the only way its overhead falls.
  private wakeMatching(stream?: CollectionStream) {
    if (this.matchWaiting.size === 0) return;
    if (this.budget.heap <= this.budget.ceiling) {
      const waiting = [...this.matchWaiting];
      this.matchWaiting.clear();
      for (const s of waiting) s.wake();
    } else if (stream && this.matchWaiting.has(stream) && this.budget.overheadOf(stream) <= 0) {
      this.matchWaiting.delete(stream);
      stream.wake();
    }
  }

  // The stream paused longest, if HEAD_PAUSE_MS ago or more, counting one resumed first that has not read yet.
  private headStream(now: number) {
    let head: CollectionStream | undefined;
    let since = now - HEAD_PAUSE_MS;
    for (const stream of this.allStreams()) {
      const at = stream.paused ? stream.pausedSince! : stream.priority;
      if (at !== undefined && at <= since) {
        head = stream;
        since = at;
      }
    }
    return head;
  }

  private watchPaused() {
    if (this.resumeTimer || this.stopping) return;
    this.resumeTimer = setInterval(() => this.enforce(), RESUME_CHECK_MS);
    this.resumeTimer.unref();
  }

  // Over the budget: evict replay-only events and close slow connections; with the heap over it, pause the
  // heaviest stream. Each paused stream resumes once the heap is under its line, the one paused longest first
  // once that is HEAD_PAUSE_MS.
  private enforce() {
    const limit = this.budget.limit;
    const streams = this.allStreams();
    if (this.budget.stored > limit) this.evictReplay(() => this.budget.stored > limit);
    this.scheduler.closeSlow(limit);
    if (this.budget.heap > limit && !streams.some(s => s.paused)) {
      const heaviest = this.budget.heaviest(streams, stream => stream.largestCharge);
      if (heaviest) {
        console.warn(
          `deliveries hold ${this.budget.heap} bytes of heap, over the ${limit} byte budget; pausing ${heaviest.label}, which holds ${this.budget.streamBytes(heaviest)}; ${this.describePause(heaviest)}`
        );
        heaviest.pause(Math.max(limit / 8, heaviest.largestCharge));
      }
    }
    const paused = streams.filter(s => s.paused).sort((a, b) => a.pausedSince! - b.pausedSince!);
    if (paused.length === 0) {
      clearInterval(this.resumeTimer);
      this.resumeTimer = undefined;
    } else {
      this.watchPaused();
      const lowest = Math.min(...paused.map(s => limit - s.pauseCharge));
      if (this.budget.heap > lowest) this.evictReplay(() => this.budget.heap > lowest);
      const head = Date.now() - paused[0].pausedSince! >= HEAD_PAUSE_MS ? paused[0] : undefined;
      for (const stream of paused) {
        if (head?.paused && stream !== head) break;
        if (this.budget.heap > limit - stream.pauseCharge) continue;
        console.info(`deliveries hold ${this.budget.heap} bytes of heap, resuming ${stream.label}`);
        this.refetch.delete(stream);
        stream.resume(stream === head);
      }
    }
    this.bodies.wake();
  }

  private describePause(stream: CollectionStream) {
    const medianMs = this.scheduler.medianMs();
    if (medianMs === undefined) return 'P unknown, no POST completed in the last minute';
    const connections = new Set<string>();
    for (const id of stream.subscriptionIds) {
      for (const s of this.subscribers.get(id) ?? []) connections.add(`${s.endpoint} ${s.connectionId}`);
    }
    const { ms, drain, perPost } = pauseEstimate({
      stored: this.budget.heap,
      limit: this.budget.limit,
      charge: stream.largestCharge,
      posts: this.estimatePosts(stream.largestJson),
      medianMs,
      connections: connections.size,
      slots: this.scheduler.healthySlots,
    });
    return `P about ${(ms / 1000).toFixed(1)}s (D=${Math.round(drain)}, c/m=${Math.round(perPost)}, L=${Math.round(medianMs)}ms, k=${connections.size})`;
  }

  // From the stream whose replay-only events hold the most, oldest first within it, so a quiet stream keeps its
  // window until the larger ones have come down to its size.
  private evictReplay(over: () => boolean) {
    if (!over()) return;
    const windows = this.allStreams()
      .map(stream => {
        const events = stream.replayOnly();
        return { stream, events, next: 0, bytes: events.reduce((sum, event) => sum + this.budget.freedBy(event), 0) };
      })
      .filter(window => window.events.length > 0);
    while (over() && windows.length > 0) {
      const window = windows.reduce((most, w) => (w.bytes > most.bytes ? w : most));
      const event = window.events[window.next++];
      window.bytes -= this.budget.freedBy(event);
      window.stream.evict(event);
      if (window.next >= window.events.length) windows.splice(windows.indexOf(window), 1);
    }
  }

  // Joiners whose replay window is gone are closed and refetch: recorded at once, so a restart still closes them,
  // and closed WINDOW_CLOSE_BURST at once and one a second after that.
  private windowLost(lost: WindowLost, joiners: SubscriberType[]) {
    if (joiners.length === 0) return;
    const reason = lost === 'evicted' ? 'its replay window was evicted for memory' : 'its replay window had already been trimmed';
    const closings = joiners.map(({ endpoint, connectionId }) => ({ endpoint, connectionId, reason }));
    this.closes.record(closings);
    this.windowQueue.push(...closings);
    this.closeWindows();
  }

  private closeWindows() {
    const now = Date.now();
    this.windowCloses = Math.min(WINDOW_CLOSE_BURST, this.windowCloses + (now - this.windowClosesAt) / 1000);
    this.windowClosesAt = now;
    const due: Closed[] = [];
    while (this.windowQueueHead < this.windowQueue.length && this.windowCloses >= 1) {
      due.push(this.windowQueue[this.windowQueueHead++]);
      this.windowCloses--;
    }
    if (this.windowQueueHead === this.windowQueue.length) {
      this.windowQueue = [];
      this.windowQueueHead = 0;
    }
    this.closeMany(due);
    if (this.windowQueue.length === 0 || this.windowTimer || this.stopping) return;
    this.windowTimer = setTimeout(() => {
      this.windowTimer = undefined;
      this.closeWindows();
    }, (1 - this.windowCloses) * 1000);
    this.windowTimer.unref();
  }
}

const mb = (bytes: number) => `${Math.round(bytes / (1024 * 1024))}MB`;

// A GC before each heap measurement, so it counts only what is live.
function collectGarbage() {
  v8.setFlagsFromString('--expose_gc');
  (vm.runInNewContext('gc') as () => void)();
}

// A digest, so the map of definitions costs a fixed size per subscription whatever its query.
export function definitionOf({ collectionName, query, fields }: Pick<IQuerySubscriptionDocument, 'collectionName' | 'query' | 'fields'>) {
  return createHash('sha256').update(JSON.stringify([collectionName, query, fields])).digest().subarray(0, 16).toString('base64');
}

function entryKey({ endpoint, connectionId, clientId, createdAt }: SubscriberType) {
  return `${endpoint} ${connectionId} ${clientId} ${createdAt ? new Date(createdAt).getTime() : ''}`;
}
