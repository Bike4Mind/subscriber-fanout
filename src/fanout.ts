import { describeConnectError, describeMongoUri } from './mongoUri.js';
import mongoose, { AnyObject } from 'mongoose';
import { IQuerySubscriptionDocument, QuerySubscription } from './querySubscription.js';
import { credentialProviderChain, sendToConnection } from './websocket.js';
import { IDataSubscriptionUpdateAction, IMessageDataToClient } from './wireTypes.js';
import { flatten } from 'flat';
import { pickBy, zip } from 'lodash-es';
import { ApiGatewayManagementApiClient } from '@aws-sdk/client-apigatewaymanagementapi';
import { Mutex } from 'async-mutex';
import { CollectionStream, isResumeExpiredError, waitForStreamInit } from './collectionStream.js';
import { FanoutCursor } from './fanoutCursor.js';
import { SubscriptionFilter, buildSubscriptionFilter, transformAndPrefixQuery } from './matcher.js';
import { organizationMfaDeliveryAllowed } from './organizationMfa.js';

export const MAX_SEND_ATTEMPTS = 3;

type SubscriberType = IQuerySubscriptionDocument['subscribers'][0];

export type SendFn = (connectionId: string, endpoint: string, message: IMessageDataToClient) => Promise<unknown>;

export type SubscriberFanoutOptions = {
  mongoUri: string;
  stage: string;
  managementEndpoint: string;
  send?: SendFn;
  // Set to 0 to disable the periodic ping (tests call pingAllSubscribers directly).
  pingIntervalMs?: number;
  buildFilter?: (subscription: IQuerySubscriptionDocument) => SubscriptionFilter;
};

export class SubscriberFanout {
  // Map each AWS API Gateway endpoint to an ApiGatewayManagementApiClient.
  // I *believe* there will only ever be one, but we provide a Map in case
  // there's ever more than one.
  private apiGatewayClients: Map<string, ApiGatewayManagementApiClient> = new Map();
  private apiGatewayClientsMutex = new Mutex();

  // Map of query ID (hash of query) to Subscriber info (ISubscription.subscribers)
  subscribers: Map<string, Array<SubscriberType>> = new Map();
  // One shared change stream per watched collection
  private streamsByCollection: Map<string, CollectionStream> = new Map();
  // Map of query ID to the collection whose shared stream it is registered on
  private subscriptionCollections: Map<string, string> = new Map();
  // Subscriptions whose query cannot be evaluated in process keep their own stream
  private dedicatedStreams: Map<string, mongoose.mongo.ChangeStream<AnyObject>> = new Map();

  private subscriptionWatcher?: mongoose.mongo.ChangeStream<IQuerySubscriptionDocument>;
  private pingTimer?: NodeJS.Timeout;

  // Subscribers whose stored endpoint no longer matches are stale and get dropped.
  private managementEndpoint: string;
  private send: SendFn;
  private buildFilter: (subscription: IQuerySubscriptionDocument) => SubscriptionFilter;

  constructor(private options: SubscriberFanoutOptions) {
    this.managementEndpoint = options.managementEndpoint;
    this.send =
      options.send ??
      (async (connectionId, endpoint, message) =>
        sendToConnection(connectionId, await this.getOrAddGatewayClient(endpoint), message));
    this.buildFilter = options.buildFilter ?? buildSubscriptionFilter;
    console.info(`SubscriberFanout: ${options.stage} starting`);
  }

  async start() {
    const url = this.options.mongoUri.replace('%STAGE%', this.options.stage).replace('localhost', '127.0.0.1');
    const target = describeMongoUri(url);

    console.debug(`connecting to MongoDB at ${target}`);
    try {
      await mongoose.connect(url, { maxPoolSize: 2 });
    } catch (error) {
      throw new Error(`failed to connect to MongoDB at ${target} (${describeConnectError(error, url)})`);
    }

    console.debug(`connected to MongoDB`);

    // Plain objects, like the ones built from change events: spreading a hydrated
    // document (as the dedicated-stream restart does) would drop its fields.
    for (const subscription of await QuerySubscription.find()) {
      await this.addSubscription({ ...subscription.toObject(), id: subscription.id }, { deferOpen: true });
    }
    await FanoutCursor.deleteMany({ collectionName: { $nin: [...this.streamsByCollection.keys()] } });
    await Promise.all([...this.streamsByCollection.values()].map(stream => stream.open()));

    const changeStream = QuerySubscription.watch([], { fullDocument: 'updateLookup' });
    changeStream.on('change', change => this.handleSubscriptionChange(change));
    this.subscriptionWatcher = changeStream;
    await waitForStreamInit(changeStream);

    const pingIntervalMs = this.options.pingIntervalMs ?? 60 * 1000; // minutely
    if (pingIntervalMs > 0) {
      this.pingTimer = setInterval(() => {
        this.pingAllSubscribers();
      }, pingIntervalMs);
    }
  }

  async stop() {
    clearInterval(this.pingTimer);
    await this.subscriptionWatcher?.close();
    await Promise.all([...this.streamsByCollection.values()].map(stream => stream.close('shutdown')));
    this.streamsByCollection.clear();
    await Promise.all([...this.dedicatedStreams.values()].map(stream => stream.close()));
    this.dedicatedStreams.clear();
    this.subscriptionCollections.clear();
    this.subscribers.clear();
    await mongoose.disconnect();
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
      case 'insert':
        // Adding a new subscription
        console.debug(
          `subscription change: ${change.operationType} ${JSON.stringify(change.ns)} ${change.documentKey._id} ${JSON.stringify(
            change.fullDocument
          )}`
        );
        await this.addSubscription({ ...change.fullDocument, id: change.documentKey._id.toString() });
        break;
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
        subscription.query
      )} fields ${JSON.stringify(subscription.fields)}, lastChange ${subscription.lastChange ?? 'null'}`
    );

    // Skip subscriptions with an invalid collection name.
    if (!subscription.collectionName || typeof subscription.collectionName !== 'string') {
      return;
    }

    if (this.subscriptionCollections.has(subscription.id) || this.dedicatedStreams.has(subscription.id)) {
      await this.removeSubscription(subscription.id);
    }
    this.subscribers.set(subscription.id, subscription.subscribers);

    let filter: SubscriptionFilter;
    try {
      filter = this.buildFilter(subscription);
    } catch (error) {
      console.warn(
        `subscription ${subscription.id} on ${subscription.collectionName} cannot be matched in process, using a dedicated stream: ${error}`
      );
      this.openDedicatedStream(subscription);
      return;
    }

    let stream = this.streamsByCollection.get(subscription.collectionName);
    const isNew = !stream;
    if (!stream) {
      stream = new CollectionStream(subscription.collectionName, (subscriptionId, change) =>
        this.sendChangeToSubscribers(change, subscriptionId)
      );
      this.streamsByCollection.set(subscription.collectionName, stream);
    }
    stream.register(subscription.id, { ...filter, subscription });
    this.subscriptionCollections.set(subscription.id, subscription.collectionName);

    if (isNew && !deferOpen) {
      await stream.open();
    }
  }

  openDedicatedStream(subscription: IQuerySubscriptionDocument) {
    const collection = mongoose.connection.collection(subscription.collectionName);
    const pipeline: Array<Record<string, unknown>> = [
      { $match: transformAndPrefixQuery(subscription.query, 'fullDocument') },
    ];
    if (Object.keys(subscription.fields ?? {}).length > 0) {
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
          },
        });
      }
    }

    const changeStream = collection.watch(pipeline, {
      fullDocument: 'updateLookup',
      ...(subscription.lastChange
        ? { startAfter: JSON.parse(subscription.lastChange) }
        : {
            startAtOperationTime: new mongoose.mongo.Timestamp({
              t: Math.floor(subscription.updatedAt.getTime() / 1000),
              i: 1,
            }),
          }),
    });
    changeStream.on('change', change => {
      this.sendChangeToSubscribers(change, subscription.id);
      // Dedicated streams are not covered by subscriberFanoutCursors, so they keep resuming from lastChange.
      QuerySubscription.updateOne({ _id: subscription.id }, { $set: { lastChange: JSON.stringify(change._id) } }).catch(
        err => console.error(`Failed to store lastChange for ${subscription.id}:`, err)
      );
    });
    changeStream.on('error', error => {
      console.error(
        `changeStream error on query ${JSON.stringify(pipeline)} for collection ${subscription.collectionName}: ${error}`
      );

      if (isResumeExpiredError(error)) {
        console.log(`Resume token expired for subscription ${subscription.id}, clearing and restarting...`);

        const subscriptionCopy: IQuerySubscriptionDocument = {
          ...subscription,
          lastChange: undefined,
        };

        QuerySubscription.updateOne({ _id: subscription.id }, { $unset: { lastChange: '' } })
          .then(() => {
            changeStream.close();
            this.dedicatedStreams.delete(subscription.id);

            // Small delay to avoid rapid retries
            setTimeout(() => {
              if (!this.subscribers.has(subscription.id)) return;
              this.openDedicatedStream(subscriptionCopy);
            }, 1000);
          })
          .catch(err => {
            console.error(`Failed to clear resume token for ${subscription.id}:`, err);
          });
      }
    });
    this.dedicatedStreams.set(subscription.id, changeStream);
    return changeStream;
  }

  async updateSubscription(subscription: IQuerySubscriptionDocument) {
    // In this case, we've been told by mongo that the subscription list for a query
    // has changed, so we need to update our in-memory version of the subscribers.  It's
    // possible there are now zero subscribers, in which case we need to close the
    // active query.
    if (subscription.subscribers.length > 0) {
      // Update the subscribers
      if (this.subscribers.get(subscription.id)?.length !== subscription.subscribers.length) {
        console.info(`updating subscription ${subscription.id} (now ${subscription.subscribers.length} subscribers)`);
      }
      this.subscribers.set(subscription.id, subscription.subscribers);
      return;
    }

    // else subscribers is empty, so we need to close the query
    if (!this.subscriptionCollections.has(subscription.id) && !this.dedicatedStreams.has(subscription.id)) {
      console.error(`no change stream for subscription ${subscription.id}`);
    }
    await this.removeSubscription(subscription.id);
    await QuerySubscription.deleteOne({ _id: subscription.id });
  }

  async deleteSubscription(subscriptionId: string) {
    console.info(`deleted subscription ${subscriptionId}`);
    await this.removeSubscription(subscriptionId);
  }

  private async removeSubscription(subscriptionId: string) {
    this.subscribers.delete(subscriptionId);

    const dedicated = this.dedicatedStreams.get(subscriptionId);
    if (dedicated) {
      this.dedicatedStreams.delete(subscriptionId);
      await dedicated.close();
    }

    const collectionName = this.subscriptionCollections.get(subscriptionId);
    if (collectionName === undefined) return;
    this.subscriptionCollections.delete(subscriptionId);

    const stream = this.streamsByCollection.get(collectionName);
    if (stream && stream.unregister(subscriptionId) === 0) {
      this.streamsByCollection.delete(collectionName);
      await stream.close('idle');
    }
  }

  async sendChangeToSubscribers(change: mongoose.mongo.ChangeStreamDocument<AnyObject>, subscriptionId: string) {
    if (
      change.operationType !== 'update' &&
      change.operationType !== 'replace' &&
      change.operationType !== 'insert' &&
      change.operationType !== 'delete'
    ) {
      console.warn(`Unexpected operationType ${change.operationType ?? 'null'}: ${JSON.stringify(change)}`);
      return;
    }

    // We transform operationType and data (below) if the document has been
    // either deleted (as indicated by operationType) or soft-deleted.
    const isDeleteish = change.operationType === 'delete' || change.fullDocument?.deletedAt;

    const storedCount = this.subscribers.get(subscriptionId)?.length ?? 0;
    const subscribers = await this.sendMessageToSubscription(
      subscriptionId,
      subscriber =>
        ({
          action: 'data_update',
          subscriptionId: subscriber.clientId,
          collectionName: change.ns.coll,
          operationType: isDeleteish ? 'delete' : change.operationType,
          data: {
            ...(isDeleteish ? {} : change.fullDocument),
            id: change.documentKey._id.toString(),
            _id: change.documentKey._id.toString(),
          },
        }) as IDataSubscriptionUpdateAction
    );

    // Update mongodb with new subscriber list
    if (!subscribers?.length) {
      // Shouldn't normally happen; clean up the orphaned subscription.
      await QuerySubscription.deleteOne({ _id: subscriptionId });
      await this.removeSubscription(subscriptionId);
      return;
    }

    if (subscribers.length !== storedCount) {
      await QuerySubscription.updateOne({ _id: subscriptionId }, { $set: { subscribers } });
    }
    this.subscribers.set(subscriptionId, subscribers);
  }

  async getOrAddGatewayClient(endpoint: string) {
    // Grab a mutex around the change to apiGatewayClients
    const release = await this.apiGatewayClientsMutex.acquire();
    try {
      let client = this.apiGatewayClients.get(endpoint);
      if (client) {
        return client;
      }

      const { source, credentials } = await credentialProviderChain();
      const expiration = credentials.expiration?.toISOString() || 'never';
      console.debug(`Creating new client for ${endpoint}, credential source ${source}, expiration ${expiration}`);
      client = new ApiGatewayManagementApiClient({
        endpoint,
        credentials,
      });
      this.apiGatewayClients.set(endpoint, client);

      if (credentials.expiration) {
        // Arrange for a renewal or an exit at `expiration`
        setTimeout(() => {
          // Delete the credentials so we'll re-gather them when needed
          this.apiGatewayClients.delete(endpoint);
        }, credentials.expiration.getTime() - Date.now());
      }

      return client;
    } finally {
      release();
    }
  }

  async sendMessageToSubscription(
    subscriptionId: string,
    getPayload: (subscriber: SubscriberType) => IMessageDataToClient
  ) {
    const subscribers = this.subscribers.get(subscriptionId);
    if (!subscribers?.length) return;

    const payloads = subscribers.map(subscriber => getPayload(subscriber));

    const promises = zip(subscribers, payloads).map(async ([subscriber, message]) => {
      // Can't happen (zip pairs them 1:1), but keeps the type-checker happy.
      if (!subscriber || !message) return;
      if (message.action === 'data_update' && message.operationType !== 'delete' &&
        !(await organizationMfaDeliveryAllowed(subscriber.connectionId, message.collectionName, message.data._id))) return;
      return await this.send(subscriber.connectionId, subscriber.endpoint, message);
    });

    const results = await Promise.allSettled(promises);
    results.forEach((result, i) => {
      if (result.status === 'rejected') {
        // Don't remove the subscriber just yet, just mark it as errored
        // so we can retry sending to it. We'll remove it after a few attempts.
        subscribers[i].attempts = (subscribers[i].attempts ?? 0) + 1;
        subscribers[i].errorReason = result.reason.toString();

        // DEBUG: Set to true to enable verbose WebSocket error logging
        const doVerbose = false;

        if (doVerbose) {
          console.debug(
            `Failed sending to subscriber ${subscribers[i].connectionId} from subscription ${subscriptionId}: ${result.reason}`
          );
        }

        if (result.reason.toString().startsWith('ExpiredTokenException')) {
          // we need to take a coordinated exit from the process and let the service manager restart us
          throw result.reason;
        }
      }
    });

    // Drop subscribers with:
    // - too many attempts
    // - old endpoints
    const cleanSubscribers = subscribers.filter(
      s => (s.attempts ?? 0) < MAX_SEND_ATTEMPTS && s.endpoint === this.managementEndpoint
    );
    if (subscribers.length > cleanSubscribers.length) {
      console.debug(
        `removing ${subscribers.length - cleanSubscribers.length} subscribers from subscription ${subscriptionId}`
      );
    }
    return cleanSubscribers;
  }

  async pingAllSubscribers() {
    const subscriptions = [...this.subscriptionCollections.keys(), ...this.dedicatedStreams.keys()];
    await Promise.all(
      subscriptions.map(async subscriptionId => {
        const subscribers = await this.sendMessageToSubscription(subscriptionId, () => ({
          action: 'pong',
        }));

        // `subscribers` can become empty since we now remove subscribers when users disconnect
        if (!subscribers?.length) {
          await QuerySubscription.deleteOne({ _id: subscriptionId });
          await this.removeSubscription(subscriptionId);
          return;
        }
        await QuerySubscription.updateOne({ _id: subscriptionId }, { $set: { subscribers } });
        this.subscribers.set(subscriptionId, subscribers);
      })
    );
  }
}
