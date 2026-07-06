/*
 * subscriber-fanout
 *
 * This microservice manages Mongo query subscriptions for clients.
 * It does so by monitoring a MongoDB collection of queries and
 * subscribers, and maintains a subscription for each of those
 * queries, fanning change-events out to the subscribers.
 *
 * This script is the frontend for a standalone system.  It can
 * be run locally for development, but generally runs through SST
 * as a persistent service.
 *
 * See DESIGN.md for more details.
 */

import mongoose, { AnyObject } from 'mongoose';
import { IQuerySubscriptionDocument, QuerySubscription } from './querySubscription.js';
import { credentialProviderChain, sendToConnection } from './websocket.js';
import { IDataSubscriptionUpdateAction, IMessageDataToClient } from './wireTypes.js';
import { flatten } from 'flat';
import { pickBy, zip } from 'lodash-es';
import { ApiGatewayManagementApiClient } from '@aws-sdk/client-apigatewaymanagementapi';
import { Mutex } from 'async-mutex';
import { Resource } from 'sst';

const MAX_SEND_ATTEMPTS = 3;

type SubscriberType = IQuerySubscriptionDocument['subscribers'][0];

class SubscriberFanout {
  // Map each AWS API Gateway endpoint to an ApiGatewayManagementApiClient.
  // I *believe* there will only ever be one, but we provide a Map in case
  // there's ever more than one.
  private apiGatewayClients: Map<string, ApiGatewayManagementApiClient> = new Map();
  private apiGatewayClientsMutex = new Mutex();

  // Map of query ID (hash of query) to Subscriber info (ISubscription.subscribers)
  subscribers: Map<string, Array<SubscriberType>> = new Map();
  // Map of query ID to the mongo changeStream handle
  subscriptions: Map<string, mongoose.mongo.ChangeStream<AnyObject>> = new Map();

  // The WebSocket management endpoint this fanout pushes to. Prefer an explicit
  // env var (self-host / plain Docker); fall back to the SST-linked Resource
  // (AWS deploy). Subscribers whose stored endpoint no longer matches are stale
  // and get dropped.
  private managementEndpoint: string =
    process.env.WEBSOCKET_MANAGEMENT_ENDPOINT || Resource.websocket.managementEndpoint;

  constructor() {
    console.info(`SubscriberFanout: ${process.env.STAGE || Resource.App.stage || 'development'} starting`);
  }

  async start() {
    const mongoURI = process.env.MONGODB_URI || Resource.MONGODB_URI.value || 'mongodb://localhost:27017/bike4mind';
    const stage = process.env.STAGE || Resource.App.stage || 'development';
    const url = mongoURI.replace('%STAGE%', stage).replace('localhost', '127.0.0.1');
    const u = new URL(url);

    console.debug(`connecting to MongoDB at ${url.replace(`:${u.password}@`, ':***@')}`);
    await mongoose.connect(url, { maxPoolSize: 2 });

    console.debug(`connected to MongoDB`);

    for (const subscription of await QuerySubscription.find()) {
      await this.addSubscription(subscription);
    }

    const changeStream = QuerySubscription.watch([], { fullDocument: 'updateLookup' });
    changeStream.on('change', change => this.handleSubscriptionChange(change));

    setInterval(() => {
      this.pingAllSubscribers();
    }, 60 * 1000); // minutely
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

  async addSubscription(subscription: IQuerySubscriptionDocument) {
    console.info(
      `adding subscription for ${subscription.collectionName} query: ${JSON.stringify(
        subscription.query
      )} fields ${JSON.stringify(subscription.fields)}, lastChange ${subscription.lastChange ?? 'null'}`
    );
    // Add a subscription to this query, store the resulting ChangeStream handle
    // in the subscriptions map.

    // Skip subscriptions with an invalid collection name.
    if (!subscription.collectionName || typeof subscription.collectionName !== 'string') {
      return;
    }

    const collection = mongoose.connection.collection(subscription.collectionName);
    const pipeline: Array<Record<string, unknown>> = [
      { $match: this.transformAndPrefixQuery(subscription.query, 'fullDocument') },
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

    // Monitor each collection for a change
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
    });
    changeStream.on('error', error => {
      console.error(
        `changeStream error on query ${JSON.stringify(pipeline)} for collection ${subscription.collectionName}: ${error}`
      );

      // Handle resume token expiration
      if (
        error.message?.includes('Resume of change stream was not possible') ||
        error.message?.includes('resume point may no longer be in the oplog') ||
        error.message?.includes('the resume token was not found.')
      ) {
        console.log(`Resume token expired for subscription ${subscription.id}, clearing and restarting...`);

        // Make a clean copy of the subscription before async operations
        const subscriptionCopy: IQuerySubscriptionDocument = {
          ...subscription,
          lastChange: undefined,
        };

        // Clear the lastChange token and restart the change stream
        QuerySubscription.updateOne({ _id: subscription.id }, { $unset: { lastChange: '' } })
          .then(() => {
            // Close the failed stream
            changeStream.close();
            this.subscriptions.delete(subscription.id);

            // Re-setup the subscription without resume token
            setTimeout(() => {
              this.addSubscription(subscriptionCopy).catch(err => {
                console.error(`Failed to restart subscription ${subscription.id}:`, err);
              });
            }, 1000); // Small delay to avoid rapid retries
          })
          .catch(err => {
            console.error(`Failed to clear resume token for ${subscription.id}:`, err);
          });
      }
    });
    this.subscriptions.set(subscription.id, changeStream);
    this.subscribers.set(subscription.id, subscription.subscribers);
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
    const changeStream = this.subscriptions.get(subscription.id);
    if (!changeStream) {
      console.error(`no change stream for subscription ${subscription.id}`);
    } else {
      this.subscriptions.delete(subscription.id);
      await changeStream.close();
    }
    await QuerySubscription.deleteOne({ _id: subscription.id });
    this.subscribers.delete(subscription.id);
  }

  async deleteSubscription(subscriptionId: string) {
    console.info(`deleted subscription ${subscriptionId}`);
    this.subscriptions.get(subscriptionId)?.close();
    this.subscriptions.delete(subscriptionId);
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
      this.subscribers.delete(subscriptionId);
      return;
    }

    await QuerySubscription.updateOne(
      { _id: subscriptionId },
      {
        $set: {
          subscribers,
          lastChange: JSON.stringify(change._id),
        },
      }
    );
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
      const client = await this.getOrAddGatewayClient(subscriber.endpoint);
      return await sendToConnection(subscriber.connectionId, client, message);
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
    const subscriptions = [...this.subscriptions.keys()];
    await Promise.all(
      subscriptions.map(async subscriptionId => {
        const subscribers = await this.sendMessageToSubscription(subscriptionId, () => ({
          action: 'pong',
        }));

        // `subscribers` can become empty since we now remove subscribers when users disconnect
        if (!subscribers?.length) {
          await QuerySubscription.deleteOne({ _id: subscriptionId });
          this.subscribers.delete(subscriptionId);
          this.subscriptions.get(subscriptionId)?.close();
          this.subscriptions.delete(subscriptionId);
          return;
        }
        await QuerySubscription.updateOne({ _id: subscriptionId }, { $set: { subscribers } });
        this.subscribers.set(subscriptionId, subscribers);
      })
    );
  }

  transformAndPrefixQuery(query: Record<string, unknown>, prefix: string): Record<string, unknown> {
    const transformObject = (obj: Record<string, unknown>): Record<string, unknown> => {
      const transformed: Record<string, unknown> = {};
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          if (key.startsWith('$')) {
            // Handle special query operators - recursively process each value in the array or object
            if (Array.isArray(obj[key])) {
              transformed[key] = (obj[key] as Array<unknown>).map(item =>
                typeof item === 'object' && item !== null && !Array.isArray(item)
                  ? transformObject(item as Record<string, unknown>)
                  : item
              );
            } else if (typeof obj[key] === 'object' && obj[key] !== null) {
              transformed[key] = transformObject(obj[key] as Record<string, unknown>);
            } else {
              transformed[key] = obj[key];
            }
          } else {
            // Handle regular field keys
            if (key === '_id') {
              // Handle _id specially - can be string or ObjectId
              if (typeof obj[key] === 'string') {
                try {
                  const objectId = new mongoose.Types.ObjectId(obj[key] as string);
                  transformed[`${prefix}.${key}`] = objectId;
                } catch (error) {
                  transformed[`${prefix}.${key}`] = obj[key];
                }
              } else {
                // Already an ObjectId or other type, just prefix
                transformed[`${prefix}.${key}`] = obj[key];
              }
            } else if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
              // Recursively transform nested objects (but not arrays)
              transformed[`${prefix}.${key}`] = transformObject(obj[key] as Record<string, unknown>);
            } else {
              // Prefix field keys with '${prefix}.' - handles primitives and arrays
              transformed[`${prefix}.${key}`] = obj[key];
            }
          }
        }
      }
      return transformed;
    };

    return transformObject(query);
  }
}

new SubscriberFanout().start().catch(error => {
  console.error(error);
  process.exit(1);
});
