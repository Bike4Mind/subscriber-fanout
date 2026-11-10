/*
 * subscriber-fanout
 *
 * This microservice manages Mongo query subscriptions for clients.
 * It does so by monitoring a MongoDB collection of queries and
 * subscribers, and maintains one change stream per watched collection,
 * fanning matching change-events out to each query's subscribers.
 *
 * This script is the frontend for a standalone system.  It can
 * be run locally for development, but generally runs through SST
 * as a persistent service.
 *
 * See DESIGN.md for more details.
 */

import { Resource } from 'sst';
import { SubscriberFanout } from './fanout.js';

const stage = process.env.STAGE || Resource.App.stage || 'development';

const fanout = new SubscriberFanout({
  mongoUri: process.env.MONGODB_URI || Resource.MONGODB_URI.value || 'mongodb://localhost:27017/bike4mind',
  stage,
  // Prefer an explicit env var (self-host / plain Docker); fall back to the
  // SST-linked Resource (AWS deploy).
  managementEndpoint: process.env.WEBSOCKET_MANAGEMENT_ENDPOINT || Resource.websocket.managementEndpoint,
});

fanout.start().catch(error => {
  console.error(error);
  process.exit(1);
});

// Flush resume tokens on a deploy or scale-in so the next task resumes where
// this one stopped instead of up to one persist interval earlier.
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, () => {
    console.info(`${signal} received, shutting down`);
    fanout
      .stop()
      .catch(error => console.error('shutdown failed:', error))
      .finally(() => process.exit(0));
  });
}
