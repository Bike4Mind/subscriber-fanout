import { after, before, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { ChangeEvent, CollectionStream, Registration, isResumeExpiredError } from '../src/collectionStream.js';
import { FanoutCursor } from '../src/fanoutCursor.js';
import { buildSubscriptionFilter } from '../src/matcher.js';
import { IQuerySubscriptionDocument } from '../src/querySubscription.js';

// Resume behaviour against real servers: a three-node set so the primary can
// step down, and a token taken from a set that is gone before it starts.

const { MongoClient } = mongoose.mongo;

let rs: MongoMemoryReplSet;
let client: mongoose.mongo.MongoClient;
// A resume token whose cluster time is older than anything in rs's oplog.
let staleToken: unknown;
let dbCounter = 0;

async function waitFor(predicate: () => boolean | Promise<boolean>, what: string, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  assert.fail(`timed out waiting for ${what}`);
}

function registration(updatedAt = new Date()): Registration {
  const subscription = { updatedAt, query: {}, fields: {} } as unknown as IQuerySubscriptionDocument;
  return { ...buildSubscriptionFilter({ query: {} }), subscription };
}

function collector() {
  const delivered: Array<{ subscriptionId: string; change: ChangeEvent }> = [];
  const ids = (subscriptionId: string) =>
    delivered
      .filter(d => d.subscriptionId === subscriptionId)
      .map(d => String((d.change as { documentKey?: { _id: unknown } }).documentKey?._id));
  return { delivered, ids, deliver: (subscriptionId: string, change: ChangeEvent) => delivered.push({ subscriptionId, change }) };
}

const currentStream = (stream: CollectionStream) => Reflect.get(stream, 'stream') as mongoose.mongo.ChangeStream | undefined;

// Only for live and startAtOperationTime streams: on a startAfter stream the driver
// sets resumeToken from the option before the server answers.
const streamReady = (stream: CollectionStream) => () => currentStream(stream)?.resumeToken != null;

async function withConnection(run: (db: mongoose.mongo.Db, streams: CollectionStream[]) => Promise<void>) {
  const name = `resume_test_${++dbCounter}`;
  const streams: CollectionStream[] = [];
  await mongoose.connect(rs.getUri(name));
  try {
    await run(client.db(name), streams);
  } finally {
    await Promise.all(streams.map(stream => stream.close('idle')));
    await mongoose.disconnect();
  }
}

// Change events arrive in order, so once the sentinel is in, any duplicate of an
// earlier event would already be in too.
async function insertSentinel(db: mongoose.mongo.Db, collection: string, c: ReturnType<typeof collector>, subscriptionId: string) {
  const id = String((await db.collection(collection).insertOne({ sentinel: true })).insertedId);
  await waitFor(() => c.ids(subscriptionId).includes(id), 'sentinel delivery');
  return id;
}

before(async () => {
  mock.method(console, 'info', () => {});
  mock.method(console, 'log', () => {});
  mock.method(console, 'error', () => {});

  const old = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
  const oldClient = new MongoClient(old.getUri());
  try {
    const oldStream = oldClient.db('resume_test_old').collection('stale').watch();
    await oldStream.tryNext();
    await oldClient.db('resume_test_old').collection('stale').insertOne({ n: 1 });
    staleToken = (await oldStream.next())._id;
    await oldStream.close();
  } finally {
    await oldClient.close();
    await old.stop();
  }
  // Cluster time has one-second resolution; make sure rs starts strictly later.
  await new Promise(resolve => setTimeout(resolve, 1100));

  rs = await MongoMemoryReplSet.create({ replSet: { count: 3, storageEngine: 'wiredTiger' } });
  client = new MongoClient(rs.getUri());
  await client.connect();
  // startAtOperationTime is whole seconds; a start before the oplog's first entry is
  // rejected as history lost, so let the oplog get a little older than one second.
  await new Promise(resolve => setTimeout(resolve, 1100));
});

after(async () => {
  await client.close();
  await rs.stop();
});

describe('isResumeExpiredError', () => {
  it('matches ChangeStreamHistoryLost by code', () => {
    assert.equal(isResumeExpiredError({ code: 286, message: 'some new wording' }), true);
    assert.ok(!isResumeExpiredError({ code: 6, message: 'host unreachable' }));
  });
});

describe('CollectionStream resume', () => {
  it('keeps delivering across a primary step-down without loss or duplicates', async () => {
    await withConnection(async (db, streams) => {
      const c = collector();
      const stream = new CollectionStream('stepdown', c.deliver);
      streams.push(stream);
      stream.register('s1', registration());
      await stream.open();

      const { insertedId: a } = await db.collection('stepdown').insertOne({ n: 1 });
      await waitFor(() => c.ids('s1').length === 1, 'delivery before step-down');

      const primary = (await client.db('admin').command({ hello: 1 })).primary as string;
      await client
        .db('admin')
        .command({ replSetStepDown: 30, secondaryCatchUpPeriodSecs: 10 })
        .catch(() => null);
      await waitFor(async () => {
        const hello = await client.db('admin').command({ hello: 1 }).catch(() => null);
        return Boolean(hello?.primary) && hello?.primary !== primary;
      }, 'a new primary');

      const { insertedId: b } = await db.collection('stepdown').insertOne({ n: 2 });
      const sentinel = await insertSentinel(db, 'stepdown', c, 's1');
      assert.deepEqual(c.ids('s1'), [String(a), String(b), sentinel]);
    });
  });

  it('starts from the oldest subscription updatedAt when no cursor is stored', async () => {
    await withConnection(async (db, streams) => {
      const c = collector();
      const stream = new CollectionStream('startAt', c.deliver);
      streams.push(stream);
      stream.register('s1', registration(new Date()));
      const { insertedId } = await db.collection('startAt').insertOne({ n: 1 });

      await stream.open();
      assert.ok('startAtOperationTime' in (Reflect.get(stream, 'initialStart') as object));
      await waitFor(() => c.ids('s1').length === 1, 'event written before open');
      assert.deepEqual(c.ids('s1'), [String(insertedId)]);
    });
  });

  it('clears a resume token that is no longer in the oplog and reopens live', async () => {
    await withConnection(async (db, streams) => {
      // The server checks the token only once it reads past it. A token from another
      // set is answered with 280 (not found) or 286 (history lost) depending on the
      // server version and timing; both must be treated as expired.
      const probe = db.collection('expiredProbe').watch([], { startAfter: staleToken });
      await db.collection('expiredProbe').insertOne({ n: 0 });
      const rejected = await probe.next().then(
        () => undefined,
        (error: { code?: number; message?: string }) => error
      );
      await probe.close();
      assert.ok(rejected, 'server accepted the stale token');
      assert.ok([280, 286].includes(rejected.code ?? 0), `server rejected the stale token with ${rejected.code}: ${rejected.message}`);
      assert.ok(isResumeExpiredError(rejected));

      await FanoutCursor.create({ collectionName: 'expired', resumeToken: JSON.stringify(staleToken) });
      const c = collector();
      const stream = new CollectionStream('expired', c.deliver, { reopenDelayMs: 10 });
      streams.push(stream);
      stream.register('s1', registration());
      // No check on initialStart right after open: a 286 can arrive while the stream is
      // still opening, so the cursor may already be cleared by then.
      await stream.open();

      await db.collection('expired').insertOne({ n: 0 });
      await waitFor(() => Object.keys(Reflect.get(stream, 'initialStart') as object).length === 0, 'expiry detected');
      await waitFor(async () => (await FanoutCursor.countDocuments({ collectionName: 'expired' })) === 0, 'cursor cleared');
      await waitFor(streamReady(stream), 'live reopen');

      const { insertedId } = await db.collection('expired').insertOne({ n: 1 });
      await waitFor(() => c.ids('s1').length === 1, 'delivery after reopen');
      assert.deepEqual(c.ids('s1'), [String(insertedId)]);
    });
  });

  // MongoDB 8.0 may batch a bulk insert outside a transaction into fewer oplog
  // entries, so events can share a cluster time. Each must still arrive once, in order.
  it('delivers every insertMany event once, live and on join-while-live replay', async () => {
    await withConnection(async (db, streams) => {
      const c = collector();
      const stream = new CollectionStream('bulk', c.deliver);
      streams.push(stream);
      stream.register('live', registration());
      await stream.open();

      const { insertedIds } = await db.collection('bulk').insertMany([1, 2, 3, 4, 5].map(n => ({ n })));
      const expected = Object.values(insertedIds).map(String);
      await waitFor(() => c.ids('live').length >= 5, 'live deliveries');

      // register() replays synchronously.
      stream.register('late', registration(new Date(Date.now() - 1000)));
      assert.deepEqual(c.ids('late'), expected);

      const sentinel = await insertSentinel(db, 'bulk', c, 'live');
      assert.deepEqual(c.ids('live'), [...expected, sentinel]);
      assert.deepEqual(c.ids('late'), [...expected, sentinel]);
    });
  });
});
