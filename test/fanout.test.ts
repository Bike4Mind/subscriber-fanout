import { after, afterEach, before, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import v8 from 'node:v8';
import mongoose from 'mongoose';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { SendFn, SubscriberFanout, SubscriberFanoutOptions, definitionOf } from '../src/fanout.js';
import { ChangeEvent, CollectionStream, DeliverFn, OpenGate, Registration, waitForStreamInit } from '../src/collectionStream.js';
import { Budget } from '../src/budget.js';
import { FanoutCursor } from '../src/fanoutCursor.js';
import { buildSubscriptionFilter } from '../src/matcher.js';
import { QuerySubscription } from '../src/querySubscription.js';
import { IDataSubscriptionUpdateAction, IMessageDataToClient } from '../src/wireTypes.js';

const { MongoClient, ObjectId } = mongoose.mongo;
type Db = mongoose.mongo.Db;

const ENDPOINT = 'https://test.invalid';

let rs: MongoMemoryReplSet;
let client: mongoose.mongo.MongoClient;
let fanout: SubscriberFanout | undefined;
let dbCounter = 0;

function freshDb() {
  const name = `fanout_test_${++dbCounter}`;
  return { name, db: client.db(name) };
}

function recorder() {
  const sent: Array<{ connectionId: string; message: IMessageDataToClient }> = [];
  const failing = new Set<string>();
  const send: SendFn = async (connectionId, _endpoint, message) => {
    if (failing.has(connectionId)) throw Object.assign(new Error('GoneException'), { name: 'GoneException', $metadata: { httpStatusCode: 410 } });
    sent.push({ connectionId, message });
  };
  const updates = (connectionId: string) =>
    sent
      .filter(s => s.connectionId === connectionId && s.message.action === 'data_update')
      .map(s => s.message as IDataSubscriptionUpdateAction);
  return { sent, failing, send, updates };
}

async function startFanout(dbName: string, send: SendFn, options: Partial<SubscriberFanoutOptions> = {}) {
  const instance = new SubscriberFanout({
    mongoUri: rs.getUri(dbName),
    stage: 'test',
    managementEndpoint: ENDPOINT,
    send,
    pingIntervalMs: 0,
    ...options,
  });
  await instance.start();
  fanout = instance;
  return instance;
}

function subscriber(connectionId: string) {
  return { endpoint: ENDPOINT, connectionId, clientId: `client-${connectionId}`, attempts: 0, createdAt: new Date() };
}

async function insertSubscription(
  db: Db,
  {
    collectionName,
    query,
    fields = {},
    connectionIds,
    updatedAt = new Date(),
  }: {
    collectionName: string;
    query: Record<string, unknown>;
    fields?: Record<string, unknown>;
    connectionIds: string[];
    updatedAt?: Date;
  }
) {
  const _id = new ObjectId();
  await db.collection('querySubscriptions').insertOne({
    _id,
    collectionName,
    queryId: _id.toHexString(),
    query,
    fields,
    subscribers: connectionIds.map(subscriber),
    createdAt: updatedAt,
    updatedAt,
  });
  return _id.toHexString();
}

async function waitFor(predicate: () => boolean | Promise<boolean>, what: string, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  assert.fail(`timed out waiting for ${what}`);
}

async function waitForSubscription(instance: SubscriberFanout, id: string) {
  await waitFor(() => instance.subscribers.has(id), `subscription ${id} to register`);
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

before(async () => {
  rs = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
  client = new MongoClient(rs.getUri());
  await client.connect();
  mock.method(console, 'debug', () => {});
});

afterEach(async () => {
  await fanout?.stop();
  fanout = undefined;
});

after(async () => {
  await client.close();
  await rs.stop();
});

describe('SubscriberFanout', () => {
  it('delivers inserts, updates and soft deletes with fields excluded', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const userId = new ObjectId();
    const id = await insertSubscription(db, {
      collectionName: 'fabfiles',
      query: { userId },
      fields: { vector: false },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);

    const files = db.collection('fabfiles');
    await files.insertOne({ userId: new ObjectId(), name: 'other', vector: [1, 2] });
    const { insertedId } = await files.insertOne({ userId, name: 'mine', vector: [1, 2] });
    await waitFor(() => r.updates('c1').length === 1, 'insert');

    const [inserted] = r.updates('c1');
    assert.equal(inserted.operationType, 'insert');
    assert.equal(inserted.collectionName, 'fabfiles');
    assert.equal(inserted.subscriptionId, 'client-c1');
    assert.equal(inserted.data.id, insertedId.toHexString());
    assert.equal(inserted.data._id, insertedId.toHexString());
    assert.equal(inserted.data.name, 'mine');
    assert.ok(!('vector' in inserted.data));

    await files.updateOne({ _id: insertedId }, { $set: { name: 'renamed' } });
    await waitFor(() => r.updates('c1').length === 2, 'update');
    const updated = r.updates('c1')[1];
    assert.equal(updated.operationType, 'update');
    assert.equal(updated.data.name, 'renamed');
    assert.equal(String(updated.data.userId), userId.toHexString());
    assert.ok(!('vector' in updated.data));

    await files.updateOne({ _id: insertedId }, { $set: { deletedAt: new Date() } });
    await waitFor(() => r.updates('c1').length === 3, 'soft delete');
    const deleted = r.updates('c1')[2];
    assert.equal(deleted.operationType, 'delete');
    assert.deepEqual(Object.keys(deleted.data).sort(), ['_id', 'id']);
  });

  it('shares one stream between subscriptions on the same collection', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const a = await insertSubscription(db, { collectionName: 'things', query: { kind: 'a' }, connectionIds: ['ca'] });
    const b = await insertSubscription(db, { collectionName: 'things', query: { kind: 'b' }, connectionIds: ['cb'] });
    await waitForSubscription(instance, a);
    await waitForSubscription(instance, b);

    await db.collection('things').insertMany([{ kind: 'a' }, { kind: 'b' }, { kind: 'c' }]);
    await waitFor(() => r.updates('ca').length === 1 && r.updates('cb').length === 1, 'both deliveries');
    await new Promise(resolve => setTimeout(resolve, 300));

    assert.equal(r.updates('ca').length, 1);
    assert.equal(r.updates('cb').length, 1);
    assert.equal(r.updates('ca')[0].data.kind, 'a');
    assert.equal(r.updates('cb')[0].data.kind, 'b');
    assert.equal(instance.openStreamCount('things'), 1);

    const cursors = await client
      .db('admin')
      .aggregate([
        { $currentOp: { idleCursors: true } },
        { $match: { ns: `${name}.things`, 'cursor.originatingCommand.pipeline.0.$changeStream': { $exists: true } } },
      ])
      .toArray();
    assert.equal(new Set(cursors.map(c => String(c.cursor.cursorId))).size, 1);
  });

  it('projects each subscription independently from the shared event', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const stripped = await insertSubscription(db, {
      collectionName: 'people',
      query: {},
      fields: { 'mfa.totpSecret': false, 'questions.answer': false },
      connectionIds: ['cs'],
    });
    const full = await insertSubscription(db, { collectionName: 'people', query: {}, connectionIds: ['cf'] });
    await waitForSubscription(instance, stripped);
    await waitForSubscription(instance, full);

    await db.collection('people').insertOne({ mfa: { totpSecret: 's', on: true }, questions: [{ q: 'q', answer: 'a' }] });
    await waitFor(() => r.updates('cs').length === 1 && r.updates('cf').length === 1, 'both deliveries');

    assert.deepEqual(r.updates('cs')[0].data.mfa, { on: true });
    assert.deepEqual(r.updates('cs')[0].data.questions, [{ q: 'q' }]);
    assert.deepEqual(r.updates('cf')[0].data.mfa, { totpSecret: 's', on: true });
    assert.deepEqual(r.updates('cf')[0].data.questions, [{ q: 'q', answer: 'a' }]);
  });

  it('keeps delivering to other subscriptions when one projection throws', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const instance = await startFanout(name, r.send, {
      buildFilter: subscription =>
        subscription.subscribers.some(s => s.connectionId === 'c1')
          ? { matcher: () => true, projector: () => assert.fail('bad projection') }
          : buildSubscriptionFilter(subscription),
    });
    const broken = await insertSubscription(db, { collectionName: 'badProjection', query: {}, connectionIds: ['c1', 'c2'] });
    const healthy = await insertSubscription(db, { collectionName: 'badProjection', query: {}, connectionIds: ['c3'] });
    await waitForSubscription(instance, broken);
    await waitForSubscription(instance, healthy);

    await db.collection('badProjection').insertMany([{ n: 1 }, { n: 2 }]);
    await waitFor(() => r.updates('c3').length === 2, 'delivery to the healthy subscription');
    await sleep(200);
    assert.equal(r.updates('c1').length + r.updates('c2').length, 0);
    assert.equal(warn.mock.calls.filter(call => String(call.arguments[0]).startsWith('projection failed')).length, 2);
    assert.deepEqual(instance.subscribers.get(broken)?.map(s => s.connectionId), ['c1', 'c2']);
  });

  it('applies inclusion and array-path exclusion projections', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const userId = new ObjectId();
    const include = await insertSubscription(db, {
      collectionName: 'users',
      query: { _id: userId },
      fields: { email: true },
      connectionIds: ['inc'],
    });
    const exclude = await insertSubscription(db, {
      collectionName: 'users',
      query: { _id: userId },
      fields: { 'securityQuestions.answer': false, 'mfa.totpSecret': false },
      connectionIds: ['exc'],
    });
    await waitForSubscription(instance, include);
    await waitForSubscription(instance, exclude);

    await db.collection('users').insertOne({
      _id: userId,
      email: 'a@example.com',
      name: 'A',
      securityQuestions: [
        { question: 'q1', answer: 'a1' },
        { question: 'q2', answer: 'a2' },
      ],
      mfa: { totpSecret: 'secret', enabled: true },
    });
    await waitFor(() => r.updates('inc').length === 1 && r.updates('exc').length === 1, 'user deliveries');

    assert.deepEqual(r.updates('inc')[0].data, {
      email: 'a@example.com',
      _id: userId.toHexString(),
      id: userId.toHexString(),
    });
    const excluded = r.updates('exc')[0].data;
    assert.deepEqual(excluded.securityQuestions, [{ question: 'q1' }, { question: 'q2' }]);
    assert.deepEqual(excluded.mfa, { enabled: true });
    assert.equal(excluded.name, 'A');
  });

  it('matches a missing field against null', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const receiverId = new ObjectId();
    const id = await insertSubscription(db, {
      collectionName: 'notifications',
      query: { receiverId, deletedAt: null },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);

    const notifications = db.collection('notifications');
    await notifications.insertOne({ receiverId, n: 1 });
    await notifications.insertOne({ receiverId, n: 2, deletedAt: new Date() });
    await notifications.insertOne({ receiverId, n: 3, deletedAt: null });
    await waitFor(() => r.updates('c1').length === 2, 'null matches');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.deepEqual(
      r.updates('c1').map(u => u.data.n),
      [1, 3]
    );
  });

  it('evaluates $and / $or / $elemMatch with stored ObjectIds', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const sessionId = new ObjectId();
    const userId = new ObjectId();
    const id = await insertSubscription(db, {
      collectionName: 'messages',
      query: {
        $and: [{ sessionId }, { $or: [{ userId }, { users: { $elemMatch: { userId, permissions: 'read' } } }] }],
      },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);

    await db.collection('messages').insertMany([
      { tag: 'owner', sessionId, userId },
      { tag: 'reader', sessionId, users: [{ userId, permissions: ['read', 'write'] }] },
      { tag: 'writer-only', sessionId, users: [{ userId, permissions: ['write'] }] },
      { tag: 'other-session', sessionId: new ObjectId(), userId },
      { tag: 'other-user', sessionId, userId: new ObjectId() },
      { tag: 'sentinel', sessionId, userId },
    ]);
    await waitFor(() => r.updates('c1').some(u => u.data.tag === 'sentinel'), 'sentinel');
    assert.deepEqual(
      r.updates('c1').map(u => u.data.tag),
      ['owner', 'reader', 'sentinel']
    );
  });

  it('replays buffered events to a subscription that joins a live stream', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const all = await insertSubscription(db, { collectionName: 'notes', query: {}, connectionIds: ['all'] });
    await waitForSubscription(instance, all);

    const joinedAt = new Date(Date.now() - 200);
    const { insertedId: early } = await db.collection('notes').insertOne({ owner: 'b', n: 1 });
    await waitFor(() => r.updates('all').length === 1, 'event buffered on the live stream');

    const late = await insertSubscription(db, {
      collectionName: 'notes',
      query: { owner: 'b' },
      connectionIds: ['late'],
      updatedAt: joinedAt,
    });
    await waitForSubscription(instance, late);
    await waitFor(() => r.updates('late').length === 1, 'replayed event');
    assert.equal(r.updates('late')[0].data._id, early.toHexString());

    const { insertedId: live } = await db.collection('notes').insertOne({ owner: 'b', n: 2 });
    await waitFor(() => r.updates('late').length === 2, 'live event');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.deepEqual(
      r.updates('late').map(u => u.data._id),
      [early.toHexString(), live.toHexString()]
    );
    assert.equal(instance.openStreamCount('notes'), 1);
  });

  it('closes the collection stream when the last subscription goes away', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const emptied = await insertSubscription(db, { collectionName: 'temp', query: {}, connectionIds: ['c1'] });
    const deleted = await insertSubscription(db, { collectionName: 'scratch', query: {}, connectionIds: ['c2'] });
    await waitForSubscription(instance, emptied);
    await waitForSubscription(instance, deleted);
    assert.equal(instance.openStreamCount('temp'), 1);
    assert.equal(instance.openStreamCount('scratch'), 1);

    await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(emptied) }, { $set: { subscribers: [] } });
    await waitFor(() => instance.openStreamCount('temp') === 0, 'temp stream to close');
    await waitFor(
      async () => (await db.collection('querySubscriptions').countDocuments({ _id: new ObjectId(emptied) })) === 0,
      'emptied subscription doc to be deleted'
    );

    await db.collection('querySubscriptions').deleteOne({ _id: new ObjectId(deleted) });
    await waitFor(() => instance.openStreamCount('scratch') === 0, 'scratch stream to close');
    assert.equal(instance.openStreamCount(), 0);
  });

  it('finishes an idle close before it disconnects at stop', async () => {
    const { name, db } = freshDb();
    const instance = await startFanout(name, recorder().send);
    const id = await insertSubscription(db, { collectionName: 'closing', query: {}, connectionIds: ['c1'] });
    await waitForSubscription(instance, id);
    await db.collection('subscriberFanoutCursors').insertOne({ collectionName: 'closing' });

    const removed = instance.deleteSubscription(id).then(() => 'ok', (error: unknown) => String(error));
    await instance.stop();
    fanout = undefined;
    assert.equal(await removed, 'ok');
    assert.equal(await db.collection('subscriberFanoutCursors').countDocuments({ collectionName: 'closing' }), 0);
  });

  it('resumes from the persisted cursor after a restart', async () => {
    const { name, db } = freshDb();
    const first = recorder();
    const id = await insertSubscription(db, { collectionName: 'resumable', query: {}, connectionIds: ['c1'] });
    const instance = await startFanout(name, first.send);
    assert.ok(instance.subscribers.has(id));

    const { insertedId: before } = await db.collection('resumable').insertOne({ n: 1 });
    await waitFor(() => first.updates('c1').length === 1, 'first delivery');
    await instance.stop();
    fanout = undefined;

    const cursor = await db.collection('subscriberFanoutCursors').findOne({ collectionName: 'resumable' });
    assert.ok(cursor?.resumeToken);

    const { insertedIds } = await db.collection('resumable').insertMany([{ n: 2 }, { n: 3 }]);
    const second = recorder();
    await startFanout(name, second.send);
    await waitFor(() => second.updates('c1').length === 2, 'resumed deliveries');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.deepEqual(
      second.updates('c1').map(u => u.data._id),
      [insertedIds[0].toHexString(), insertedIds[1].toHexString()]
    );
    assert.ok(!second.updates('c1').some(u => u.data._id === before.toHexString()));
  });

  it('falls back to a dedicated stream when the query cannot be matched in process', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    const warn = t.mock.method(console, 'warn', () => {});
    const instance = await startFanout(name, r.send);
    const id = await insertSubscription(db, {
      collectionName: 'fallback',
      query: { tag: 'yes', $comment: 'mingo rejects $comment, Mongo accepts it' },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);

    assert.ok(warn.mock.calls.some(call => String(call.arguments[0]).includes(id)));
    assert.equal(instance.dedicatedStreamCount(), 1);
    assert.equal(instance.openStreamCount('fallback'), 0);

    await db.collection('fallback').insertMany([{ tag: 'no' }, { tag: 'yes' }]);
    await waitFor(() => r.updates('c1').length === 1, 'dedicated delivery');
    assert.equal(r.updates('c1')[0].data.tag, 'yes');
  });

  it('drops the document on the server for a dedicated filter and looks it up before matching, sending the ids of one already deleted', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const id = await insertSubscription(db, {
      collectionName: 'lazy',
      query: { tag: 'yes', $comment: 'dedicated' },
      fields: { secret: 0 },
      connectionIds: ['c1'],
    });
    const instance = await startFanout(name, r.send);
    const stream = (Reflect.get(instance, 'dedicatedStreams') as Map<string, CollectionStream>).get(id)!;
    assert.deepEqual(Reflect.get(stream, 'pipeline').at(-1), { $project: { fullDocument: 0 } });

    await db.collection('lazy').insertOne({ tag: 'yes', first: true, secret: 1 });
    await waitFor(() => r.updates('c1').length === 1, 'the first insert');
    const [first] = r.updates('c1');
    assert.equal(first.data.first, true);
    assert.equal(first.data.secret, undefined);

    let asked = false;
    let release!: () => void;
    const gate = new Promise<void>(resolve => (release = resolve));
    const collection = mongoose.connection.collection.bind(mongoose.connection);
    t.mock.method(mongoose.connection, 'collection', (collectionName: string) => {
      const real = collection(collectionName);
      const findOne = real.findOne.bind(real) as (...args: unknown[]) => Promise<unknown>;
      return Object.assign(Object.create(real), {
        findOne: async (...args: unknown[]) => {
          asked = true;
          await gate;
          return findOne(...args);
        },
      });
    });
    const { insertedId } = await db.collection('lazy').insertOne({ tag: 'yes', n: 2 });
    await waitFor(() => asked, 'the lookup');
    await db.collection('lazy').deleteOne({ _id: insertedId });
    release();
    // The server-side filter needs the document, so the delete never matches.
    await waitFor(() => r.updates('c1').length === 2, 'the second insert');
    await sleep(500);
    const [, gone, ...rest] = r.updates('c1');
    assert.deepEqual(rest, []);
    assert.deepEqual(gone, {
      action: 'data_update',
      subscriptionId: 'client-c1',
      collectionName: 'lazy',
      operationType: 'insert',
      data: { id: insertedId.toHexString(), _id: insertedId.toHexString() },
    });
  });

  it('looks a document up once for every dedicated stream that read the change', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const ids = [];
    for (const n of [1, 2, 3]) {
      ids.push(
        await insertSubscription(db, {
          collectionName: 'sharedLookup',
          query: { tag: 'yes', n: { $ne: -n }, $comment: 'dedicated' },
          fields: { secret: 0 },
          connectionIds: [`c${n}`],
        })
      );
    }
    for (const id of ids) await waitForSubscription(instance, id);
    assert.equal(instance.dedicatedStreamCount(), 3);
    await sleep(300);
    const findOne = t.mock.method(mongoose.connection.collection('sharedLookup'), 'findOne');
    await db.collection('sharedLookup').insertOne({ tag: 'yes', secret: 1, n: 0 });
    await waitFor(() => ['c1', 'c2', 'c3'].every(c => r.updates(c).length === 1), 'the change on every stream');
    assert.equal(findOne.mock.callCount(), 1);
    for (const c of ['c1', 'c2', 'c3']) assert.equal(r.updates(c)[0].data.secret, undefined);
  });

  it('starts when a subscription cannot be watched at all, and still serves the others', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    t.mock.method(console, 'warn', () => {});
    const error = t.mock.method(console, 'error', () => {});
    const broken = await insertSubscription(db, {
      collectionName: 'unwatchable',
      query: { tag: 'yes', $comment: 'dedicated' },
      connectionIds: ['c1'],
    });
    await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(broken) }, { $set: { lastChange: 'not json' } });
    const owned = await insertSubscription(db, {
      collectionName: 'unwatchable',
      query: { hasOwnProperty: 'mine', $comment: 'dedicated' },
      connectionIds: ['c2'],
    });

    const instance = await startFanout(name, r.send);
    assert.ok(error.mock.calls.some(call => String(call.arguments[0]).includes(broken)));
    assert.equal(instance.subscribers.has(broken), false);
    assert.equal(instance.dedicatedStreamCount(), 1);

    await db.collection('unwatchable').insertMany([{ hasOwnProperty: 'other' }, { hasOwnProperty: 'mine' }]);
    await waitFor(() => r.updates('c2').length === 1, 'delivery to the watchable subscription');
    assert.equal(r.updates('c2')[0].data.hasOwnProperty, 'mine');
    assert.equal(instance.subscribers.has(owned), true);
  });

  it('delivers nothing to a subscription whose filter MongoDB rejects', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    const warn = t.mock.method(console, 'warn', () => {});
    const instance = await startFanout(name, r.send);
    const rejected = await insertSubscription(db, {
      collectionName: 'rejected',
      query: { tag: { $not: 'yes' } },
      connectionIds: ['c1'],
    });
    const valid = await insertSubscription(db, { collectionName: 'rejected', query: { tag: 'yes' }, connectionIds: ['c2'] });
    await waitForSubscription(instance, rejected);
    await waitForSubscription(instance, valid);
    assert.equal(warn.mock.calls.filter(call => String(call.arguments[0]).includes(rejected)).length, 1);
    assert.equal(instance.dedicatedStreamCount(), 0);

    await db.collection('rejected').insertMany([{ tag: 'no' }, { tag: 'yes' }]);
    await waitFor(() => r.updates('c2').length === 1, 'delivery to the valid subscription');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.equal(r.updates('c1').length, 0);
  });

  it('resumes a dedicated stream after a restart without redelivering', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const first = recorder();
    await insertSubscription(db, {
      collectionName: 'dedicatedResume',
      query: { tag: 'yes', $comment: 'dedicated' },
      connectionIds: ['c1'],
      updatedAt: new Date(Date.now() - 60 * 1000),
    });
    const instance = await startFanout(name, first.send);
    assert.equal(instance.dedicatedStreamCount(), 1);

    const { insertedId: before } = await db.collection('dedicatedResume').insertOne({ tag: 'yes' });
    await waitFor(() => first.updates('c1').length === 1, 'first delivery');
    await new Promise(resolve => setTimeout(resolve, 300));
    await instance.stop();
    fanout = undefined;

    const { insertedId: after } = await db.collection('dedicatedResume').insertOne({ tag: 'yes' });
    const second = recorder();
    await startFanout(name, second.send);
    await waitFor(() => second.updates('c1').length >= 1, 'resumed delivery');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.deepEqual(
      second.updates('c1').map(u => u.data._id),
      [after.toHexString()]
    );
    assert.notEqual(after.toHexString(), before.toHexString());
  });

  it('removes a connection a ping finds gone, and closes the stream when none remain', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    r.failing.add('dead');
    const instance = await startFanout(name, r.send, { disconnect: async () => {} });
    const lonely = await insertSubscription(db, { collectionName: 'pinged', query: {}, connectionIds: ['dead'] });
    const mixed = await insertSubscription(db, {
      collectionName: 'mixed',
      query: {},
      connectionIds: ['alive', 'dead'],
    });
    await waitForSubscription(instance, lonely);
    await waitForSubscription(instance, mixed);

    await instance.pingAllSubscribers();
    await waitFor(
      async () => (await db.collection('querySubscriptions').countDocuments({ _id: new ObjectId(lonely) })) === 0,
      'emptied subscription doc to be deleted'
    );
    await waitFor(() => instance.openStreamCount('pinged') === 0, 'pinged stream to close');

    const remaining = await db.collection('querySubscriptions').findOne({ _id: new ObjectId(mixed) });
    assert.deepEqual(
      remaining?.subscribers.map((s: { connectionId: string }) => s.connectionId),
      ['alive']
    );
    assert.equal(instance.openStreamCount('mixed'), 1);
    assert.ok(r.sent.some(s => s.connectionId === 'alive' && s.message.action === 'pong'));
  });

  it('keeps old-stream semantics for null on a dotted path by matching it server-side', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    t.mock.method(console, 'warn', () => {});
    const instance = await startFanout(name, r.send);
    const id = await insertSubscription(db, {
      collectionName: 'teams',
      query: { 'members.deletedAt': null },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);
    assert.equal(instance.dedicatedStreamCount(), 1);

    await db.collection('teams').insertMany([
      { name: 'all-deleted', members: [{ deletedAt: new Date() }] },
      { name: 'one-active', members: [{ deletedAt: new Date() }, { name: 'x' }] },
    ]);
    await waitFor(() => r.updates('c1').length === 1, 'dotted-null delivery');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.deepEqual(
      r.updates('c1').map(u => u.data.name),
      ['one-active']
    );
  });

  it('reopens the shared stream after the collection is dropped', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    t.mock.method(console, 'warn', () => {});
    const instance = await startFanout(name, r.send);
    const id = await insertSubscription(db, { collectionName: 'dropped', query: { tag: 'yes' }, connectionIds: ['c1'] });
    await waitForSubscription(instance, id);

    await db.collection('dropped').insertOne({ tag: 'yes', n: 1 });
    await waitFor(() => r.updates('c1').length === 1, 'delivery before drop');
    await db.collection('dropped').drop();
    await new Promise(resolve => setTimeout(resolve, 1500));

    await db.collection('dropped').insertOne({ tag: 'yes', n: 2 });
    await waitFor(() => r.updates('c1').length === 2, 'delivery after drop');
    assert.equal(r.updates('c1')[1].data.n, 2);
    assert.equal(instance.openStreamCount('dropped'), 1);
  });

  for (const when of ['while it runs', 'while it is stopped', 'before it first starts'] as const) {
    it(`keeps a dedicated stream delivering when its collection is dropped ${when}`, async t => {
      const { name, db } = freshDb();
      const r = recorder();
      t.mock.method(console, 'warn', () => {});
      t.mock.method(console, 'info', () => {});
      const collection = db.collection('droppedDedicated');
      await collection.insertOne({ tag: 'yes', n: 0 });
      await insertSubscription(db, { collectionName: 'droppedDedicated', query: { tag: 'yes', $comment: 'dedicated' }, connectionIds: ['c1'] });
      if (when === 'before it first starts') await collection.drop();
      let instance = await startFanout(name, r.send);
      assert.equal(instance.dedicatedStreamCount(), 1);
      await collection.insertOne({ tag: 'yes', n: 1 });
      await waitFor(() => r.updates('c1').some(u => u.data.n === 1), 'delivery');
      if (when === 'before it first starts') return;

      if (when === 'while it is stopped') {
        await instance.stop();
        fanout = undefined;
      }
      await collection.drop();
      await collection.insertOne({ tag: 'yes', n: 2 });
      if (when === 'while it is stopped') instance = await startFanout(name, r.send);
      await waitFor(() => r.updates('c1').some(u => u.data.n === 2), 'delivery after the drop');
    });
  }

  it('removes a closed connection from its subscriptions only once the gateway confirms the close', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    const r = recorder();
    const sends = new Map<string, number>();
    const send: SendFn = async (connectionId, endpoint, message) => {
      sends.set(connectionId, (sends.get(connectionId) ?? 0) + 1);
      if (connectionId !== 'ok') throw Object.assign(new Error('TimeoutError'), { name: 'TimeoutError' });
      await r.send(connectionId, endpoint, message);
    };
    const failure = (name: string, status: number) => Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } });
    const instance = await startFanout(name, send, {
      disconnect: async connectionId => {
        if (connectionId === 'unconfirmed') throw failure('TimeoutError', 0);
        if (connectionId === 'gone') throw failure('GoneException', 410);
      },
    });
    const id = await insertSubscription(db, { collectionName: 'confirmed', query: {}, connectionIds: ['ok', 'unconfirmed', 'gone'] });
    await waitForSubscription(instance, id);

    await db.collection('confirmed').insertOne({ n: 0 });
    const listed = async () =>
      ((await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) }))?.subscribers ?? []).map(
        (s: { connectionId: string }) => s.connectionId
      );
    await waitFor(async () => !(await listed()).includes('gone'), 'the gone connection to be removed');
    await db.collection('confirmed').insertMany([{ n: 1 }, { n: 2 }]);
    await waitFor(() => r.updates('ok').length === 3, 'deliveries to the healthy connection');
    await sleep(300);
    assert.deepEqual(await listed(), ['ok', 'unconfirmed']);
    assert.equal(sends.get('unconfirmed'), 1);
  });

  it('keeps a closed connection blocked across a restart until the gateway confirms the close', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    const r = recorder();
    const send: SendFn = (connectionId, endpoint, message) =>
      connectionId === 'hung' && message.action === 'data_update' ? new Promise<void>(() => {}) : r.send(connectionId, endpoint, message);
    const first = await startFanout(name, send, {
      sendTimeoutMs: 200,
      disconnect: async () => {
        throw Object.assign(new Error('InternalServerError'), { $metadata: { httpStatusCode: 500 } });
      },
    });
    const id = await insertSubscription(db, { collectionName: 'carried', query: {}, connectionIds: ['hung', 'ok'] });
    await waitForSubscription(first, id);
    await db.collection('carried').insertOne({ n: 0 });
    await waitFor(async () => (await db.collection('subscriberFanoutCloses').countDocuments({ connectionId: 'hung' })) === 1, 'the close to be recorded');
    await first.stop();
    fanout = undefined;

    let confirm!: () => void;
    const confirmed = new Promise<void>(resolve => (confirm = resolve));
    const second = await startFanout(name, send, { disconnect: () => confirmed });
    await waitForSubscription(second, id);
    await db.collection('carried').insertOne({ n: 1 });
    await waitFor(() => r.updates('ok').length === 2, 'both changes to the open connection');
    assert.equal(r.updates('hung').length, 0);
    confirm();
    const listed = async () =>
      ((await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) }))?.subscribers ?? []).map(
        (s: { connectionId: string }) => s.connectionId
      );
    await waitFor(async () => (await listed()).join() === 'ok', 'the closed connection to leave its subscription');
    await waitFor(async () => (await db.collection('subscriberFanoutCloses').countDocuments()) === 0, 'the close record to clear');
  });

  it('serves no connection it closed or found gone when a subscriber list names it again, and pulls it again', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    const r = recorder();
    const gone = Object.assign(new Error('GoneException'), { name: 'GoneException', $metadata: { httpStatusCode: 410 } });
    const send: SendFn = async (connectionId, endpoint, message) => {
      if (message.action === 'data_update' && connectionId === 'gone') throw gone;
      if (message.action === 'data_update' && connectionId === 'hung') return new Promise<void>(() => {});
      await r.send(connectionId, endpoint, message);
    };
    const options = { sendTimeoutMs: 200, disconnect: async () => {} };
    let instance = await startFanout(name, send, options);
    const id = await insertSubscription(db, { collectionName: 'relisted', query: {}, connectionIds: ['ok', 'hung', 'gone'] });
    await waitForSubscription(instance, id);
    const listed = async () =>
      ((await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) }))?.subscribers ?? []).map(
        (s: { connectionId: string }) => s.connectionId
      );
    const relist = () =>
      db.collection('querySubscriptions').updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: { $each: [subscriber('hung'), subscriber('gone')] } } } as never);

    await db.collection('relisted').insertOne({ n: 0 });
    await waitFor(async () => (await listed()).join() === 'ok', 'both connections to be removed');
    await waitFor(async () => (await db.collection('subscriberFanoutTombstones').countDocuments()) === 2, 'both tombstones');
    await relist();
    await waitFor(async () => (await listed()).join() === 'ok', 'the re-listed connections to be pulled again');
    await db.collection('relisted').insertOne({ n: 1 });
    await waitFor(() => r.updates('ok').length === 2, 'the second change');

    await instance.stop();
    fanout = undefined;
    await relist();
    instance = await startFanout(name, send, options);
    await waitForSubscription(instance, id);
    await waitFor(async () => (await listed()).join() === 'ok', 'the re-listed connections to be pulled after a restart');
    await db.collection('relisted').insertOne({ n: 2 });
    await waitFor(() => r.updates('ok').length === 3, 'the third change');
    await sleep(300);
    assert.deepEqual(instance.subscribers.get(id)?.map(s => s.connectionId), ['ok']);
    assert.equal(await db.collection('subscriberFanoutCloses').countDocuments(), 0);
  });

  it('sends a connection on another gateway one message, then removes it from every subscription', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const sent: string[] = [];
    const send: SendFn = async (connectionId, _endpoint, message) => {
      if (message.action === 'data_update') sent.push(`${connectionId}:${message.data.n}`);
    };
    const instance = await startFanout(name, send);
    const foreign = { ...subscriber('foreign'), endpoint: 'https://other.invalid' };
    const ids = await Promise.all(
      ['foreignA', 'foreignB'].map(collectionName => insertSubscription(db, { collectionName, query: {}, connectionIds: ['ok'] }))
    );
    for (const id of ids) {
      await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: foreign } } as never);
    }
    await waitFor(() => ids.every(id => instance.subscribers.get(id)?.length === 2), 'both lists to name the foreign connection');

    await db.collection('foreignA').insertOne({ n: 1 });
    await waitFor(() => sent.length === 2, 'the first change');
    await db.collection('foreignB').insertOne({ n: 2 });
    await waitFor(() => sent.length === 3, 'the second change');
    await sleep(300);
    assert.deepEqual(sent.sort(), ['foreign:1', 'ok:1', 'ok:2']);
    for (const id of ids) {
      await waitFor(
        async () => (await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) }))?.subscribers.length === 1,
        'the foreign entry to be pulled'
      );
    }
    assert.equal(await db.collection('subscriberFanoutTombstones').countDocuments({ connectionId: 'foreign' }), 1);
  });

  it('removes a connection on another gateway stored without createdAt', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const _id = new ObjectId();
    const { createdAt: _, ...stored } = { ...subscriber('X'), endpoint: 'https://other.invalid' };
    await db.collection('querySubscriptions').insertOne({
      _id,
      collectionName: 'foreignEntry',
      queryId: _id.toHexString(),
      query: {},
      fields: {},
      subscribers: [subscriber('L'), stored],
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const instance = await startFanout(name, r.send);
    // Only the fanout's own pull is under test: the watcher would pull the entry again from the list it reads back.
    (Reflect.get(instance, 'subscriptionWatcher') as mongoose.mongo.ChangeStream).removeAllListeners('change');
    await db.collection('foreignEntry').insertOne({ n: 1 });
    await waitFor(() => r.updates('L').length === 1 && r.updates('X').length === 1, 'the change');
    await waitFor(
      async () => (await db.collection('querySubscriptions').findOne({ _id }))?.subscribers.length === 1,
      'the foreign entry to be pulled',
      5000
    );
    assert.equal(await db.collection('subscriberFanoutTombstones').countDocuments({ connectionId: 'X' }), 1);
  });

  it('holds a send whose connection never completes as a gateway fault, and keeps the connection', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    const env = { AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'test', AWS_SECRET_ACCESS_KEY: 'test', AWS_EC2_METADATA_DISABLED: 'true' };
    const before = Object.fromEntries(Object.keys(env).map(key => [key, process.env[key]]));
    Object.assign(process.env, env);
    t.after(() => {
      for (const [key, value] of Object.entries(before)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    });
    // An address that drops the SYN, so the TCP connect never completes.
    const endpoint = 'http://10.255.255.1:8080';
    const disconnected: string[] = [];
    const instance = new SubscriberFanout({
      mongoUri: rs.getUri(name),
      stage: 'test',
      managementEndpoint: endpoint,
      pingIntervalMs: 0,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    fanout = instance;
    await instance.start();
    const _id = new ObjectId();
    await db.collection('querySubscriptions').insertOne({
      _id,
      collectionName: 'unconnected',
      queryId: _id.toHexString(),
      query: {},
      fields: {},
      subscribers: [{ ...subscriber('K'), endpoint }],
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    await waitForSubscription(instance, _id.toHexString());
    await db.collection('unconnected').insertOne({ n: 1 });
    await sleep(9000);
    assert.deepEqual(disconnected, []);
    assert.deepEqual(warn.mock.calls.map(call => String(call.arguments[0])).filter(message => message.startsWith('closing')), []);
  });

  it('settles a change to a subscription without subscribers at once, so the token moves while its delete runs', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const live = await insertSubscription(db, { collectionName: 'emptyDelete', query: { k: 'live' }, connectionIds: ['c1'] });
    const empty = await insertSubscription(db, { collectionName: 'emptyDelete', query: { k: 'empty' }, connectionIds: [] });
    await waitForSubscription(instance, live);
    await waitForSubscription(instance, empty);
    const deleteOne = QuerySubscription.deleteOne.bind(QuerySubscription);
    const deletes = t.mock.method(QuerySubscription, 'deleteOne', (...args: Parameters<typeof deleteOne>) =>
      sleep(10_000).then(() => deleteOne(...args))
    );
    const cursor = () => db.collection('subscriberFanoutCursors').findOne({ collectionName: 'emptyDelete' });
    assert.equal(await cursor(), null);

    const started = Date.now();
    await db.collection('emptyDelete').insertMany([
      { k: 'empty', n: 1 },
      { k: 'live', n: 2 },
    ]);
    await waitFor(async () => (await cursor()) !== null, 'the token to be saved', 8000);
    assert.ok(Date.now() - started < 7500, `saved after ${Date.now() - started}ms`);
    assert.equal(deletes.mock.callCount(), 1);
    assert.ok(await db.collection('querySubscriptions').findOne({ _id: new ObjectId(empty) }));
    assert.deepEqual(r.updates('c1').map(u => u.data.n), [2]);
  });

  it('closes the connections of a stream whose resume position has left the oplog, and reopens it live', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'log', () => {});
    const id = await insertSubscription(db, {
      collectionName: 'expiring',
      query: { tag: 'yes', $comment: 'dedicated' },
      connectionIds: ['c1'],
    });
    const disconnected: string[] = [];
    // Unconfirmed, so c1 stays in the subscription document but is dropped from every list that comes in.
    const instance = await startFanout(name, r.send, {
      disconnect: async connectionId => {
        disconnected.push(connectionId);
        return new Promise(() => {});
      },
    });
    const dedicated = (): mongoose.mongo.ChangeStream | undefined =>
      Reflect.get((Reflect.get(instance, 'dedicatedStreams') as Map<string, CollectionStream>).get(id)!, 'stream');
    const original = dedicated();
    assert.ok(original);

    original.emit('error', new Error('resume point may no longer be in the oplog'));
    await waitFor(() => dedicated() !== undefined && dedicated() !== original, 'dedicated stream to reopen');
    assert.deepEqual(disconnected, ['c1']);
    await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: subscriber('c2') } as never });
    await waitFor(() => instance.subscribers.get(id)?.map(s => s.connectionId).join() === 'c2', 'the new subscriber');
    await new Promise(resolve => setTimeout(resolve, 300));

    await db.collection('expiring').insertOne({ tag: 'yes' });
    await waitFor(() => r.updates('c2').length === 1, 'delivery after reopen');
    assert.equal(r.updates('c1').length, 0);
  });

  it('redelivers changes whose sends were in flight at shutdown after a restart', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    await insertSubscription(db, { collectionName: 'inflight', query: {}, connectionIds: ['fast', 'slow'] });
    const first = recorder();
    // The slow connection never answers for anything after the first change.
    const send: SendFn = (connectionId, endpoint, message) =>
      connectionId === 'slow' && message.action === 'data_update' && Number(message.data.n) > 0
        ? new Promise(() => {})
        : first.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send);

    const { insertedId: settled } = await db.collection('inflight').insertOne({ n: 0 });
    await waitFor(() => first.updates('slow').length === 1, 'settled delivery');
    const { insertedIds } = await db.collection('inflight').insertMany([{ n: 1 }, { n: 2 }, { n: 3 }]);
    await waitFor(() => first.updates('fast').length === 4, 'fast deliveries');
    await instance.stop(2000);
    fanout = undefined;

    const second = recorder();
    await startFanout(name, second.send);
    await waitFor(() => second.updates('slow').length === 3, 'redelivery');
    await sleep(300);
    assert.deepEqual(
      second.updates('slow').map(u => u.data._id),
      Object.values(insertedIds).map(id => id.toHexString())
    );
    assert.ok(!second.updates('fast').some(u => u.data._id === settled.toHexString()));
  });

  it('does not let a slow connection hold up another subscriber', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    // Models the SDK's shared socket pool, which a slow connection could otherwise fill.
    let sockets = 3;
    const waiting: Array<() => void> = [];
    let release!: () => void;
    const slow = new Promise<void>(resolve => (release = resolve));
    const send: SendFn = async (connectionId, endpoint, message) => {
      while (sockets === 0) await new Promise<void>(resolve => waiting.push(resolve));
      sockets--;
      try {
        if (connectionId === 'slow') await slow;
        await r.send(connectionId, endpoint, message);
      } finally {
        sockets++;
        waiting.shift()?.();
      }
    };
    const instance = await startFanout(name, send, { sendConcurrency: 2 });
    const id = await insertSubscription(db, { collectionName: 'busy', query: {}, connectionIds: ['slow', 'fast'] });
    await waitForSubscription(instance, id);

    await db.collection('busy').insertMany([1, 2, 3, 4, 5, 6].map(n => ({ n })));
    await waitFor(() => r.updates('fast').length === 6, 'fast deliveries while the slow connection is stalled', 3000);
    assert.equal(r.updates('slow').length, 0);
    release();
    await waitFor(() => r.updates('slow').length === 6, 'slow deliveries');
  });

  it('keeps per-connection order when sends finish out of order', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    const send: SendFn = async (connectionId, endpoint, message) => {
      await sleep(Math.random() * 15);
      await r.send(connectionId, endpoint, message);
    };
    const instance = await startFanout(name, send);
    const a = await insertSubscription(db, { collectionName: 'ordered', query: { even: true }, connectionIds: ['c1'] });
    const b = await insertSubscription(db, { collectionName: 'ordered', query: { even: false }, connectionIds: ['c1'] });
    await waitForSubscription(instance, a);
    await waitForSubscription(instance, b);

    const docs = Array.from({ length: 30 }, (_, n) => ({ n, even: n % 2 === 0 }));
    await db.collection('ordered').insertMany(docs);
    await waitFor(() => r.updates('c1').length === 30, 'all deliveries');
    assert.deepEqual(
      r.updates('c1').map(u => u.data.n),
      docs.map(d => d.n)
    );
  });

  it('closes a connection whose queue stalls but not one that keeps up with a burst', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const stuck: Array<() => void> = [];
    const send: SendFn = (connectionId, endpoint, message) =>
      connectionId === 'stuck' ? new Promise<void>(resolve => stuck.push(resolve)) : r.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send, {
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const id = await insertSubscription(db, { collectionName: 'flood', query: {}, connectionIds: ['stuck', 'ok'] });
    await waitForSubscription(instance, id);

    await db.collection('flood').insertMany(Array.from({ length: 1500 }, (_, n) => ({ n })));
    await waitFor(() => r.updates('ok').length === 1500, 'the burst to the healthy connection');
    await waitFor(() => disconnected.length > 0, 'the stalled connection to close');
    await db.collection('flood').insertOne({ n: 1500 });
    assert.deepEqual(disconnected, ['stuck']);
    stuck.forEach(resolve => resolve());
    await waitFor(async () => {
      const doc = await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) });
      return doc?.subscribers.map((s: { connectionId: string }) => s.connectionId).join() === 'ok';
    }, 'stuck subscriber to be removed');
    await waitFor(() => r.updates('ok').length === 1501, 'delivery after the close');
    assert.deepEqual(
      instance.subscribers.get(id)?.map(s => s.connectionId),
      ['ok']
    );
  });

  it('closes a connection whose sends time out and retries one that is throttled', async t => {
    const { name, db } = freshDb();
    const warnings = t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const failure = (name: string, extra: object = {}) => Object.assign(new Error(name), { name, ...extra });
    let throttled = false;
    const send: SendFn = async (connectionId, endpoint, message) => {
      if (connectionId === 'hung') throw failure('TimeoutError');
      if (connectionId === 'throttled' && message.action === 'data_update' && Number(message.data.n) === 0 && !throttled) {
        throttled = true;
        throw failure('LimitExceededException', { $metadata: { httpStatusCode: 429 } });
      }
      await r.send(connectionId, endpoint, message);
    };
    const instance = await startFanout(name, send, { disconnect: async connectionId => void disconnected.push(connectionId) });
    const id = await insertSubscription(db, { collectionName: 'flaky', query: {}, connectionIds: ['hung', 'throttled'] });
    await waitForSubscription(instance, id);

    await db.collection('flaky').insertOne({ n: 0 });
    await waitFor(() => disconnected.length > 0, 'the hung connection to close', 3000);
    assert.deepEqual(disconnected, ['hung']);
    await db.collection('flaky').insertOne({ n: 1 });
    await waitFor(() => r.updates('throttled').length === 2, 'the throttled change, retried, and the next');
    assert.deepEqual(r.updates('throttled').map(u => u.data.n), [0, 1]);
    assert.ok(!warnings.mock.calls.some(call => String(call.arguments[0]).includes('dropped a message')));
    assert.deepEqual(
      instance.subscribers.get(id)?.map(s => s.connectionId),
      ['throttled']
    );
  });

  it('keeps a subscriber that joins while a failing one is being dropped', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    // Each failure takes a while, so the deliveries holding the old list are still running when it changes.
    const send: SendFn = (connectionId, endpoint, message) =>
      connectionId === 'dead'
        ? sleep(200).then(() =>
            Promise.reject(Object.assign(new Error('GoneException'), { name: 'GoneException', $metadata: { httpStatusCode: 410 } }))
          )
        : r.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send, { disconnect: async () => {} });
    const id = await insertSubscription(db, { collectionName: 'handover', query: {}, connectionIds: ['dead'] });
    await waitForSubscription(instance, id);

    await db.collection('handover').insertMany([{ n: 0 }, { n: 1 }, { n: 2 }]);
    await db
      .collection('querySubscriptions')
      .updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: subscriber('fresh') } as never });
    await sleep(800);
    await db.collection('handover').insertOne({ n: 3 });
    await waitFor(() => r.updates('fresh').some(u => u.data.n === 3), 'delivery to the subscriber that joined');
    const doc = await db.collection('querySubscriptions').findOne({ _id: new ObjectId(id) });
    assert.deepEqual(
      doc?.subscribers.map((s: { connectionId: string }) => s.connectionId),
      ['fresh']
    );
  });

  it('keeps delivering on seven collections whose subscribers leave and come back', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    // A slow cursor delete keeps each idle close running past the gap.
    const deleteCursor = FanoutCursor.deleteOne.bind(FanoutCursor);
    t.mock.method(FanoutCursor, 'deleteOne', (...args: Parameters<typeof deleteCursor>) =>
      sleep(4000).then(() => deleteCursor(...args))
    );
    const instance = await startFanout(name, r.send);
    const subscriptions = db.collection('querySubscriptions');
    const range = [...Array(7).keys()];
    const ids = await Promise.all(
      range.map(i => insertSubscription(db, { collectionName: `rejoin${i}`, query: {}, connectionIds: [`c${i}`] }))
    );
    for (const id of ids) await waitForSubscription(instance, id);

    await Promise.all(
      range.map(async i => {
        await subscriptions.updateOne({ queryId: ids[i] }, { $pull: { subscribers: { connectionId: `c${i}` } } as never });
        await sleep(i % 2 ? 3000 : 1500);
        // As the app subscribes: push onto the query's doc if it is still there, or create a new one.
        const { matchedCount } = await subscriptions.updateOne(
          { queryId: ids[i] },
          { $push: { subscribers: subscriber(`c${i}`) } as never, $set: { updatedAt: new Date() } }
        );
        if (matchedCount === 0) {
          await subscriptions.insertOne({
            collectionName: `rejoin${i}`,
            queryId: ids[i],
            query: {},
            fields: {},
            subscribers: [subscriber(`c${i}`)],
            createdAt: new Date(),
            updatedAt: new Date(),
          });
        }
        await db.collection(`rejoin${i}`).insertOne({ n: i });
        await waitFor(() => r.updates(`c${i}`).length === 1, `delivery on rejoin${i}`, 3000);
      })
    );
  });

  it('redelivers a dedicated stream change whose send was in flight at shutdown', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    await insertSubscription(db, {
      collectionName: 'dedicatedInflight',
      query: { tag: 'yes', $comment: 'dedicated' },
      connectionIds: ['c1'],
      updatedAt: new Date(Date.now() - 60 * 1000),
    });
    const first = recorder();
    const send: SendFn = (connectionId, endpoint, message) =>
      message.action === 'data_update' && Number(message.data.n) > 0
        ? new Promise(() => {})
        : first.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send);
    assert.equal(instance.dedicatedStreamCount(), 1);

    await db.collection('dedicatedInflight').insertOne({ tag: 'yes', n: 0 });
    await waitFor(() => first.updates('c1').length === 1, 'first delivery');
    const { insertedId } = await db.collection('dedicatedInflight').insertOne({ tag: 'yes', n: 1 });
    await sleep(300);
    await instance.stop(1000);
    fanout = undefined;

    const second = recorder();
    await startFanout(name, second.send);
    await waitFor(() => second.updates('c1').length >= 1, 'redelivery');
    await sleep(300);
    assert.deepEqual(
      second.updates('c1').map(u => u.data._id),
      [insertedId.toHexString()]
    );
  });

  it('redelivers replayed changes whose sends were in flight at shutdown', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const first = recorder();
    const send: SendFn = (connectionId, endpoint, message) =>
      connectionId === 'late' ? new Promise(() => {}) : first.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send);
    const early = await insertSubscription(db, { collectionName: 'replayed', query: {}, connectionIds: ['early'] });
    await waitForSubscription(instance, early);
    const { insertedIds } = await db.collection('replayed').insertMany([{ n: 0 }, { n: 1 }, { n: 2 }]);
    await waitFor(() => first.updates('early').length === 3, 'live deliveries');

    // Joins the open stream, so it is served from the replay buffer.
    const late = await insertSubscription(db, {
      collectionName: 'replayed',
      query: {},
      connectionIds: ['late'],
      updatedAt: new Date(Date.now() - 30 * 1000),
    });
    await waitForSubscription(instance, late);
    await sleep(200);
    await instance.stop(1000);
    fanout = undefined;

    const second = recorder();
    await startFanout(name, second.send);
    await waitFor(() => second.updates('late').length >= 3, 'redelivery of the replayed changes');
    assert.deepEqual(
      [...new Set(second.updates('late').map(u => u.data._id))].sort(),
      Object.values(insertedIds)
        .map(id => id.toHexString())
        .sort()
    );
  });

  it('keeps every connection open through a burst fanned out to 200 connections', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const disconnected: string[] = [];
    let delivered = 0;
    const send: SendFn = async (_connectionId, _endpoint, message) => {
      if (message.action === 'data_update') delivered++;
    };
    const instance = await startFanout(name, send, { disconnect: async connectionId => void disconnected.push(connectionId) });
    const ids = await Promise.all(
      Array.from({ length: 10 }, (_, s) =>
        insertSubscription(db, {
          collectionName: 'fanned',
          query: {},
          connectionIds: Array.from({ length: 20 }, (_, i) => `c${s}-${i}`),
        })
      )
    );
    for (const id of ids) await waitForSubscription(instance, id);

    await db.collection('fanned').insertMany(Array.from({ length: 100 }, (_, n) => ({ n, pad: 'x'.repeat(10_000) })));
    await waitFor(() => delivered === 20_000 || disconnected.length > 0, 'every delivery', 30000);
    assert.equal(disconnected.length, 0, `closed ${disconnected.length} connections`);
  });

  it('closes a slow connection once the budget is spent, and keeps delivering to the fast one', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const send: SendFn = async (connectionId, endpoint, message) => {
      if (connectionId === 'slow' && message.action === 'data_update') await sleep(1500);
      await r.send(connectionId, endpoint, message);
    };
    const instance = await startFanout(name, send, {
      maxQueuedBytes: 150_000,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const id = await insertSubscription(db, { collectionName: 'spent', query: {}, connectionIds: ['slow', 'fast'] });
    await waitForSubscription(instance, id);
    await db.collection('spent').insertOne({ n: -1 });
    await waitFor(() => r.updates('slow').length === 1, 'the slow connection to answer once', 5000);

    await db.collection('spent').insertMany(Array.from({ length: 10 }, (_, n) => ({ n, pad: 'x'.repeat(20_000) })));
    // Within 8s: a body waiting for room waives the 10s a connection must be slow by L.
    await waitFor(() => disconnected.length > 0, 'the slow connection to close', 8000);
    await waitFor(() => r.updates('fast').length === 11, 'every change to the fast connection');
    assert.deepEqual(disconnected, ['slow']);
    assert.deepEqual(r.updates('fast').map(u => u.data.n), [-1, ...Array(10).keys()]);
  });

  it('deletes an emptied subscription once rather than on every change', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const send: SendFn = async () => {
      throw Object.assign(new Error('TimeoutError'), { name: 'TimeoutError' });
    };
    const instance = await startFanout(name, send, { disconnect: async () => {} });
    const id = await insertSubscription(db, { collectionName: 'emptied', query: {}, connectionIds: ['hung'] });
    await waitForSubscription(instance, id);
    // The pull never lands, so the doc keeps the closed connection and cannot be deleted.
    t.mock.method(QuerySubscription, 'updateMany', () => new Promise(() => {}));
    const deletes = t.mock.method(QuerySubscription, 'deleteOne');

    await db.collection('emptied').insertOne({ n: 0 });
    await waitFor(() => instance.subscribers.get(id)?.length === 0, 'the hung connection to close');
    await db.collection('emptied').insertMany(Array.from({ length: 50 }, (_, n) => ({ n: n + 1 })));
    await sleep(1000);
    assert.ok(deletes.mock.callCount() <= 1, `${deletes.mock.callCount()} deletes`);
  });

  it('sends a message to a hung connection once, and closes the connection at the request timeout', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    const disconnected: string[] = [];
    let sends = 0;
    const send: SendFn = (_connectionId, _endpoint, message) =>
      message.action === 'data_update' ? (sends++, new Promise<void>(() => {})) : Promise.resolve();
    const instance = await startFanout(name, send, {
      sendTimeoutMs: 1500,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const id = await insertSubscription(db, { collectionName: 'aging', query: {}, connectionIds: ['stuck'] });
    await waitForSubscription(instance, id);

    await db.collection('aging').insertMany([{ n: 0 }, { n: 1 }, { n: 2 }]);
    const started = Date.now();
    await waitFor(() => disconnected.length > 0, 'the hung connection to close', 10_000);
    // Cut at its threshold, the first message keeps running without its slot until the request timer ends it.
    const ms = Date.now() - started;
    assert.ok(ms >= 1400 && ms < 3000, `closed after ${ms}ms`);
    assert.equal(sends, 1);
    assert.ok(warn.mock.calls.some(call => String(call.arguments[0]) === 'closing connection stuck: a send went unanswered past the request timeout'));
  });

  it('cuts hung attempts so they do not hold the send slots', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const send: SendFn = (connectionId, endpoint, message) =>
      ['c0', 'c1'].includes(connectionId) && message.action === 'data_update'
        ? new Promise<void>(() => {})
        : r.send(connectionId, endpoint, message);
    const instance = await startFanout(name, send, { sendConcurrency: 2, disconnect: async () => {} });
    const ids: string[] = [];
    for (let k = 0; k < 6; k++) {
      ids.push(await insertSubscription(db, { collectionName: 'slots', query: { k }, connectionIds: [`c${k}`] }));
    }
    for (const id of ids) await waitForSubscription(instance, id);

    const started = Date.now();
    await db.collection('slots').insertMany(Array.from({ length: 6 }, (_, k) => ({ k })));
    await waitFor(() => [2, 3, 4, 5].every(k => r.updates(`c${k}`).length === 1), 'the healthy connections', 3000);
    // Well before the 5s request timeout that would otherwise free the slots.
    assert.ok(Date.now() - started < 2500, `delivered after ${Date.now() - started}ms`);
  });

  it('does not count the time a connection waits for a send slot towards its drain', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const send: SendFn = async (connectionId, endpoint, message) => {
      await sleep(50);
      await r.send(connectionId, endpoint, message);
    };
    const instance = await startFanout(name, send, {
      sendConcurrency: 1,
      maxDrainMs: 1000,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const a = await insertSubscription(db, { collectionName: 'slotWait', query: { k: 'a' }, connectionIds: ['a'] });
    const b = await insertSubscription(db, { collectionName: 'slotWait', query: { k: 'b' }, connectionIds: ['b'] });
    await waitForSubscription(instance, a);
    await waitForSubscription(instance, b);

    // Each holds 15 POSTs of about 50ms alone, but sharing one slot they take about 1.5s of wall time.
    await db.collection('slotWait').insertMany(Array.from({ length: 30 }, (_, n) => ({ n, k: n % 2 ? 'b' : 'a' })));
    await waitFor(() => r.updates('a').length === 15 && r.updates('b').length === 15, 'every change');
    assert.deepEqual(disconnected, []);
  });

  it('keeps fast connections open when a burst they share passes the budget, and closes the stuck one', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const stuck: Array<() => void> = [];
    // Fast, but slow enough that the fast connections wait well past sendTimeoutMs for a slot.
    const send: SendFn = async (connectionId, endpoint, message) => {
      if (connectionId === 'stuck' && message.action === 'data_update') return new Promise<void>(resolve => stuck.push(resolve));
      await sleep(40);
      await r.send(connectionId, endpoint, message);
    };
    const fast = Array.from({ length: 10 }, (_, i) => `f${i}`);
    const instance = await startFanout(name, send, {
      sendConcurrency: 3,
      maxQueuedBytes: 300_000,
      sendTimeoutMs: 150,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const id = await insertSubscription(db, { collectionName: 'sharedBurst', query: {}, connectionIds: [...fast, 'stuck'] });
    await waitForSubscription(instance, id);

    await db.collection('sharedBurst').insertMany(Array.from({ length: 12 }, (_, n) => ({ n, pad: 'x'.repeat(20_000) })));
    await waitFor(() => fast.every(c => r.updates(c).length === 12) || disconnected.some(c => c !== 'stuck'), 'the burst');
    await waitFor(() => disconnected.length > 0, 'the stuck connection to close');
    assert.deepEqual(disconnected, ['stuck']);
    for (const c of fast) assert.deepEqual(r.updates(c).map(u => u.data.n), [...Array(12).keys()]);
    stuck.forEach(resolve => resolve());
  });

  it('pauses the stream holding the budget while connections that are not slow catch up, and resumes without losing any', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    const info = t.mock.method(console, 'info', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const send: SendFn = async (connectionId, endpoint, message) => {
      await sleep(30);
      await r.send(connectionId, endpoint, message);
    };
    const connections = ['h0', 'h1', 'h2', 'h3', 'h4'];
    const instance = await startFanout(name, send, {
      maxQueuedBytes: 200_000,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const id = await insertSubscription(db, { collectionName: 'pausing', query: {}, connectionIds: connections });
    await waitForSubscription(instance, id);

    await db.collection('pausing').insertMany(Array.from({ length: 40 }, (_, n) => ({ n, pad: 'x'.repeat(20_000) })));
    await waitFor(() => connections.every(c => r.updates(c).length === 40), 'every change');
    for (const c of connections) assert.deepEqual(r.updates(c).map(u => u.data.n), [...Array(40).keys()]);
    assert.deepEqual(disconnected, []);
    assert.ok(
      warn.mock.calls.some(call =>
        /(over the 200000 byte budget; pausing pausing, which holds \d+|over the 250000 byte line; pausing it); P (about [\d.]+s \(D=\d+, c\/m=\d+, L=\d+ms, k=5\)|unknown)/.test(
          String(call.arguments[0])
        )
      )
    );
    assert.ok(info.mock.calls.some(call => /resuming pausing/.test(String(call.arguments[0]))));
  });

  it('reports a heap shortfall once the first 1000 subscriptions of a startup load project one', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'info', () => {});
    const errors: string[] = [];
    const docs = Array.from({ length: 1500 }, (_, i) => {
      const _id = new ObjectId();
      return { _id, collectionName: 'many', queryId: _id.toHexString(), query: { n: i }, fields: {}, subscribers: [subscriber(`m${i}`)] };
    });
    await db.collection('querySubscriptions').insertMany(docs);
    const stats = v8.getHeapStatistics.bind(v8);
    let loadedAtReport = -1;
    let instance: SubscriberFanout | undefined;
    t.mock.method(console, 'error', (message: string) => {
      errors.push(message);
      if (message.startsWith('loading 1500 subscriptions')) loadedAtReport = instance?.subscribers.size ?? -1;
    });
    t.mock.method(v8, 'getHeapStatistics', () => {
      const real = stats();
      return { ...real, heap_size_limit: real.used_heap_size + 30 * 1024 * 1024 };
    });
    instance = new SubscriberFanout({ mongoUri: rs.getUri(name), stage: 'test', managementEndpoint: ENDPOINT, send: async () => {}, pingIntervalMs: 0 });
    fanout = instance;
    await instance.start();
    const report = errors.find(message => message.startsWith('loading 1500 subscriptions'));
    assert.match(report ?? '', /as the first 1000 took, with a 16MB delivery budget needs a heap limit of at least \d+MB, \d+MB over the \d+MB heap/);
    assert.equal(loadedAtReport, 1000);
  });

  it('evicts replay windows from the stream whose window holds the most, oldest first within it', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const quiet = await insertSubscription(db, { collectionName: 'quietWindow', query: {}, connectionIds: ['q'] });
    const busy = await insertSubscription(db, { collectionName: 'busyWindow', query: {}, connectionIds: ['b'] });
    await waitForSubscription(instance, quiet);
    await waitForSubscription(instance, busy);
    await sleep(300);
    await db.collection('quietWindow').insertMany(Array.from({ length: 3 }, (_, n) => ({ n, pad: 'q'.repeat(1000) })));
    await waitFor(() => r.updates('q').length === 3, 'the quiet changes');
    await db.collection('busyWindow').insertMany(Array.from({ length: 10 }, (_, n) => ({ n, pad: 'b'.repeat(20_000) })));
    await waitFor(() => r.updates('b').length === 10, 'the busy changes');
    const streams = Reflect.get(instance, 'streamsByCollection') as Map<string, CollectionStream>;
    const [q, b] = [streams.get('quietWindow')!, streams.get('busyWindow')!];
    await waitFor(() => q.replayOnly().length === 3 && b.replayOnly().length === 10, 'both windows to be replay-only');
    const budget = Reflect.get(instance, 'budget') as Budget;
    const target = budget.stored - 5 * 20_000;
    (Reflect.get(instance, 'evictReplay') as (over: () => boolean) => void).call(instance, () => budget.stored > target);
    assert.equal(q.replayOnly().length, 3);
    const left = b.replayOnly().map(event => (event.change as { fullDocument?: { n: number } }).fullDocument?.n);
    assert.ok(left.length > 0 && left.length <= 5, `${left}`);
    assert.deepEqual(left, [...Array(10).keys()].slice(10 - left.length));
  });

  it('closes joiners whose replay window was evicted for memory, ten at once and then one a second', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const disconnected: string[] = [];
    const instance = await startFanout(name, r.send, { disconnect: async connectionId => void disconnected.push(connectionId) });
    const id = await insertSubscription(db, { collectionName: 'evictedWindow', query: {}, connectionIds: ['c0'] });
    await waitForSubscription(instance, id);
    await sleep(300);
    const joinedAt = new Date(Date.now() - 2000);
    await db.collection('evictedWindow').insertMany([{ n: 1 }, { n: 2 }]);
    await waitFor(() => r.updates('c0').length === 2, 'the changes');
    const stream = (Reflect.get(instance, 'streamsByCollection') as Map<string, CollectionStream>).get('evictedWindow')!;
    await waitFor(() => stream.replayOnly().length === 2, 'the window to be replay-only');
    (Reflect.get(instance, 'evictReplay') as (over: () => boolean) => void).call(instance, () => true);
    assert.equal(stream.replayOnly().length, 0);

    const joiners = Array.from({ length: 12 }, (_, n) => ({ ...subscriber(`j${n}`), createdAt: joinedAt }));
    await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: { $each: joiners } } } as never);
    await waitFor(() => disconnected.length === 10, 'ten joiners to be closed');
    const tenAt = Date.now();
    await sleep(300);
    assert.equal(disconnected.length, 10);
    await waitFor(() => disconnected.length === 12, 'the other joiners to be closed');
    assert.ok(Date.now() - tenAt >= 1700, `${Date.now() - tenAt}ms`);
    assert.deepEqual(disconnected.slice(0, 10).sort(), joiners.slice(0, 10).map(j => j.connectionId).sort());
    assert.deepEqual(disconnected.slice(10), ['j10', 'j11']);
    assert.deepEqual(instance.subscribers.get(id)?.map(s => s.connectionId), ['c0']);
    assert.ok(warn.mock.calls.some(call => /^closing 10 connections, j0 first: its replay window was evicted for memory/.test(String(call.arguments[0]))));
  });

  it('keeps a burst to 500 subscriptions inside a 32MB budget without pausing or closing', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    const disconnected: string[] = [];
    let delivered = 0;
    const send: SendFn = async (_connectionId, _endpoint, message) => {
      if (message.action === 'data_update') delivered++;
    };
    const instance = await startFanout(name, send, {
      maxQueuedBytes: 32 * 1024 * 1024,
      disconnect: async connectionId => void disconnected.push(connectionId),
    });
    const docs = Array.from({ length: 500 }, (_, i) => {
      const _id = new ObjectId();
      return { _id, collectionName: 'wide', queryId: _id.toHexString(), query: {}, fields: {}, subscribers: [subscriber(`w${i}`)] };
    });
    await db.collection('querySubscriptions').insertMany(docs);
    await waitFor(() => docs.every(d => instance.subscribers.has(d._id.toHexString())), 'every subscription to register');

    await db.collection('wide').insertMany(Array.from({ length: 40 }, (_, n) => ({ n, pad: 'x'.repeat(10_000) })));
    await waitFor(() => delivered === 20_000 || disconnected.length > 0, 'every delivery', 60000);
    assert.equal(disconnected.length, 0, `closed ${disconnected.length} connections`);
    assert.ok(!warn.mock.calls.some(call => String(call.arguments[0]).includes('pausing')));
  });

  for (const [kind, query] of [
    ['shared', {}],
    ['dedicated', { $comment: 'dedicated' }],
  ] as const) {
    it(`replays recent changes to a subscriber that joins an existing ${kind} subscription`, async t => {
      const { name, db } = freshDb();
      t.mock.method(console, 'warn', () => {});
      const r = recorder();
      const instance = await startFanout(name, r.send);
      const id = await insertSubscription(db, { collectionName: 'joined', query, connectionIds: ['first'] });
      await waitForSubscription(instance, id);
      assert.equal(instance.dedicatedStreamCount(), kind === 'dedicated' ? 1 : 0);
      await sleep(300);

      // Written after the joiner's initial fetch and before its subscribe lands.
      const { insertedId: missed } = await db.collection('joined').insertOne({ n: 0 });
      await waitFor(() => r.updates('first').length === 1, 'delivery to the first subscriber');
      await db
        .collection('querySubscriptions')
        .updateOne({ _id: new ObjectId(id) }, { $push: { subscribers: subscriber('joiner') } as never });
      await waitFor(() => instance.subscribers.get(id)?.length === 2, 'the joiner to register');
      const { insertedId: live } = await db.collection('joined').insertOne({ n: 1 });
      await waitFor(() => r.updates('first').length === 2, 'live delivery');
      await sleep(300);
      assert.deepEqual(
        r.updates('joiner').map(u => u.data._id),
        [missed.toHexString(), live.toHexString()]
      );
      assert.equal(r.updates('first').length, 2);
    });
  }

  it('delivers a large update on a dedicated stream and skips an event over 16MB', async t => {
    const { name, db } = freshDb();
    const warn = t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'info', () => {});
    const r = recorder();
    const instance = await startFanout(name, r.send);
    // A projection only the server can apply keeps the document on the stream.
    const id = await insertSubscription(db, {
      collectionName: 'hugeDedicated',
      query: { $comment: 'dedicated' },
      fields: { 'x.0': 0 },
      connectionIds: ['c1'],
    });
    await waitForSubscription(instance, id);
    assert.equal(instance.dedicatedStreamCount(), 1);
    await sleep(300);
    const huge = db.collection<{ _id?: string | mongoose.mongo.ObjectId; [field: string]: unknown }>('hugeDedicated');

    const { insertedId } = await huge.insertOne({ big: 'a'.repeat(8_400_000) });
    await huge.updateOne({ _id: insertedId }, { $set: { big: 'b'.repeat(8_400_000) } });
    await waitFor(() => r.updates('c1').length === 2, 'large insert and update');
    assert.equal(r.updates('c1')[1].operationType, 'update');
    assert.equal(String(r.updates('c1')[1].data.big)[0], 'b');

    const _id = 'k'.repeat(10_000);
    const padding = 16 * 1024 * 1024 - 200 - mongoose.mongo.BSON.calculateObjectSize({ _id, pad: '' });
    await huge.insertMany([{ n: 'before' }, { _id, pad: 'p'.repeat(padding) }, { n: 'after' }]);
    await waitFor(() => r.updates('c1').length === 4, 'the changes around the oversized one');
    assert.deepEqual(
      r.updates('c1').slice(2).map(u => u.data.n),
      ['before', 'after']
    );
    assert.ok(warn.mock.calls.some(call => String(call.arguments[0]).startsWith('skipped a change on hugeDedicated')));
  });

  it('logs the delivery budget it sizes from the heap, and the heap per subscription', async t => {
    const { name, db } = freshDb();
    const info = t.mock.method(console, 'info', () => {});
    for (let n = 0; n < 3; n++) await insertSubscription(db, { collectionName: 'sized', query: { n }, connectionIds: [`c${n}`] });
    const instance = await startFanout(name, recorder().send);
    const budget = Reflect.get(instance, 'budget') as Budget;
    assert.ok(budget.limit >= 16 * 1024 * 1024 && budget.limit <= 256 * 1024 * 1024);
    assert.ok(
      info.mock.calls.some(call =>
        /^delivery budget \d+MB of a \d+MB heap, \d+MB in use after loading 3 subscriptions at about \d+KB each; documents up to about \d+MB are delivered, 64 POSTs in flight at once$/.test(
          String(call.arguments[0])
        )
      )
    );
  });

  it('stops within its deadline when Mongo is down', async t => {
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'warn', () => {});
    const down = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
    const downClient = new MongoClient(down.getUri());
    await downClient.connect();
    const db = downClient.db('down');
    await insertSubscription(db, { collectionName: 'things', query: {}, connectionIds: ['c1'] });
    const r = recorder();
    const instance = new SubscriberFanout({
      mongoUri: down.getUri('down'),
      stage: 'test',
      managementEndpoint: ENDPOINT,
      send: r.send,
      pingIntervalMs: 0,
    });
    try {
      await instance.start();
      await db.collection('things').insertOne({ n: 1 });
      await waitFor(() => r.updates('c1').length === 1, 'delivery');
      await downClient.close();
      await down.stop();

      const started = Date.now();
      await instance.stop(1000);
      assert.ok(Date.now() - started < 1500, `stop took ${Date.now() - started}ms`);
    } finally {
      await mongoose.disconnect();
      await down.stop();
    }
  });

  it('logs a failed subscription write instead of crashing', async t => {
    const { name, db } = freshDb();
    const errors = t.mock.method(console, 'error', () => {});
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    t.after(() => process.off('unhandledRejection', onUnhandled));
    const r = recorder();
    r.failing.add('dead');
    const instance = await startFanout(name, r.send, { disconnect: async () => {} });
    const _id = new ObjectId();
    await db.collection('querySubscriptions').insertOne({
      _id,
      collectionName: 'hiccup',
      queryId: _id.toHexString(),
      query: {},
      fields: {},
      subscribers: [subscriber('alive'), subscriber('dead')],
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    await waitForSubscription(instance, _id.toHexString());

    t.mock.method(QuerySubscription, 'updateMany', () => Promise.reject(new Error('mongo hiccup')));
    await db.collection('hiccup').insertOne({ n: 1 });
    await waitFor(() => errors.mock.calls.some(call => String(call.arguments[0]).includes('mongo hiccup')), 'logged');
    await db.collection('hiccup').insertOne({ n: 2 });
    await waitFor(() => r.updates('alive').length === 2, 'delivery after the failed write');
    assert.deepEqual(unhandled, []);
  });

  it('reopens the querySubscriptions watcher after an error', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'error', () => {});
    const r = recorder();
    const instance = await startFanout(name, r.send);
    const watcher = () => Reflect.get(instance, 'subscriptionWatcher') as mongoose.mongo.ChangeStream;
    const original = watcher();
    original.emit('error', new Error('connection reset'));
    await waitFor(() => watcher() !== original, 'watcher to reopen');

    const id = await insertSubscription(db, { collectionName: 'afterReset', query: {}, connectionIds: ['c1'] });
    await waitForSubscription(instance, id);
    await db.collection('afterReset').insertOne({ n: 1 });
    await waitFor(() => r.updates('c1').length === 1, 'delivery');
  });

  it('reloads every subscription when the watcher loses its resume position', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const kept = await insertSubscription(db, { collectionName: 'resync', query: { k: 'kept' }, connectionIds: ['c1'] });
    const removed = await insertSubscription(db, { collectionName: 'resync', query: { k: 'removed' }, connectionIds: ['c2'] });
    const instance = await startFanout(name, r.send);
    const watcher = () => Reflect.get(instance, 'subscriptionWatcher') as mongoose.mongo.ChangeStream;
    const original = watcher();
    original.emit('error', new Error('Resume of change stream was not possible, as the resume point may no longer be in the oplog.'));
    // Made while no watcher is open, so only the reload sees them.
    const added = await insertSubscription(db, { collectionName: 'resync', query: { k: 'added' }, connectionIds: ['c3'] });
    await db.collection('querySubscriptions').deleteOne({ _id: new ObjectId(removed) });
    await db.collection('querySubscriptions').updateOne({ _id: new ObjectId(kept) }, { $push: { subscribers: subscriber('c4') } as never });

    await waitFor(() => watcher() !== undefined && watcher() !== original, 'the watcher to reopen', 5000);
    await waitForSubscription(instance, added);
    assert.ok(!instance.subscribers.has(removed));
    assert.deepEqual(instance.subscribers.get(kept)?.map(s => s.connectionId), ['c1', 'c4']);
    await db.collection('resync').insertMany([{ k: 'kept' }, { k: 'removed' }, { k: 'added' }]);
    await waitFor(() => ['c1', 'c3', 'c4'].every(c => r.updates(c).length === 1), 'deliveries after the reload');
    assert.equal(r.updates('c2').length, 0);
  });

  it('reloads every subscription after querySubscriptions is dropped', async t => {
    const { name, db } = freshDb();
    t.mock.method(console, 'warn', () => {});
    const r = recorder();
    const before = await insertSubscription(db, { collectionName: 'dropped', query: {}, connectionIds: ['c1'] });
    const instance = await startFanout(name, r.send);
    const original = Reflect.get(instance, 'subscriptionWatcher');
    await db.collection('querySubscriptions').drop();
    await waitFor(() => !instance.subscribers.has(before), 'the dropped subscription to be removed');
    await waitFor(() => Reflect.get(instance, 'subscriptionWatcher') !== undefined && Reflect.get(instance, 'subscriptionWatcher') !== original, 'the watcher to reopen');
    const after = await insertSubscription(db, { collectionName: 'dropped', query: {}, connectionIds: ['c2'] });
    await waitForSubscription(instance, after);
    await db.collection('dropped').insertOne({ n: 1 });
    await waitFor(() => r.updates('c2').length === 1, 'delivery to the new subscription');
    assert.equal(r.updates('c1').length, 0);
  });

  it('applies subscription changes made after the startup load and before the watcher opens', async t => {
    const { name, db } = freshDb();
    const r = recorder();
    const removed = await insertSubscription(db, { collectionName: 'loadGap', query: {}, connectionIds: ['c1'] });
    let added = '';
    // Called once the subscriptions are loaded and before the querySubscriptions watcher opens.
    const deleteMany = FanoutCursor.deleteMany.bind(FanoutCursor);
    t.mock.method(FanoutCursor, 'deleteMany', async (filter: object) => {
      added = await insertSubscription(db, { collectionName: 'loadGap', query: {}, connectionIds: ['c2'] });
      await db.collection('querySubscriptions').deleteOne({ _id: new ObjectId(removed) });
      return deleteMany(filter);
    });
    const instance = await startFanout(name, r.send);

    await waitForSubscription(instance, added);
    await waitFor(() => !instance.subscribers.has(removed), 'the deleted subscription to be removed');
    await db.collection('loadGap').insertOne({ n: 1 });
    await waitFor(() => r.updates('c2').length === 1, 'delivery to the added subscription');
    assert.equal(r.updates('c1').length, 0);
  });
});

describe('CollectionStream', () => {
  type Delivered = { subscriptionId: string; change: ChangeEvent };

  function registration(query: Record<string, unknown>, overrides: Partial<Registration> = {}): Registration {
    return { ...buildSubscriptionFilter({ query }), updatedAt: new Date(), ...overrides };
  }

  const deliverTo =
    (delivered: Delivered[]): DeliverFn =>
    (subscriptionId, { change }, _projector, settled) => {
      delivered.push({ subscriptionId, change });
      settled();
      return true;
    };

  async function withConnection(run: (db: Db) => Promise<void>) {
    const { name, db } = freshDb();
    await mongoose.connect(rs.getUri(name));
    try {
      await run(db);
    } finally {
      await mongoose.disconnect();
    }
  }

  const currentStream = (stream: CollectionStream) =>
    Reflect.get(stream, 'stream') as mongoose.mongo.ChangeStream | undefined;

  it('keeps delivering to other subscriptions when one filter throws', async t => {
    t.mock.method(console, 'warn', () => {});
    await withConnection(async db => {
      const delivered: Delivered[] = [];
      const stream = new CollectionStream('projections', deliverTo(delivered));
      stream.register(
        'broken',
        registration(
          {},
          {
            matcher: () => {
              throw new Error('bad filter');
            },
          }
        )
      );
      stream.register('healthy', registration({}));
      await stream.open();
      await db.collection('projections').insertOne({ n: 1 });
      await waitFor(() => delivered.length === 1, 'healthy delivery');
      assert.equal(delivered[0].subscriptionId, 'healthy');
      await stream.close('idle');
    });
  });

  const busy = (ms: number) => {
    const end = performance.now() + ms;
    while (performance.now() < end);
  };
  const n = (event: { change: ChangeEvent }) => (event.change as { fullDocument?: { n: number } }).fullDocument?.n;

  it('matches an event in slices, so what waits runs before every registration is visited', async () => {
    await withConnection(async db => {
      const log: string[] = [];
      const stream = new CollectionStream('sliced', (subscriptionId, _event, _projector, settled) => {
        if (subscriptionId === 'r0') setImmediate(() => log.push('between'));
        settled();
        return true;
      });
      for (let i = 0; i < 20; i++) stream.register(`r${i}`, registration({}, { matcher: () => (busy(1), log.push(`r${i}`), true) }));
      await stream.open();
      await db.collection('sliced').insertOne({ n: 1 });
      await waitFor(() => log.includes('r19') && log.includes('between'), 'matching');
      assert.ok(log.indexOf('between') < log.indexOf('r19'), log.join());
      await stream.close('idle');
    });
  });

  it('gives a registration that joins during the matching each event once, and replays the event being matched to a joiner', async () => {
    await withConnection(async db => {
      const delivered: string[] = [];
      const replayed: string[] = [];
      const longAgo = new Date(Date.now() - 60_000);
      const stream: CollectionStream = new CollectionStream('joining', (subscriptionId, event, _projector, settled) => {
        delivered.push(`${subscriptionId}:${n(event)}`);
        if (n(event) === 1 && subscriptionId === 'r5') {
          stream.register('late', registration({}, { updatedAt: longAgo }));
          for (const id of ['r0', 'r19']) {
            stream.replay(id, longAgo, (joined, replay, _projector, done) => {
              replayed.push(`${joined}:${n(replay)}`);
              done();
              return true;
            });
          }
        }
        settled();
        return true;
      });
      for (let i = 0; i < 20; i++) stream.register(`r${i}`, registration({}, { matcher: () => (busy(1), true) }));
      await stream.open();
      await db.collection('joining').insertOne({ n: 1 });
      await db.collection('joining').insertOne({ n: 2 });
      await waitFor(() => delivered.includes('late:2'), 'the second change to the late registration');
      assert.deepEqual(delivered.filter(d => d.startsWith('late:')), ['late:1', 'late:2']);
      assert.deepEqual(replayed, ['r0:1', 'r19:1']);
      await stream.close('idle');
    });
  });

  it('stops reading while too many events wait to be matched, and matches all of them in order', async t => {
    const warn = t.mock.method(console, 'warn', () => {});
    const info = t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const delivered: number[] = [];
      const stream = new CollectionStream(
        'held',
        (_subscriptionId, event, _projector, settled) => {
          delivered.push(n(event)!);
          settled();
          return true;
        },
        { matchQueueEvents: 4 }
      );
      stream.register('r', registration({}, { matcher: () => (busy(20), true) }));
      await stream.open();
      await db.collection('held').insertMany(Array.from({ length: 30 }, (_, i) => ({ n: i })));
      await waitFor(() => delivered.length === 30, 'every change');
      assert.deepEqual(delivered, [...Array(30).keys()]);
      assert.ok(warn.mock.calls.some(call => /events on held wait to be matched, about \d+ms of matching; stopped reading/.test(String(call.arguments[0]))));
      assert.ok(info.mock.calls.some(call => /events on held wait to be matched; reading again/.test(String(call.arguments[0]))));
      await stream.close('idle');
    });
  });

  it('matches what a paused stream has read, and reads the rest in order once resumed', async t => {
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const delivered: number[] = [];
      const stream = new CollectionStream('pausedQueue', (_subscriptionId, event, _projector, settled) => {
        delivered.push(n(event)!);
        settled();
        return true;
      });
      stream.register('r', registration({}, { matcher: () => (busy(20), true) }));
      await stream.open();
      await db.collection('pausedQueue').insertMany(Array.from({ length: 10 }, (_, i) => ({ n: i })));
      await waitFor(() => delivered.length > 0, 'the first change');
      stream.pause(1);
      await waitFor(() => stream.queued === 0, 'what was read to be matched');
      const read = delivered.length;
      await db.collection('pausedQueue').insertOne({ n: 10 });
      await sleep(300);
      assert.equal(delivered.length, read);
      stream.resume();
      await waitFor(() => delivered.length === 11, 'every change');
      assert.deepEqual(delivered, [...Array(11).keys()]);
      await stream.close('idle');
    });
  });

  it('resets the backoff after a clean reopen', async t => {
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const delivered: Delivered[] = [];
      const stream = new CollectionStream('flaky', deliverTo(delivered), {
        reopenDelayMs: 10,
      });
      stream.register('s1', registration({}));
      await stream.open();

      for (let attempt = 0; attempt < 8; attempt++) {
        const before = currentStream(stream);
        assert.ok(before);
        before.emit('error', new Error('transient network error'));
        await waitFor(() => currentStream(stream) !== undefined && currentStream(stream) !== before, 'reopen');
        await waitFor(() => Reflect.get(stream, 'retries') === 0, 'clean init');
      }

      await db.collection('flaky').insertOne({ n: 1 });
      await waitFor(() => delivered.length === 1, 'delivery after reopens');
      await stream.close('idle');
    });
  });

  it('does not retry the expired start position after the token expires', async t => {
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'log', () => {});
    t.mock.method(console, 'info', () => {});
    await withConnection(async () => {
      const stream = new CollectionStream('expired', () => true, { reopenDelayMs: 10 });
      stream.register('s1', registration({}));
      await stream.open();
      assert.ok('startAtOperationTime' in (Reflect.get(stream, 'initialStart') as object));

      const before = currentStream(stream);
      before?.emit('error', new Error('Resume of change stream was not possible, as the resume point may no longer be in the oplog.'));
      await waitFor(() => currentStream(stream) !== undefined && currentStream(stream) !== before, 'live reopen');
      assert.deepEqual(Reflect.get(stream, 'initialStart'), {});
      await stream.close('idle');
    });
  });

  it('leaves no cursor behind when it closes idle with a token write in flight', async t => {
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const delivered: Delivered[] = [];
      const stream = new CollectionStream('idleClose', deliverTo(delivered));
      stream.register('s1', registration({}));
      await stream.open();
      await db.collection('idleClose').insertOne({ n: 1 });
      await waitFor(() => delivered.length === 1, 'delivery');

      const persist = Reflect.get(stream, 'persistToken') as () => Promise<void>;
      void persist.call(stream);
      await stream.close('idle');
      assert.equal(await FanoutCursor.countDocuments({ collectionName: 'idleClose' }), 0);

      // A late event after close must neither deliver nor re-arm the token write.
      const handleChange = Reflect.get(stream, 'handleChange') as (change: unknown) => void;
      handleChange.call(stream, delivered[0].change);
      assert.equal(delivered.length, 1);
      assert.equal(Reflect.get(stream, 'persistTimer'), undefined);
    });
  });
  it('parks a collection whose stream keeps failing while others keep delivering', async t => {
    const errors = t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'info', () => {});
    const exit = t.mock.method(process, 'exit', () => undefined as never);
    await withConnection(async db => {
      await db.createCollection('aView', { viewOn: 'base', pipeline: [] });
      const viewStream = new CollectionStream('aView', () => true, { reopenDelayMs: 5, maxReopenDelayMs: 20 });
      viewStream.register('s1', registration({}));
      const delivered: Delivered[] = [];
      const healthy = new CollectionStream('base', deliverTo(delivered));
      healthy.register('s2', registration({}));
      await Promise.all([viewStream.open(), healthy.open()]);

      await waitFor(() => Reflect.get(viewStream, 'retries') > 8, 'repeated reopen attempts');
      assert.ok(errors.mock.calls.filter(call => String(call.arguments[0]).includes('aView')).length > 8);
      await db.collection('base').insertOne({ n: 1 });
      await waitFor(() => delivered.length === 1, 'delivery on the healthy collection');
      assert.equal(exit.mock.callCount(), 0);
      await Promise.all([viewStream.close('idle'), healthy.close('idle')]);
    });
  });

  it('delivers an update whose full event is over 16MB and skips an event that is', async t => {
    const warn = t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const delivered: Delivered[] = [];
      const stream = new CollectionStream('huge', deliverTo(delivered));
      stream.register('s1', registration({}));
      await stream.open();
      const huge = db.collection<{ _id?: string | mongoose.mongo.ObjectId; [field: string]: unknown }>('huge');

      // updateDescription repeats the 8.4MB field the looked-up document already carries.
      const { insertedId } = await huge.insertOne({ big: 'a'.repeat(8_400_000) });
      await huge.updateOne({ _id: insertedId }, { $set: { big: 'b'.repeat(8_400_000) } });
      await waitFor(() => delivered.length === 2, 'large insert and update');
      const update = delivered[1].change as { operationType: string; fullDocument?: { big: string } };
      assert.equal(update.operationType, 'update');
      assert.equal(update.fullDocument?.big[0], 'b');

      // Near the 16MB document cap, a long _id pushes even the bare insert event past the limit.
      const _id = 'k'.repeat(10_000);
      const padding = 16 * 1024 * 1024 - 200 - mongoose.mongo.BSON.calculateObjectSize({ _id, pad: '' });
      await huge.insertMany([{ n: 'before' }, { _id, pad: 'p'.repeat(padding) }, { n: 'after' }]);
      await waitFor(() => delivered.length === 4, 'the changes around the oversized one');
      assert.deepEqual(
        delivered.slice(2).map(d => (d.change as { fullDocument?: { n: string } }).fullDocument?.n),
        ['before', 'after']
      );
      assert.ok(warn.mock.calls.some(call => String(call.arguments[0]).startsWith('skipped a change on huge')));
      await stream.close('idle');
    });
  });

  it('charges the replay buffer to the budget, evicts what only it holds, and trims it while the collection is quiet', async t => {
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const budget = new Budget(1e9);
      const stream = new CollectionStream('bulky', () => true, { budget, bufferMaxAgeMs: 300 });
      stream.register('s1', registration({}));
      await stream.open();
      await db.collection('bulky').insertMany(Array.from({ length: 10 }, () => ({ blob: 'x'.repeat(20_000) })));
      await waitFor(() => stream.replayOnly().length === 10, 'events');

      // The raw and normalized copies share each 20KB document's strings, which are charged once.
      assert.ok(budget.stored > 10 * 20_000 && budget.stored < 10 * 25_000, `charged ${budget.stored}`);
      const before = budget.stored;
      stream.evict(stream.replayOnly()[0]);
      assert.equal(stream.replayOnly().length, 9);
      assert.ok(budget.stored < before);
      await waitFor(() => stream.replayOnly().length === 0, 'timed trim', 2000);
      assert.equal(budget.stored, 0);
      await stream.close('idle');
    });
  });

  it('skips an event that cannot fit the budget and keeps reading', async t => {
    t.mock.method(console, 'info', () => {});
    const warn = t.mock.method(console, 'warn', () => {});
    await withConnection(async db => {
      const budget = new Budget(50_000);
      const delivered: unknown[] = [];
      const stream = new CollectionStream('admit', (_id, event, _projector, settled) => {
        delivered.push((event.change as { fullDocument?: { n: number } }).fullDocument?.n);
        settled();
        return true;
      }, { budget });
      stream.register('s1', registration({}));
      await stream.open();
      await db.collection('admit').insertMany([{ n: 1 }, { n: 2, blob: 'x'.repeat(30_000) }, { n: 3 }]);
      await waitFor(() => delivered.length === 2, 'deliveries');
      assert.deepEqual(delivered, [1, 3]);
      assert.ok(warn.mock.calls.some(call => /skipped a change on admit .* over the 50000 byte budget/.test(String(call.arguments[0]))));
      await stream.close('idle');
    });
  });

  it('sizes its batches to about 1MB from the latest events, 16 before any', async t => {
    t.mock.method(console, 'info', () => {});
    await withConnection(async db => {
      const stream = new CollectionStream('batches', () => true);
      stream.register('s1', registration({}));
      const batchSize = () => (Reflect.get(stream, 'batchSize') as () => number).call(stream);
      assert.equal(batchSize(), 16);
      await stream.open();
      await db.collection('batches').insertMany([{ pad: 'x'.repeat(200_000) }, { pad: 'x'.repeat(10) }]);
      await waitFor(() => (Reflect.get(stream, 'recentSizes') as number[]).length === 2, 'events');
      assert.equal(batchSize(), 5);
      await stream.close('idle');
    });
  });

  it('lets four opens run at once and starts the next as one finishes its turn', async () => {
    const gate = new OpenGate();
    const releases = await Promise.all([1, 2, 3, 4].map(() => gate.acquire()));
    let fifth = false;
    const next = gate.acquire().then(release => {
      fifth = true;
      return release;
    });
    await sleep(20);
    assert.equal(fifth, false);
    releases[0]();
    releases[0]();
    (await next)();
    assert.equal(fifth, true);
    const sixth = gate.acquire();
    const seventh = gate.acquire();
    let seventhGranted = false;
    void seventh.then(() => (seventhGranted = true));
    await sixth;
    await sleep(20);
    assert.equal(seventhGranted, false, 'a double release frees one turn');
  });

  it('narrows from where the stream got to when no change has arrived yet', async t => {
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'info', () => {});
    await withConnection(async () => {
      const stream = new CollectionStream('narrowStart', () => true);
      // No updatedAt, as after an expired token: the stream opens live.
      stream.register('s1', registration({}, { updatedAt: undefined }));
      await stream.open();
      const before = currentStream(stream);
      assert.ok(before);
      await waitForStreamInit(before);

      const reopens: Array<{ start: object; mode: string }> = [];
      const openStream = Reflect.get(stream, 'openStream') as (start: object, mode: string) => Promise<void>;
      Reflect.set(stream, 'openStream', (start: object, mode: string) => {
        reopens.push({ start, mode });
        return openStream.call(stream, start, mode);
      });
      before.emit('error', Object.assign(new Error('BSONObjectTooLarge'), { code: 10334 }));
      await waitFor(() => reopens.length === 1, 'narrow reopen');
      assert.equal(reopens[0].mode, 'narrow');
      assert.ok('startAfter' in reopens[0].start);
      await stream.close('idle');
    });
  });
});

describe('definitionOf', () => {
  it('is a 128-bit digest that changes with the collection, the query or the fields', () => {
    const base = { collectionName: 'c', query: { tag: 'x'.repeat(10_000) }, fields: { a: 1 } };
    const digest = definitionOf(base);
    assert.equal(Buffer.from(digest, 'base64').length, 16);
    assert.equal(definitionOf({ ...base, query: { tag: 'x'.repeat(10_000) } }), digest);
    for (const changed of [{ ...base, collectionName: 'd' }, { ...base, query: { tag: 'y' } }, { ...base, fields: { a: 0 } }]) {
      assert.notEqual(definitionOf(changed), digest);
    }
  });
});
