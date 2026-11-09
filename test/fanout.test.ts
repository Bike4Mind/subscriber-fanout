import { after, afterEach, before, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MAX_SEND_ATTEMPTS, SendFn, SubscriberFanout } from '../src/fanout.js';
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
    if (failing.has(connectionId)) throw new Error('GoneException');
    sent.push({ connectionId, message });
  };
  const updates = (connectionId: string) =>
    sent
      .filter(s => s.connectionId === connectionId && s.message.action === 'data_update')
      .map(s => s.message as IDataSubscriptionUpdateAction);
  return { sent, failing, send, updates };
}

async function startFanout(dbName: string, send: SendFn) {
  const instance = new SubscriberFanout({
    mongoUri: rs.getUri(dbName),
    stage: 'test',
    managementEndpoint: ENDPOINT,
    send,
    pingIntervalMs: 0,
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

  it('drops subscribers after repeated failed pings and closes the stream when none remain', async () => {
    const { name, db } = freshDb();
    const r = recorder();
    r.failing.add('dead');
    const instance = await startFanout(name, r.send);
    const lonely = await insertSubscription(db, { collectionName: 'pinged', query: {}, connectionIds: ['dead'] });
    const mixed = await insertSubscription(db, {
      collectionName: 'mixed',
      query: {},
      connectionIds: ['alive', 'dead'],
    });
    await waitForSubscription(instance, lonely);
    await waitForSubscription(instance, mixed);

    // Each ping writes subscribers back, and the querySubscriptions watcher
    // echoes that write into memory; let it settle so attempts are not rolled back.
    const settle = () => new Promise(resolve => setTimeout(resolve, 300));
    for (let i = 1; i < MAX_SEND_ATTEMPTS; i++) {
      await instance.pingAllSubscribers();
      await settle();
    }
    assert.equal(await db.collection('querySubscriptions').countDocuments({ _id: new ObjectId(lonely) }), 1);
    assert.equal(instance.openStreamCount('pinged'), 1);

    await instance.pingAllSubscribers();
    assert.equal(await db.collection('querySubscriptions').countDocuments({ _id: new ObjectId(lonely) }), 0);
    assert.equal(instance.openStreamCount('pinged'), 0);

    const remaining = await db.collection('querySubscriptions').findOne({ _id: new ObjectId(mixed) });
    assert.deepEqual(
      remaining?.subscribers.map((s: { connectionId: string }) => s.connectionId),
      ['alive']
    );
    assert.equal(instance.openStreamCount('mixed'), 1);
    assert.ok(r.sent.some(s => s.connectionId === 'alive' && s.message.action === 'pong'));
  });
});
