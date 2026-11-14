import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { SubscriberFanout, SubscriberFanoutOptions } from '../src/fanout.js';
import { CollectionStream } from '../src/collectionStream.js';
import { Budget } from '../src/budget.js';
import { buildSubscriptionFilter } from '../src/matcher.js';
import { IDataSubscriptionUpdateAction } from '../src/wireTypes.js';

// A fanout that is never started, with changes fed to its streams directly: the tests set the batches and the
// timing, and nothing reaches Mongo.
mongoose.set('bufferCommands', false);

const { ObjectId } = mongoose.mongo;
type ObjectId = mongoose.mongo.ObjectId;

const ENDPOINT = 'https://test.invalid';
const START = Date.UTC(2026, 0, 1);
const LIMIT = 1_000_000;

const flush = () => new Promise(resolve => setImmediate(resolve));
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function advance(ms: number, step = 10) {
  for (let t = 0; t < ms; t += step) {
    mock.timers.tick(step);
    await flush();
  }
}

const forbidden = () => Object.assign(new Error('Forbidden'), { name: 'ForbiddenException', $metadata: { httpStatusCode: 403 } });

const entry = (connectionId: string, createdAt = new Date()) => ({
  endpoint: ENDPOINT,
  connectionId,
  clientId: `client-${connectionId}`,
  attempts: 0,
  createdAt,
});
type Entry = ReturnType<typeof entry>;

let seq = 0;

// An update as a stream that looks its documents up reads it: without the document.
function update(collection: string, _id: ObjectId, operationType = 'update') {
  seq++;
  return {
    _id: { _data: `token-${seq}` },
    operationType,
    ns: { db: 'test', coll: collection },
    documentKey: { _id },
    clusterTime: new mongoose.mongo.Timestamp({ t: Math.floor(Date.now() / 1000), i: seq }),
  };
}

function change(collection: string, document: Record<string, unknown>) {
  const _id = new ObjectId();
  return { ...update(collection, _id, 'insert'), fullDocument: { _id, ...document } };
}

// As the driver emits a batch: every change of it before the match loop's next turn.
function feed(stream: CollectionStream, changes: object[]) {
  for (const c of changes) (stream as unknown as { handleChange(change: object, from: undefined): void }).handleChange(c, undefined);
}

let warns: string[] = [];
const fanouts: SubscriberFanout[] = [];

beforeEach(() => {
  mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: START });
  warns = [];
  mock.method(console, 'warn', (message: unknown) => void warns.push(String(message)));
  for (const level of ['info', 'debug', 'error'] as const) mock.method(console, level, () => {});
});

afterEach(async () => {
  for (const fanout of fanouts.splice(0)) {
    (Reflect.get(fanout, 'scheduler') as { stop(): void }).stop();
    (Reflect.get(fanout, 'closes') as { stop(): void }).stop();
    for (const stream of (Reflect.get(fanout, 'allStreams') as () => CollectionStream[]).call(fanout)) await stream.close('idle');
  }
  mock.timers.reset();
  mock.reset();
});

type Answer = (connectionId: string, update?: IDataSubscriptionUpdateAction) => unknown;

function harness(answer: Answer = () => {}, options: Partial<SubscriberFanoutOptions> = {}) {
  const got: Record<string, Array<{ id: string; tag: unknown; at: number }>> = {};
  const calls: Record<string, number> = {};
  const disconnected: string[] = [];
  const closedAt: Record<string, number> = {};
  const fanout = new SubscriberFanout({
    mongoUri: 'mongodb://unused',
    stage: 'test',
    managementEndpoint: ENDPOINT,
    pingIntervalMs: 0,
    maxQueuedBytes: LIMIT,
    send: async (connectionId, _endpoint, message) => {
      calls[connectionId] = (calls[connectionId] ?? 0) + 1;
      const update = message.action === 'data_update' ? message : undefined;
      await answer(connectionId, update);
      if (update) (got[connectionId] ??= []).push({ id: update.data.id, tag: update.data.tag, at: Date.now() - START });
    },
    disconnect: async connectionId => {
      disconnected.push(connectionId);
      closedAt[connectionId] = Date.now() - START;
    },
    ...options,
  });
  fanouts.push(fanout);
  const subscribe = async (collectionName: string, query: object, connectionIds: string[]) => {
    const id = new ObjectId().toHexString();
    const subscribers = connectionIds.map(c => entry(c));
    await fanout.addSubscription({ id, collectionName, query, fields: {}, subscribers, updatedAt: new Date(0) } as never, { deferOpen: true });
    const stream =
      (Reflect.get(fanout, 'dedicatedStreams') as Map<string, CollectionStream>).get(id) ??
      (Reflect.get(fanout, 'streamsByCollection') as Map<string, CollectionStream>).get(collectionName)!;
    const saved: string[] = [];
    Reflect.set(stream, 'tokens', { load: async () => undefined, save: async (token: string) => void saved.push(token), clear: async () => {} });
    return { id, stream, subscribers, saved };
  };
  const relist = (id: string, collectionName: string, subscribers: Entry[]) =>
    fanout.updateSubscription({ id, collectionName, query: {}, fields: {}, subscribers, updatedAt: new Date() } as never);
  return {
    fanout,
    got,
    calls,
    disconnected,
    closedAt,
    subscribe,
    relist,
    tags: (connectionId: string) => (got[connectionId] ?? []).map(r => r.tag),
    budget: Reflect.get(fanout, 'budget') as Budget,
  };
}

const closing = () => warns.filter(w => w.startsWith('closing'));

// The highest the heap reaches, checked at every charge.
function watchHeap(budget: Budget) {
  const seen = { peak: budget.heap };
  const add = budget.add.bind(budget);
  budget.add = (bytes: number, stream?: object) => {
    add(bytes, stream);
    seen.peak = Math.max(seen.peak, budget.heap);
  };
  return seen;
}

const bodiesOf = (h: ReturnType<typeof harness>) =>
  Reflect.get(h.fanout, 'bodies') as { waiters: Array<{ waiter: CollectionStream }>; wake(): void };
const line = (h: ReturnType<typeof harness>) => bodiesOf(h).waiters.map(w => w.waiter.label);

// Over the budget with room beside the bodies for the first waiter's only: each builder waits its turn.
async function overBudget(h: ReturnType<typeof harness>, run: () => Promise<void>) {
  h.budget.heap += LIMIT / 2;
  h.budget.bodies += LIMIT;
  try {
    await run();
  } finally {
    h.budget.heap -= LIMIT / 2;
    h.budget.bodies -= LIMIT / 2;
  }
}
const makeRoom = (h: ReturnType<typeof harness>) => {
  h.budget.bodies -= LIMIT / 2;
  bodiesOf(h).wake();
};

describe('403s', () => {
  it('closes a connection after three 403s in a row that no other connection on the endpoint shares', async () => {
    const h = harness(() => {
      throw forbidden();
    });
    const { stream } = await h.subscribe('c', {}, ['X']);
    feed(stream, [change('c', { tag: 'a' })]);
    await advance(1000);
    assert.equal(h.calls.X, 3);
    assert.deepEqual(h.disconnected, ['X']);
    assert.ok(h.closedAt.X <= 600);
    assert.match(closing()[0], /^closing connection X: 403 not shared by the endpoint/);
  });

  it('closes none of many connections all getting 403s until their oldest message has been sending for 60s', async () => {
    const h = harness(() => {
      throw forbidden();
    });
    const conns = Array.from({ length: 50 }, (_, i) => `c${i}`);
    let stream: CollectionStream | undefined;
    for (const c of conns) stream = (await h.subscribe('c', { to: c }, [c])).stream;
    for (const c of conns) {
      feed(stream!, [1, 2, 3].map(n => change('c', { to: c, tag: n })));
      await advance(100);
    }
    await advance(54_500);
    assert.deepEqual(h.disconnected, []);
    await advance(6000);
    assert.equal(h.disconnected.length, 50);
    assert.ok(closing().every(w => w.includes('its oldest message has been sending for 60s')));
  });

  it('closes none of 1000 connections while every other POST gets a 403 for 10s, and delivers everything in order', async () => {
    let posts = 0;
    const h = harness(
      () => {
        if (Date.now() - START < 10_000 && posts++ % 2 === 0) throw forbidden();
      },
      { maxQueuedBytes: 16 * LIMIT }
    );
    const conns = Array.from({ length: 1000 }, (_, i) => `c${i}`);
    const { stream } = await h.subscribe('c', {}, conns);
    for (let n = 0; n < 12; n++) {
      feed(stream, [change('c', { tag: n })]);
      await advance(1000);
    }
    await advance(20_000);
    assert.deepEqual(h.disconnected, []);
    const all = Array.from({ length: 12 }, (_, n) => n);
    for (const c of conns) assert.deepEqual(h.tags(c), all, c);
  });

  it('starts the count again after a 403 another connection on the endpoint shares', async () => {
    const h = harness(connectionId => {
      if ((connectionId === 'X' && h.calls.X <= 5) || (connectionId === 'Y' && h.calls.Y === 1)) throw forbidden();
    });
    const { stream } = await h.subscribe('c', { to: 'X' }, ['X']);
    await h.subscribe('c', { to: 'Y' }, ['Y']);
    feed(stream, [change('c', { to: 'X', tag: 'x' })]);
    await advance(450);
    feed(stream, [change('c', { to: 'Y', tag: 'y' })]);
    await advance(10_000);
    assert.deepEqual(h.disconnected, []);
    assert.equal(h.calls.X, 6);
    assert.deepEqual(h.tags('X'), ['x']);
    assert.deepEqual(h.tags('Y'), ['y']);
  });

  it("keeps counting a connection's 403s while another connection on the endpoint is answered", async () => {
    const h = harness(connectionId => {
      if (connectionId === 'X') throw forbidden();
    });
    const { stream } = await h.subscribe('c', { to: 'X' }, ['X']);
    await h.subscribe('c', { to: 'Y' }, ['Y']);
    feed(stream, [change('c', { to: 'X', tag: 'x' })]);
    for (let n = 0; n < 20; n++) {
      feed(stream, [change('c', { to: 'Y', tag: n })]);
      await advance(50);
    }
    assert.deepEqual(h.disconnected, ['X']);
    assert.ok(h.closedAt.X <= 600);
    assert.equal(h.tags('Y').length, 20);
  });

  it('never counts a 403 to a ping against the pinged connection', async () => {
    const h = harness(() => {
      throw forbidden();
    });
    await h.subscribe('c', {}, ['X']);
    for (let n = 0; n < 4; n++) {
      const pinging = h.fanout.pingAllSubscribers();
      await advance(100);
      await pinging;
    }
    assert.equal(h.calls.X, 4);
    assert.deepEqual(h.disconnected, []);
  });

  it('closes only the connections refused on their own, and delivers the rest in order once the gateway recovers', async () => {
    let recovered = false;
    const errors: Record<string, () => Error> = {
      H: () => Object.assign(new Error('connect EHOSTUNREACH 10.0.0.1:443'), { code: 'EHOSTUNREACH' }),
      N: () => Object.assign(new Error('connect ENETUNREACH 10.0.0.1:443'), { code: 'ENETUNREACH' }),
      P: () => Object.assign(new Error('Could not load credentials from any providers'), { name: 'CredentialsProviderError' }),
      F: forbidden,
      B: () => Object.assign(new Error('Invalid connectionId'), { name: 'BadRequestException', $metadata: { httpStatusCode: 400 } }),
    };
    const h = harness(connectionId => {
      if (!recovered || connectionId === 'F' || connectionId === 'B') throw errors[connectionId]();
    });
    const { stream } = await h.subscribe('nu', {}, Object.keys(errors));
    for (let n = 0; n < 3; n++) {
      feed(stream, [change('nu', { tag: n })]);
      await advance(800);
    }
    await advance(1500);
    assert.deepEqual([...h.disconnected].sort(), ['B', 'F']);
    assert.ok(closing().some(w => w.startsWith('closing connection F: 403 not shared by the endpoint')));
    assert.ok(closing().some(w => w.startsWith('closing connection B: the gateway rejected a send')));
    recovered = true;
    await advance(6000);
    for (const c of ['H', 'N', 'P']) assert.deepEqual(h.tags(c), [0, 1, 2], c);
  });
});

describe('outages', () => {
  it('delivers each message within 60s to every open connection of a 5000-connection subscription relisted after each close', async () => {
    let down = Infinity;
    let up = Infinity;
    const h = harness(
      async () => {
        const start = Date.now();
        if (start >= down && start < up) {
          await sleep(1);
          throw Object.assign(new Error('connect EHOSTUNREACH 10.0.0.1:443'), { code: 'EHOSTUNREACH' });
        }
        await sleep(20);
        if (start < down && Date.now() >= down) await new Promise(() => {});
      },
      { maxQueuedBytes: 76 * LIMIT }
    );
    const conns = Array.from({ length: 5000 }, (_, i) => `f-${i}`);
    const { id, stream, subscribers } = await h.subscribe('nu', {}, conns);
    const written: number[] = [];
    const write = () => {
      written.push(Date.now() - START);
      feed(stream, [change('nu', { tag: written.length })]);
    };
    write();
    let pulled = 0;
    while (Date.now() - START < 90_000) {
      if (Date.now() - START >= 1000 && down === Infinity) {
        down = Date.now();
        write();
      }
      if (Date.now() - START >= 4000 && up === Infinity) {
        up = Date.now();
        write();
      }
      // Each close's pull comes back as a change of the whole subscription, and handling it holds the event loop.
      if (pulled < h.disconnected.length) {
        const gone = new Set(h.disconnected.slice(0, ++pulled));
        const listed = subscribers.filter(s => !gone.has(s.connectionId)).map(s => ({ ...s, createdAt: new Date(s.createdAt) }));
        const started = performance.now();
        await h.relist(id, 'nu', listed);
        mock.timers.tick(Math.round(performance.now() - started));
      }
      mock.timers.tick(10);
      await flush();
    }
    const closed = new Set(h.disconnected);
    const late = conns.filter(
      c => !closed.has(c) && written.some((at, n) => !(h.got[c] ?? []).some(r => r.tag === n + 1 && r.at <= at + 60_000))
    );
    assert.equal(late.length, 0, `${late.length} open connections missed a deadline, ${closed.size} closed`);
    const logged = closing().reduce((sum, w) => {
      const line = /^closing (?:connection |(\d+) connections)/.exec(w);
      return sum + (line ? Number(line[1] ?? 1) : 0);
    }, 0);
    assert.ok(logged >= closed.size, `${closed.size} closed, ${logged} logged`);
  });
});

describe('memory', () => {
  it('delivers every change while a body waits for room that a later delivery holds', async () => {
    const h = harness(async connectionId => {
      if (connectionId === 'C') await sleep(500);
    });
    const { stream } = await h.subscribe('g', { kind: 'b' }, ['A']);
    await h.subscribe('g', { kind: { $in: ['a', 'b', 'x'] } }, ['B']);
    await h.subscribe('g', { kind: 'c' }, ['C']);
    const ev = (tag: string, kind: string, size: number) => change('g', { tag, kind, pad: tag[0].repeat(size) });
    const xs = Array.from({ length: 8 }, (_, i) => `x${i}`);
    feed(stream, [ev('pre', 'c', 20_000), ev('e0', 'a', 450_000), ev('e1', 'b', 1000), ...xs.map(x => ev(x, 'x', 50_000))]);
    await advance(10_000);
    feed(stream, [ev('late', 'b', 1)]);
    await advance(20_000);
    assert.deepEqual(h.tags('A'), ['e1', 'late']);
    assert.deepEqual(h.tags('B'), ['e0', 'e1', ...xs, 'late']);
    assert.deepEqual(h.tags('C'), ['pre']);
    assert.deepEqual(h.disconnected, []);
  });

  it('delivers every change when the connection holding a built body waits behind one that waits for room', async () => {
    const h = harness(async (connectionId, update) => {
      if (connectionId === 'C') await sleep(500);
      if (update?.data.tag === 's') await sleep(1200);
    });
    const { stream } = await h.subscribe('g', { kind: 'c' }, ['C']);
    await h.subscribe('g', { kind: { $in: ['a', 'x'] } }, ['X']);
    await h.subscribe('g', { kind: { $in: ['s', 'y', 'b'] } }, ['Y']);
    await h.subscribe('g', { kind: 'b' }, ['A']);
    const ev = (tag: string, kind: string, size: number) => change('g', { tag, kind, pad: tag[0].repeat(size) });
    const xs = Array.from({ length: 8 }, (_, i) => `x${i}`);
    feed(stream, [ev('pre', 'c', 20_000), ev('e0', 'a', 450_000), ev('s', 's', 1000), ev('ey', 'y', 1000), ev('e1', 'b', 1000), ...xs.map(x => ev(x, 'x', 50_000))]);
    await advance(5000);
    feed(stream, [ev('late', 'b', 10)]);
    await advance(10_000);
    assert.deepEqual(h.tags('X'), ['e0', ...xs]);
    assert.deepEqual(h.tags('Y'), ['s', 'ey', 'e1', 'late']);
    assert.deepEqual(h.tags('A'), ['e1', 'late']);
    assert.deepEqual(h.disconnected, []);
  });

  it('matches what a stream read while other streams keep delivering', async t => {
    const h = harness(async connectionId => {
      await sleep(connectionId === 'cp' ? 10 : 30);
    });
    const p = await h.subscribe('p', {}, ['cp']);
    const quiet = await Promise.all(['q0', 'q1', 'q2', 'q3'].map(q => h.subscribe(q, {}, [`k${q}`])));
    const feeds: NodeJS.Timeout[] = [];
    t.after(() => feeds.forEach(clearInterval));
    quiet.forEach(({ stream }, i) =>
      setTimeout(() => feeds.push(setInterval(() => feed(stream, [change(stream.collectionName, { pad: 'q'.repeat(200) })]), 50)), i * 12)
    );
    await advance(500);
    feed(p.stream, Array.from({ length: 14 }, (_, n) => change('p', { tag: n, pad: 'p'.repeat(75_000) })));
    await advance(15_000);
    assert.equal(h.tags('cp').length, 14);
    assert.deepEqual(h.disconnected, []);
  });

  it('resumes every paused stream once the budget allows', async () => {
    const h = harness(() => sleep(20));
    const p1 = await h.subscribe('p1', {}, ['c1']);
    const p2 = await h.subscribe('p2', {}, ['c2']);
    await h.subscribe('r', {}, ['k']);
    p1.stream.pause(LIMIT / 8);
    await advance(10);
    p2.stream.pause(LIMIT / 8);
    const pinging = h.fanout.pingAllSubscribers();
    await advance(1000);
    await pinging;
    await advance(4000);
    assert.ok(!p1.stream.paused && !p2.stream.paused);
    feed(p1.stream, [change('p1', { tag: 'one' })]);
    feed(p2.stream, [change('p2', { tag: 'two' })]);
    await advance(1000);
    assert.deepEqual(h.tags('c1'), ['one']);
    assert.deepEqual(h.tags('c2'), ['two']);
  });

  it('resumes the stream paused longest first once it has waited 10s, and admits what it reads next under the ceiling', async () => {
    const h = harness();
    const streams = await Promise.all(['p0', 'p1', 'p2'].map((p, i) => h.subscribe(p, {}, [`c${i}`])));
    const [p0, p1, p2] = streams.map(s => s.stream);
    const ev = (stream: CollectionStream, tag: string) => change(stream.collectionName, { tag, pad: 'x'.repeat(100_000 * (3 - Number(stream.collectionName[1]))) });
    h.budget.add(1.3 * LIMIT);
    feed(p2, [ev(p2, 'first')]);
    await advance(1000);
    feed(p1, [ev(p1, 'first')]);
    await advance(1000);
    feed(p0, [ev(p0, 'first')]);
    assert.ok(p0.paused && p1.paused && p2.paused);
    await advance(10_000);
    assert.ok(p0.paused && p1.paused && p2.paused);
    h.budget.add(-0.45 * LIMIT);
    await advance(250);
    assert.ok(!p2.paused && p1.paused && p0.paused);
    feed(p2, [ev(p2, 'again')]);
    await advance(100);
    assert.deepEqual(h.tags('c2'), ['again']);
    h.budget.add(-0.85 * LIMIT);
    await advance(250);
    assert.ok(!p1.paused && !p0.paused);
    feed(p1, [ev(p1, 'again')]);
    feed(p0, [ev(p0, 'again')]);
    await advance(100);
    assert.deepEqual(h.tags('c1'), ['again']);
    assert.deepEqual(h.tags('c0'), ['again']);
  });

  it("keeps the heap within the ceiling plus one change's overhead while a batch fans out to 1000 connections each", async () => {
    const h = harness(() => sleep(5));
    const connections = Array.from({ length: 1000 }, (_, i) => `c${i}`);
    const { stream } = await h.subscribe('w', {}, connections);
    const seen = watchHeap(h.budget);
    feed(stream, Array.from({ length: 8 }, (_, n) => change('w', { tag: n })));
    h.budget.add(LIMIT);
    for (let ms = 0; ms < 20_000 && h.tags('c999').length < 8; ms += 100) await advance(100);
    h.budget.add(-LIMIT);
    assert.ok(seen.peak > h.budget.ceiling && seen.peak <= h.budget.ceiling + 150 + (56 + 320) * 1000, `${seen.peak}`);
    const all = Array.from({ length: 8 }, (_, n) => n);
    assert.deepEqual(h.tags('c0'), all);
    assert.deepEqual(h.tags('c999'), all);
  });

  it("holds a stream's next change while its own deliveries are held over the ceiling, then matches one whose overhead alone is over it", async () => {
    const h = harness(async connectionId => {
      if (connectionId === 'S') await sleep(3000);
    });
    const wide = await h.subscribe('w', {}, ['S', ...Array.from({ length: 3500 }, (_, i) => `c${i}`)]);
    feed(wide.stream, ['w0', 'w1'].map(tag => change('w', { tag })));
    h.budget.add(1.3 * LIMIT);
    await advance(2900);
    assert.ok(h.budget.overheadOf(wide.stream) > 0);
    assert.ok(Object.values(h.got).every(got => got.every(r => r.tag === 'w0')));
    for (let ms = 0; ms < 10_000 && h.tags('S').length < 2; ms += 100) await advance(100);
    for (const c of ['S', 'c0', 'c3499']) assert.deepEqual(h.tags(c), ['w0', 'w1'], c);
    h.budget.add(-1.3 * LIMIT);
  });

  it("matches a stream's changes while another's slow deliveries keep the heap over the ceiling, and holds that stream's next change until they settle or close", async () => {
    const h = harness(
      async connectionId => {
        if (connectionId === 'slow') await sleep(20_000);
        if (connectionId === 'dead') throw Object.assign(new Error('Service Unavailable'), { $metadata: { httpStatusCode: 503 } });
      },
      { sendTimeoutMs: 30_000 }
    );
    const a = await h.subscribe('a', {}, ['slow', 'dead']);
    const b = await h.subscribe('b', {}, ['k']);
    feed(a.stream, ['a0', 'a1'].map(tag => change('a', { tag })));
    feed(b.stream, ['b0', 'b1'].map(tag => change('b', { tag })));
    h.budget.add(1.3 * LIMIT);
    await advance(1000);
    assert.deepEqual(h.tags('k'), ['b0', 'b1']);
    await advance(29_000);
    assert.deepEqual(h.tags('slow'), ['a0']);
    for (let ms = 0; ms < 60_000 && h.tags('slow').length < 2; ms += 100) await advance(100);
    assert.deepEqual(h.disconnected, ['dead']);
    assert.deepEqual(h.tags('slow'), ['a0', 'a1']);
    assert.ok(h.closedAt.dead >= 60_000 && h.got.slow[1].at - h.closedAt.dead <= 22_000, `${h.closedAt.dead} ${h.got.slow[1].at}`);
    h.budget.add(-1.3 * LIMIT);
  });

  it('matches a stream promptly after the budget shrinks under its read backlog while another stream drains to a slow client', async () => {
    const h = harness(connectionId => sleep(connectionId === 'G' ? 900 : 20));
    const w = await h.subscribe('w', {}, ['G']);
    const s = await h.subscribe('s', {}, ['F']);
    feed(w.stream, Array.from({ length: 30 }, (_, n) => change('w', { tag: n })));
    await advance(300);
    let fed = 0;
    while (h.budget.heap + 25_000 <= h.budget.ceiling) feed(s.stream, [change('s', { tag: fed++, pad: 'x'.repeat(10_000) })]);
    h.budget.limit = 0.6 * LIMIT;
    (Reflect.get(h.fanout, 'wakeMatching') as () => void).call(h.fanout);
    const fedAt = Date.now() - START;
    await advance(1000);
    assert.ok(h.got.F?.[0].at - fedAt < 1000, `${h.got.F?.[0].at} ${fedAt}`);
    for (let ms = 0; ms < 10_000 && h.tags('F').length < fed; ms += 100) await advance(100);
    assert.deepEqual(h.tags('F'), Array.from({ length: fed }, (_, n) => n));
    assert.ok(h.tags('G').length < 30);
  });

  it('holds no stream at the match gate for pings in flight', async () => {
    let pings = 0;
    const h = harness(async (connectionId, update) => {
      if (update || connectionId === 'P') return;
      pings++;
      await sleep(5000);
      pings--;
    });
    const { stream } = await h.subscribe('s', {}, ['P']);
    await h.subscribe('q', {}, Array.from({ length: 10 }, (_, i) => `q${i}`));
    let pinged = false;
    void h.fanout.pingAllSubscribers().then(() => (pinged = true));
    feed(stream, [0, 1, 2].map(n => change('s', { tag: n })));
    h.budget.add(1.3 * LIMIT);
    await advance(500);
    assert.ok(pings > 0 && !pinged);
    assert.deepEqual(h.tags('P'), [0, 1, 2]);
    h.budget.add(-1.3 * LIMIT);
    for (let ms = 0; ms < 60_000 && !pinged; ms += 500) await advance(500);
    assert.ok(pinged);
  });

  it('matches what a stream read while the heap is over the ceiling with no delivery overhead held', async () => {
    const h = harness();
    const { stream } = await h.subscribe('u', {}, ['U']);
    feed(stream, [0, 1, 2].map(n => change('u', { tag: n })));
    h.budget.add(1.3 * LIMIT);
    await advance(100);
    assert.deepEqual(h.tags('U'), [0, 1, 2]);
    h.budget.add(-1.3 * LIMIT);
  });

  it('lets the next builder go once the only subscriber of a delivery waiting for room is closed', async () => {
    const h = harness();
    const s = await h.subscribe('s', { k: 'y' }, ['V']);
    await h.subscribe('s', { k: 'w' }, ['W']);
    const t = await h.subscribe('t', {}, ['F']);
    feed(s.stream, [change('s', { k: 'y', tag: 'sy' })]);
    feed(t.stream, [change('t', { tag: 't0' })]);
    await overBudget(h, async () => {
      await advance(100);
      assert.deepEqual(line(h), ['s', 't']);
      (Reflect.get(h.fanout, 'closeConnection') as Function).call(h.fanout, ENDPOINT, 'V', 'closed in the test');
      makeRoom(h);
      await advance(100);
      assert.deepEqual(line(h), []);
      assert.deepEqual(h.tags('F'), ['t0']);
    });
    assert.deepEqual(h.tags('V'), []);
  });

  it('lets the next builder go once the registration of a delivery waiting for room is replaced', async () => {
    const h = harness();
    const s = await h.subscribe('s', { k: 'y' }, ['V']);
    await h.subscribe('s', { k: 'w' }, ['W']);
    const t = await h.subscribe('t', {}, ['F']);
    feed(s.stream, [change('s', { k: 'y', tag: 'sy' })]);
    feed(t.stream, [change('t', { tag: 't0' })]);
    await overBudget(h, async () => {
      await advance(100);
      assert.deepEqual(line(h), ['s', 't']);
      // The doc's delete has no Mongo here and fails; the registration is gone by then.
      await h.relist(s.id, 's', []).catch(() => {});
      await h.fanout.addSubscription({ id: s.id, collectionName: 's', query: { k: 'z' }, fields: {}, subscribers: [entry('V2')], updatedAt: new Date(0) } as never, {
        deferOpen: true,
      });
      makeRoom(h);
      await advance(100);
      assert.deepEqual(line(h), []);
      assert.deepEqual(h.tags('F'), ['t0']);
    });
    assert.deepEqual(h.tags('V'), []);
  });

  it('keeps the range of a replay waiting for room, and its joiner, and sends the joiner nothing live before it', async () => {
    const h = harness();
    const { id, stream, subscribers } = await h.subscribe('w', {}, ['A']);
    Reflect.set(stream, 'state', 'open');
    feed(stream, ['r0', 'r1', 'r2'].map(tag => change('w', { tag })));
    await advance(1000);
    h.budget.bodies += 2 * LIMIT;
    await h.relist(id, 'w', [...subscribers, entry('J', new Date(START - 5000))]);
    feed(stream, ['l0', 'l1'].map(tag => change('w', { tag })));
    await advance(40_000);
    assert.deepEqual(h.tags('J'), []);
    assert.deepEqual(h.disconnected, []);
    h.budget.bodies -= 2 * LIMIT;
    (Reflect.get(h.fanout, 'bodies') as { wake(): void }).wake();
    await advance(1000);
    assert.deepEqual(h.tags('J'), ['r0', 'r1', 'r2', 'l0', 'l1']);
    assert.deepEqual(h.tags('A'), ['r0', 'r1', 'r2', 'l0', 'l1']);
  });

  it('pings no subscriber of a subscription whose replay at registration is still to be sent', async () => {
    const h = harness();
    const { stream } = await h.subscribe('w', {}, ['A']);
    Reflect.set(stream, 'state', 'open');
    feed(stream, ['r0', 'r1'].map(tag => change('w', { tag })));
    await advance(1000);
    h.budget.bodies += 2 * LIMIT;
    await h.subscribe('w', {}, ['N']);
    await advance(100);
    const pinging = h.fanout.pingAllSubscribers();
    await advance(1000);
    await pinging;
    assert.equal(h.calls.A, 3);
    assert.equal(h.calls.N, undefined);
    h.budget.bodies -= 2 * LIMIT;
    bodiesOf(h).wake();
    await advance(1000);
    assert.deepEqual(h.tags('N'), ['r0', 'r1']);
    assert.equal(h.calls.N, 2);
  });

  it('sends a joiner no more of its replay once it has left the subscription', async () => {
    const h = harness();
    const { id, stream, subscribers } = await h.subscribe('w', {}, ['A']);
    Reflect.set(stream, 'state', 'open');
    feed(stream, ['r0', 'r1'].map(tag => change('w', { tag })));
    await advance(1000);
    h.budget.bodies += 2 * LIMIT;
    await h.relist(id, 'w', [...subscribers, entry('J', new Date(START - 5000))]);
    await advance(100);
    await h.relist(id, 'w', subscribers);
    h.budget.bodies -= 2 * LIMIT;
    bodiesOf(h).wake();
    feed(stream, [change('w', { tag: 'l0' })]);
    await advance(1000);
    assert.deepEqual(h.tags('J'), []);
    assert.deepEqual(h.tags('A'), ['r0', 'r1', 'l0']);
    assert.deepEqual(line(h), []);
  });

  it('records the joiners whose replay window was trimmed at once, and closes ten of them at once and the rest one a second', async () => {
    const h = harness(undefined, { maxQueuedBytes: 4 * LIMIT });
    const { id, stream, subscribers } = await h.subscribe('w', {}, ['A']);
    Reflect.set(stream, 'state', 'open');
    feed(stream, Array.from({ length: 600 }, (_, n) => change('w', { tag: n })));
    await advance(10_000);
    assert.equal(h.tags('A').length, 600);
    const closes = Reflect.get(h.fanout, 'closes') as { record?(closings: object[]): void };
    const record = closes.record?.bind(closes);
    const recorded: object[][] = [];
    closes.record = closings => {
      recorded.push(closings);
      record?.(closings);
    };
    const joiners = Array.from({ length: 12 }, (_, n) => entry(`J${n}`, new Date(START - 5000)));
    await h.relist(id, 'w', [...subscribers, ...joiners]);
    await flush();
    assert.equal(h.disconnected.length, 10);
    assert.deepEqual(
      recorded.map(closings => closings.length),
      [12]
    );
    await advance(1000);
    assert.equal(h.disconnected.length, 11);
    await advance(1000);
    assert.equal(h.disconnected.length, 12);
    assert.ok(closing().every(w => w.includes('its replay window had already been trimmed')));
  });
});

describe('lookups', () => {
  const dedicated = (subscription: { collectionName: string }) => {
    if (subscription.collectionName === 'd') throw new Error('matched on the server');
    return buildSubscriptionFilter(subscription as never);
  };

  it('sends a change ids-only once its lookup has failed for 31s, gives later changes one 6s try each, and full documents once Mongo answers', async () => {
    let answering = false;
    const waiting: Array<() => void> = [];
    const ids = Array.from({ length: 4 }, () => new ObjectId());
    mock.method(mongoose.connection, 'collection', () => ({
      findOne: ({ _id }: { _id: ObjectId }) =>
        new Promise(resolve => {
          const answer = () => resolve({ _id, tag: `d${ids.findIndex(id => id.equals(_id))}` });
          if (answering) answer();
          else waiting.push(answer);
        }),
    }));
    const h = harness(undefined, { buildFilter: dedicated });
    const { stream } = await h.subscribe('d', {}, ['A']);
    feed(stream, ids.slice(0, 3).map(id => update('d', id)));
    await advance(30_500);
    assert.deepEqual(h.got.A ?? [], []);
    await advance(7000);
    await advance(2500);
    answering = true;
    waiting.splice(0).forEach(answer => answer());
    await advance(100);
    feed(stream, [update('d', ids[3])]);
    await advance(500);
    const got = h.got.A ?? [];
    assert.deepEqual(
      got.map(({ id, tag }) => [id, tag]),
      ids.map((id, n) => [String(id), n < 2 ? undefined : `d${n}`])
    );
    assert.ok(got[0].at >= 31_000 && got[0].at < 31_500, `${got[0].at}`);
    assert.ok(got[1].at >= 37_000 && got[1].at < 37_500, `${got[1].at}`);
    assert.deepEqual(h.disconnected, []);
  });

  it('sends the ids of a change whose document is over the budget, with a warning, and skips an event over it, pausing nothing', async () => {
    const big = new ObjectId();
    mock.method(mongoose.connection, 'collection', () => ({ findOne: async () => ({ _id: big, pad: 'x'.repeat(LIMIT) }) }));
    const h = harness(undefined, { buildFilter: dedicated });
    const d = await h.subscribe('d', {}, ['A']);
    const z = await h.subscribe('z', {}, ['Z']);
    feed(d.stream, [update('d', big)]);
    const skipped = change('z', { pad: 'x'.repeat(LIMIT) });
    feed(z.stream, [change('z', { tag: 'z1' }), skipped]);
    await advance(6000);
    assert.deepEqual(
      (h.got.A ?? []).map(({ id, tag }) => [id, tag]),
      [[String(big), undefined]]
    );
    assert.deepEqual(h.tags('Z'), ['z1']);
    assert.ok(warns.some(w => w.startsWith(`sending the ids of a change on d to ${big} without its document`)));
    assert.ok(warns.some(w => w.startsWith('skipped a change on z')));
    assert.ok(!d.stream.paused && !z.stream.paused);
    assert.equal(z.saved.at(-1), JSON.stringify(skipped._id));
  });

  it("keeps the head stream's place while its refused lookup is fetched again, though it reads newer changes meanwhile", async () => {
    let calls = 0;
    let answerLater: (() => void) | undefined;
    mock.method(mongoose.connection, 'collection', () => ({
      findOne: ({ _id }: { _id: ObjectId }) =>
        new Promise(resolve => {
          const answer = () => resolve({ _id, pad: 'x'.repeat(LIMIT / 4) });
          if (++calls === 2) answerLater = answer;
          else answer();
        }),
    }));
    const h = harness(undefined, { buildFilter: dedicated });
    const d = await h.subscribe('d', {}, ['A']);
    const headStream = () => (Reflect.get(h.fanout, 'headStream') as (now: number) => CollectionStream | undefined).call(h.fanout, Date.now());
    h.budget.add(LIMIT);
    feed(d.stream, [update('d', new ObjectId())]);
    await advance(100);
    assert.ok(d.stream.paused);
    await advance(10_500);
    h.budget.add(-LIMIT);
    await advance(300);
    assert.ok(!d.stream.paused && answerLater);
    assert.equal(headStream(), d.stream);
    feed(d.stream, [update('d', new ObjectId())]);
    await advance(100);
    assert.equal(headStream(), d.stream);
    answerLater();
    await advance(100);
    assert.equal(headStream(), undefined);
    assert.equal(h.got.A?.length, 2);
  });

  it('lets the next builder go once a delivery waiting for room finds its body built by another stream', async () => {
    mock.method(mongoose.connection, 'collection', () => ({ findOne: async ({ _id }: { _id: ObjectId }) => ({ _id, tag: 'd' }) }));
    const h = harness(undefined, { buildFilter: dedicated });
    const d1 = await h.subscribe('d', {}, ['A']);
    const d2 = await h.subscribe('d', {}, ['B']);
    const t = await h.subscribe('t', {}, ['F']);
    const read = update('d', new ObjectId());
    feed(d1.stream, [read]);
    feed(d2.stream, [{ ...read }]);
    await overBudget(h, async () => {
      await advance(100);
      feed(t.stream, [change('t', { tag: 't0' })]);
      await advance(100);
      const waiting = line(h);
      assert.deepEqual([waiting.slice(0, 2).sort(), waiting[2]], [[d1.stream.label, d2.stream.label].sort(), 't']);
      makeRoom(h);
      await advance(100);
      assert.deepEqual(line(h), []);
      assert.deepEqual(h.tags('F'), ['t0']);
    });
    assert.deepEqual([h.tags('A'), h.tags('B')], [['d'], ['d']]);
  });
});
