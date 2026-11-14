import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { Budget } from '../src/budget.js';
import { DROPPED, ENTRY_BYTES, JOB_BYTES, Job, Message, Scheduler, SchedulerLimits, classifyFailure } from '../src/scheduler.js';

type TestJob = Job & { name: string; frame: number };
type Post = {
  conn: string;
  job: string;
  index: number;
  at: number;
  signal: AbortSignal;
  probation: boolean;
  connected: () => void;
  resolve: () => void;
  reject: (error: unknown) => void;
};

const flush = () => new Promise(resolve => setImmediate(resolve));

async function advance(ms: number, step = 10) {
  for (let t = 0; t < ms; t += step) {
    mock.timers.tick(step);
    await flush();
  }
}

const failure = (extra: object) => Object.assign(new Error('failed'), extra);

function setup(limits: Partial<SchedulerLimits> = {}, budgetLimit = 1e9) {
  const budget = new Budget(budgetLimit);
  const posts: Post[] = [];
  const settled: Array<{ job: string; conn: string; result: PromiseSettledResult<unknown> }> = [];
  const closed: Array<{ conn: string; reason: string; detail?: string; at: number }> = [];
  const gone: string[] = [];
  const expired: unknown[] = [];
  const answer = new Map<string, (post: Post) => void>();
  let bodyWait = 0;
  const stall = (ms: number) => (bodyWait = ms);
  // Connections whose POSTs connect only when the test calls post.connected(); others reuse a socket.
  const unconnected = new Set<string>();
  const blocked = new Set<string>();
  const message = (job: TestJob, conn: string): Message => ({
    posts: job.posts,
    frameBytes: () => job.frame,
    post: (index, signal, probation, connected) =>
      new Promise<void>((resolve, reject) => {
        const post = { conn, job: job.name, index, at: Date.now(), signal, probation, connected, resolve, reject };
        posts.push(post);
        if (!unconnected.has(conn)) connected();
        answer.get(conn)?.(post);
      }),
  });
  const scheduler = new Scheduler<TestJob>(
    budget,
    { slots: 4, requestTimeoutMs: 5000, maxDrainMs: 60_000, ...limits },
    {
      open: (job, index) => message(job, job.targets[index].connectionId),
      settle: (job, index, result) => settled.push({ job: job.name, conn: job.targets[index].connectionId, result }),
      close: list => list.forEach(({ connectionId, reason, detail }) => closed.push({ conn: connectionId, reason, detail, at: Date.now() })),
      gone: (_endpoint, conn) => gone.push(conn),
      extra: () => 0,
      check: () => {},
      roomWaitMs: () => bodyWait,
      blocked: key => blocked.has(key),
      once: job => job.ping === true,
      expired: error => void expired.push(error),
    }
  );
  const send = (name: string, conns: string[], { posts: count = 1, frame = 100, bytes = 1000, endpoint = 'e', ping = false } = {}) =>
    scheduler.add({
      name,
      frame,
      bytes,
      posts: count,
      pending: 0,
      ping,
      targets: conns.map(connectionId => ({ endpoint, connectionId })),
    });
  const ok =
    (ms = 0) =>
    (post: Post) =>
      setTimeout(post.resolve, ms);
  const hang = () => () => {};
  const order = (conn?: string) => posts.filter(p => !conn || p.conn === conn).map(p => `${p.conn}:${p.job}.${p.index}`);
  const state = (conn: string, endpoint = 'e') =>
    (scheduler as unknown as { conns: Map<string, { state: string }> }).conns.get(`${endpoint} ${conn}`)?.state;
  return { budget, scheduler, posts, settled, closed, gone, expired, answer, unconnected, blocked, stall, send, ok, hang, order, state };
}

beforeEach(() => {
  mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
  // The chunk gap runs on performance.now(), which the mock timers leave alone.
  mock.method(performance, 'now', () => Date.now());
  mock.method(console, 'warn', () => {});
});
afterEach(() => mock.reset());

describe('Scheduler', () => {
  it('sends each connection its messages in order, one POST at a time, and takes turns per chunk', async () => {
    const h = setup({ slots: 1 });
    h.answer.set('a', h.ok(10));
    h.answer.set('b', h.ok(10));
    h.send('m1', ['a', 'b'], { posts: 3 });
    h.send('m2', ['a'], { posts: 1 });
    await advance(200);
    assert.deepEqual(h.order(), ['a:m1.0', 'b:m1.0', 'a:m1.1', 'b:m1.1', 'a:m1.2', 'b:m1.2', 'a:m2.0']);
    assert.deepEqual(
      h.settled.map(s => `${s.conn}:${s.job}`),
      ['a:m1', 'b:m1', 'a:m2']
    );
    assert.equal(h.budget.stored, 0);
    assert.equal(h.budget.frames, 0);
  });

  it('settles a target being closed or removed at once as dropped, without queueing for it', async () => {
    const h = setup();
    h.answer.set('a', h.ok(5));
    h.blocked.add('e b');
    h.send('m', ['a', 'b']);
    assert.deepEqual(
      h.settled.map(s => [s.conn, s.result.status === 'fulfilled' && s.result.value === DROPPED]),
      [['b', true]]
    );
    assert.ok(!h.scheduler.busy('e', 'b'));
    await advance(50);
    assert.deepEqual(h.order(), ['a:m.0']);
    assert.equal(h.budget.stored, 0);
  });

  it("counts a job's delivery bytes toward its stream until they settle", async () => {
    const h = setup();
    const stream = {};
    h.answer.set('a', h.ok(5));
    h.answer.set('b', h.hang());
    h.scheduler.add({ name: 'm', frame: 100, bytes: 1000, posts: 1, pending: 0, stream, targets: [{ endpoint: 'e', connectionId: 'a' }, { endpoint: 'e', connectionId: 'b' }] });
    assert.equal(h.budget.streamBytes(stream), 1000 + JOB_BYTES + 2 * ENTRY_BYTES);
    await advance(50);
    assert.equal(h.budget.streamBytes(stream), 1000 + JOB_BYTES + ENTRY_BYTES);
    h.scheduler.close('e', 'b');
    assert.equal(h.budget.streamBytes(stream), 0);
  });

  it('keeps slots for healthy connections while new ones hang', async () => {
    const h = setup({ slots: 2, probationSlots: 1 });
    h.answer.set('w1', h.ok(5));
    h.answer.set('w2', h.ok(5));
    h.send('w', ['w1', 'w2']);
    await advance(50);
    h.answer.set('w1', h.ok(100));
    h.answer.set('w2', h.ok(100));
    h.answer.set('n1', h.hang());
    h.answer.set('n2', h.hang());
    h.send('y', ['w1', 'w2']);
    h.send('z', ['w1', 'w2']);
    h.send('x', ['n1', 'n2']);
    await advance(250);
    assert.deepEqual(h.order(), ['w1:w.0', 'w2:w.0', 'w1:y.0', 'w2:y.0', 'n1:x.0', 'w1:z.0', 'w2:z.0']);
  });

  it('releases a whole message cut at its threshold, freeing its slot, and closes the connection at the request timeout', async () => {
    const h = setup({ slots: 1 });
    h.answer.set('hung', h.hang());
    h.answer.set('other', h.ok(5));
    h.send('m', ['hung']);
    h.send('m', ['other']);
    await advance(240);
    assert.equal(h.state('hung'), 'flight');
    assert.deepEqual(h.order(), ['hung:m.0']);
    await advance(30);
    assert.equal(h.state('hung'), 'released');
    assert.deepEqual(h.order(), ['hung:m.0', 'other:m.0']);
    await advance(5000);
    assert.equal(h.posts.filter(p => p.conn === 'hung').length, 1);
    assert.deepEqual(h.closed.map(c => [c.conn, c.reason]), [['hung', 'a send went unanswered past the request timeout']]);
    assert.ok(h.posts[0].signal.aborted);
    assert.deepEqual(h.settled.at(-1), { job: 'm', conn: 'hung', result: { status: 'fulfilled', value: DROPPED } });
  });

  it('delivers a released message once when its answer comes late, and takes the answer as a latency sample', async () => {
    const h = setup();
    h.answer.set('c', h.ok(2000));
    h.send('m1', ['c']);
    h.send('m2', ['c']);
    await advance(1000);
    assert.equal(h.state('c'), 'released');
    assert.deepEqual(h.order(), ['c:m1.0'], 'the next message waits for the released one');
    await advance(1100);
    assert.deepEqual(h.order(), ['c:m1.0', 'c:m2.0']);
    assert.equal(h.state('c'), 'flight', 'cut no earlier than twice its 2s answer');
    await advance(2100);
    assert.deepEqual(
      h.settled.map(s => `${s.job}:${s.result.status}`),
      ['m1:fulfilled', 'm2:fulfilled']
    );
    assert.deepEqual(h.closed, []);
  });

  it('aborts a whole message past the release byte cap and sends it again after 1s', async () => {
    const h = setup({}, 1e10);
    h.answer.set('big', h.hang());
    h.send('m', ['big'], { frame: 33 * 1024 * 1024 });
    await advance(3000);
    const attempts = h.posts.filter(p => p.conn === 'big').map(p => p.at);
    assert.ok(attempts.length >= 2, `${attempts.length} attempts`);
    assert.ok(attempts[1] - attempts[0] >= 1250 && attempts[1] - attempts[0] < 1300, `gap ${attempts[1] - attempts[0]}`);
    assert.ok(h.posts[0].signal.aborted);
    assert.deepEqual(h.closed, []);
  });

  it('retries a cut chunk at once with the same index', async () => {
    const h = setup();
    let first = true;
    h.answer.set('c', post => {
      if (post.index === 1 && first) first = false;
      else setTimeout(post.resolve, 5);
    });
    h.send('m', ['c'], { posts: 3 });
    await advance(3000);
    assert.deepEqual(h.order(), ['c:m.0', 'c:m.1', 'c:m.1', 'c:m.2']);
    assert.ok(h.posts[2].at - h.posts[1].at < 1100);
    assert.equal(h.settled.length, 1);
    assert.equal(h.closed.length, 0);
  });

  it('holds a message through gateway faults, 100ms, 400ms, 1s, 2s, 4s then 5s apart, and never drops it', async () => {
    const h = setup();
    const faults = [
      failure({ $metadata: { httpStatusCode: 503 } }),
      failure({ $metadata: { httpStatusCode: 429 } }),
      failure({ name: 'TimeoutError', code: 'ECONNRESET' }),
      failure({ code: 'EHOSTUNREACH' }),
      failure({ name: 'CredentialsProviderError' }),
      failure({ name: 'TimeoutError', code: 'EPIPE' }),
      failure({ $metadata: { httpStatusCode: 500 } }),
    ];
    let n = 0;
    h.answer.set('c', post => (post.job === 'm1' && n < faults.length ? post.reject(faults[n++]) : post.resolve()));
    h.send('m1', ['c']);
    h.send('m2', ['c']);
    await advance(20_000);
    const attempts = h.posts.filter(p => p.job === 'm1').map(p => p.at);
    const gaps = attempts.slice(1).map((at, i) => at - attempts[i]);
    assert.equal(gaps.length, faults.length);
    [100, 400, 1000, 2000, 4000, 5000, 5000].forEach((hold, i) => assert.ok(gaps[i] >= hold && gaps[i] < hold + 30, `gap ${i} was ${gaps[i]}`));
    assert.deepEqual(
      h.settled.map(s => `${s.job}:${s.result.status}`),
      ['m1:fulfilled', 'm2:fulfilled']
    );
    assert.deepEqual(h.closed, []);
  });

  it('closes a connection whose next chunk gateway faults would hold past 20s after the chunk before it', async () => {
    const h = setup();
    h.answer.set('c', post =>
      setTimeout(() => (post.index === 1 && Date.now() < 35_000 ? post.reject(failure({ $metadata: { httpStatusCode: 503 } })) : post.resolve()), 20)
    );
    h.send('m', ['c'], { posts: 3 });
    await advance(45_000, 20);
    assert.equal(h.closed.length, 1);
    assert.equal(h.closed[0].reason, 'the next chunk of a partly sent message would go out over 20s after the one before it');
    assert.ok(h.closed[0].at > 15_000 && h.closed[0].at < 20_000, `closed at ${h.closed[0].at}`);
    assert.ok(!h.order().includes('c:m.2'));
    assert.deepEqual(
      h.settled.map(s => s.result.status === 'fulfilled' && s.result.value === DROPPED),
      [true]
    );
  });

  it('closes a connection whose next chunk waits for a retry token past 20s after the chunk before it', async () => {
    const h = setup({ slots: 1 });
    const unavailable = (post: Post) => setTimeout(() => post.reject(failure({ $metadata: { httpStatusCode: 503 } })), 5);
    const others = Array.from({ length: 40 }, (_, n) => `x${n}`);
    for (const conn of others) h.answer.set(conn, unavailable);
    h.answer.set('c', post => (post.index === 0 ? setTimeout(post.resolve, 5) : unavailable(post)));
    h.send('m', ['c'], { posts: 2 });
    h.send('n', others);
    await advance(50_000, 50);
    const closed = h.closed.find(c => c.conn === 'c');
    assert.equal(closed?.reason, 'the next chunk of a partly sent message would go out over 20s after the one before it');
    assert.ok(closed.at > 20_000 && closed.at < 50_000, `closed at ${closed.at}`);
    assert.deepEqual(h.order('c'), ['c:m.0', 'c:m.1']);
  });

  it('sends a message again after the gateway resets its socket, which the SDK reports as a timeout, and keeps the connection', async () => {
    const h = setup();
    let reset = true;
    h.answer.set('c', post => {
      if (!reset) return post.resolve();
      reset = false;
      post.reject(failure({ name: 'TimeoutError', code: 'ECONNRESET', message: 'socket hang up' }));
    });
    h.send('m', ['c']);
    await advance(1000);
    assert.deepEqual(h.order(), ['c:m.0', 'c:m.0']);
    assert.deepEqual(
      h.settled.map(s => s.result.status),
      ['fulfilled']
    );
    assert.deepEqual(h.closed, []);
  });

  it('takes a retry token after each hold: 2S to start with, then S a second', async () => {
    const h = setup({ slots: 4 });
    const conns = Array.from({ length: 40 }, (_, n) => `c${n}`);
    for (const conn of conns) h.answer.set(conn, post => post.reject(failure({ $metadata: { httpStatusCode: 503 } })));
    h.send('m', conns);
    await advance(10);
    assert.equal(h.posts.length, 40);
    await advance(140);
    assert.equal(h.posts.length - 40, 8);
    await advance(1000);
    const retries = h.posts.length - 40;
    assert.ok(retries >= 11 && retries <= 13, `${retries} retries`);
    assert.deepEqual(h.closed, []);
  });

  it('still grants retry tokens after stop, so connections waiting for one at shutdown send during the drain', async () => {
    const h = setup({ slots: 1 });
    let failing = true;
    const conns = ['a', 'b', 'c', 'd'];
    for (const conn of conns)
      h.answer.set(conn, post => setTimeout(() => (failing ? post.reject(failure({ $metadata: { httpStatusCode: 503 } })) : post.resolve()), 20));
    for (const conn of conns) h.send(conn, [conn]);
    const all = () => [...(h.scheduler as unknown as { conns: Map<string, { state: string; needsToken: boolean; holdUntil: number }> }).conns.values()];
    for (let t = 0; t < 3000 && failing; t += 10) {
      await advance(10);
      if (all().every(c => c.state === 'held' && c.needsToken && c.holdUntil <= Date.now())) {
        h.scheduler.stop();
        failing = false;
      }
    }
    assert.ok(!failing);
    await advance(5600);
    h.scheduler.end();
    assert.deepEqual(h.settled.map(s => `${s.conn}:${s.result.status}`).sort(), conns.map(c => `${c}:fulfilled`));
  });

  it('cuts a connect that takes over 3s and holds it as a gateway fault', async () => {
    const h = setup();
    h.unconnected.add('c');
    let tries = 0;
    h.answer.set('c', post => {
      if (++tries > 1) post.connected();
      if (tries > 1) setTimeout(post.resolve, 5);
    });
    h.send('m', ['c']);
    await advance(2990);
    assert.ok(!h.posts[0].signal.aborted);
    await advance(30);
    assert.ok(h.posts[0].signal.aborted);
    await advance(200);
    assert.equal(h.posts.length, 2);
    assert.ok(h.posts[1].at - h.posts[0].at >= 3100 && h.posts[1].at - h.posts[0].at < 3150);
    assert.deepEqual(h.closed, []);
    assert.deepEqual(h.settled[0].result, { status: 'fulfilled', value: undefined });
  });

  it("closes a connection once its oldest message has used 60s of its own time, counting holds but not its turn's wait", async () => {
    const h = setup();
    h.answer.set('c', post => post.reject(failure({ $metadata: { httpStatusCode: 503 } })));
    h.send('m', ['c']);
    await advance(59_000, 50);
    assert.equal(h.closed.length, 0);
    await advance(1500, 50);
    assert.deepEqual(h.closed.map(c => [c.conn, c.reason]), [['c', 'its oldest message has been sending for 60s']]);
  });

  it('tries a ping once and settles it as failed on a fault, without a hold or a close', async () => {
    const h = setup();
    h.answer.set('c', post => post.reject(failure({ $metadata: { httpStatusCode: 503 } })));
    h.answer.set('d', h.hang());
    h.send('p', ['c', 'd'], { ping: true });
    await advance(6000);
    assert.equal(h.posts.length, 2);
    assert.deepEqual(
      h.settled.map(s => `${s.conn}:${s.result.status}`),
      ['c:rejected', 'd:rejected']
    );
    assert.deepEqual(h.closed, []);
  });

  it('reports expired credentials and settles the send without trying again', async () => {
    const h = setup();
    const error = failure({ name: 'ExpiredTokenException' });
    h.answer.set('c', post => post.reject(error));
    h.send('m', ['c']);
    await advance(1000);
    assert.deepEqual(h.expired, [error]);
    assert.equal(h.posts.length, 1);
    assert.equal(h.settled[0].result.status, 'rejected');
  });

  it('closes a connection whose send the gateway rejects', async () => {
    const h = setup();
    h.answer.set('c', post => post.reject(failure({ $metadata: { httpStatusCode: 400 } })));
    h.send('m', ['c']);
    await advance(100);
    assert.deepEqual(h.closed.map(c => [c.conn, c.reason]), [['c', 'the gateway rejected a send']]);
    assert.match(h.closed[0].detail!, /failed/);
  });

  it('reports a gone connection without closing it, and drops its queue', async () => {
    const h = setup();
    h.answer.set('g', post => post.reject(failure({ $metadata: { httpStatusCode: 410 } })));
    h.send('m1', ['g']);
    h.send('m2', ['g']);
    await advance(100);
    assert.deepEqual(h.gone, ['g']);
    assert.equal(h.closed.length, 0);
    assert.deepEqual(
      h.settled.map(s => s.result),
      [
        { status: 'fulfilled', value: DROPPED },
        { status: 'fulfilled', value: DROPPED },
      ]
    );
    assert.equal(h.posts.length, 1);
  });

  it('closes a connection whose POST runs to the request timeout', async () => {
    const h = setup({ requestTimeoutMs: 200 });
    h.answer.set('t', h.hang());
    h.send('m', ['t']);
    await advance(400);
    assert.deepEqual(h.closed.map(c => c.conn), ['t']);
    assert.ok(h.posts[0].signal.aborted);
  });

  it('closes a connection once what it alone holds has taken over the drain limit to send for 10s', async () => {
    const h = setup({ maxDrainMs: 10_000 });
    h.answer.set('slow', h.ok(1000));
    h.answer.set('fast', h.ok(1));
    h.send('warm', ['slow', 'fast']);
    await advance(10_000);
    for (let n = 0; n < 40; n++) h.send(`m${n}`, ['slow', 'fast']);
    await advance(10_000);
    assert.equal(h.closed.length, 0);
    await advance(1500);
    assert.deepEqual(h.closed.map(c => c.conn), ['slow']);
    assert.equal(h.closed[0].reason, 'what it alone holds would take over 10s to send');
    assert.match(h.closed[0].detail!, /^\d+s$/);
    assert.equal(h.settled.filter(s => s.conn === 'fast' && s.result.status === 'fulfilled' && s.result.value === undefined).length, 41);
  });

  it('lets a response that arrived while the event loop was busy land before the cut', async () => {
    const h = setup();
    h.answer.set('c', h.ok(10));
    h.send('warm', ['c']);
    await advance(1100);
    // Due after the 250ms cut, but both come due in one turn of the loop, as after a long synchronous stretch.
    h.answer.set('c', h.ok(300));
    h.send('m', ['c']);
    mock.timers.tick(400);
    await flush();
    await flush();
    assert.deepEqual(h.order(), ['c:warm.0', 'c:m.0']);
    assert.ok(!h.posts[1].signal.aborted);
    assert.equal(h.settled.length, 2);
  });

  it('starts the abort clock once the socket has connected', async () => {
    const h = setup();
    h.unconnected.add('c');
    h.answer.set('c', post => {
      setTimeout(post.connected, 1500);
      setTimeout(post.resolve, 1600);
    });
    h.send('m', ['c']);
    await advance(2000);
    assert.equal(h.posts.length, 1);
    assert.deepEqual(h.closed, []);
    assert.equal(h.settled[0].result.status, 'fulfilled');
  });

  it('cuts a new connection at 250ms with no history, then at twice its own p99 in the frame size class', async () => {
    const h = setup();
    h.answer.set('c', h.ok(600));
    h.send('first', ['c']);
    await advance(240);
    assert.equal(h.state('c'), 'flight');
    await advance(20);
    assert.equal(h.state('c'), 'released');
    await advance(10_000, 50);
    assert.equal(h.settled.length, 1);
    // Keeps Ref, and so the 2 x Ref floor, low.
    h.answer.set('f', h.ok(5));
    h.send('fast', ['f']);
    await advance(1000, 50);
    h.answer.set('c', h.ok(1100));
    h.send('small', ['c']);
    await advance(1050);
    assert.equal(h.state('c'), 'flight', 'under twice its own 600ms');
    await advance(500);
    h.send('large', ['c'], { frame: 64 * 1024 });
    await advance(240);
    assert.equal(h.state('c'), 'flight');
    await advance(20);
    assert.equal(h.state('c'), 'released', 'no history in the large class');
  });

  it('starts a new connection before one that is trying again', async () => {
    const h = setup({ slots: 1 });
    h.answer.set('r', h.hang());
    h.answer.set('o', h.hang());
    h.answer.set('u', h.ok(5));
    h.send('m', ['r'], { posts: 2 });
    h.send('m', ['o']);
    await advance(300);
    h.send('m', ['u']);
    await advance(260);
    assert.deepEqual(h.order().slice(0, 4), ['r:m.0', 'o:m.0', 'u:m.0', 'r:m.0']);
  });

  it('caps the threshold of healthy attempts while the healthy rotation is over 3.5s, and lowers the ones in flight', async () => {
    const h = setup({ slots: 8 });
    const hung = Array.from({ length: 80 }, (_, n) => `h${n}`);
    // Keep Ref, and so the 2 x Ref floor, low.
    const fast = Array.from({ length: 10 }, (_, n) => `f${n}`);
    for (const conn of hung) h.answer.set(conn, h.ok(400));
    for (const conn of fast) h.answer.set(conn, h.ok(5));
    h.send('warm', [...hung, ...fast]);
    await advance(50_000, 50);
    assert.equal(h.settled.length, 90);
    const cuts = new Map<string, number[]>();
    for (const conn of [...hung, ...fast]) {
      h.answer.set(conn, post => {
        post.signal.addEventListener('abort', () => cuts.set(conn, [...(cuts.get(conn) ?? []), Date.now() - post.at]));
        if (conn.startsWith('f')) setTimeout(post.resolve, 5);
      });
    }
    for (let n = 0; n < 20; n++) h.send(`fast${n}`, fast);
    // Chunks are aborted when cut, where a whole message would be released.
    h.send('m', hung, { posts: 2 });
    await advance(4000, 10);
    assert.deepEqual(
      fast.filter(conn => cuts.has(conn)),
      []
    );
    const hungCuts = hung.flatMap(conn => cuts.get(conn) ?? []);
    assert.ok(hungCuts.includes(800), 'cut at twice its own p99 before the cap');
    assert.ok(hungCuts.some(ms => ms > 400 && ms < 800), `lowered in flight: ${hungCuts.join(' ')}`);
    assert.ok(hungCuts.some(ms => ms <= 350), `capped: ${hungCuts.join(' ')}`);
  });

  it('closes a suspect whose attempt is unanswered 2s after its socket connected while others on its endpoint answer', async () => {
    const h = setup();
    h.answer.set('x', h.ok(5));
    h.answer.set('f', h.ok(5));
    h.send('warm', ['x', 'f']);
    await advance(1000);
    h.answer.set('x', h.hang());
    for (let n = 0; n < 2000; n++) h.send(`f${n}`, ['f']);
    // Its chunk is aborted at each cut until the time in flight makes it slow.
    h.send('m', ['x'], { posts: 2 });
    await advance(10_000);
    assert.deepEqual(h.closed.map(c => c.conn), ['x']);
    assert.equal(h.closed[0].reason, 'a send went unanswered while others to its endpoint answered');
    assert.match(h.closed[0].detail!, /^\d+ms against 2000ms$/);
    const after = h.closed[0].at - h.posts.filter(p => p.conn === 'x').at(-1)!.at;
    assert.ok(after > 2000 && after <= 2250, `closed ${after}ms after its suspect attempt`);
  });

  it('waits twice Ref_3s for a suspect when others on its endpoint answer in over 1s', async () => {
    const h = setup();
    h.answer.set('x', h.ok(5));
    h.answer.set('f', h.ok(5));
    h.send('warm', ['x', 'f']);
    await advance(1000);
    h.answer.set('f', h.ok(1500));
    h.answer.set('x', post => {
      if (h.posts.filter(p => p.conn === 'x').length > 6) setTimeout(post.resolve, 2500);
    });
    for (let n = 0; n < 30; n++) h.send(`f${n}`, ['f']);
    h.send('m', ['x'], { posts: 2 });
    await advance(15_000);
    assert.deepEqual(h.closed, []);
    assert.ok(h.settled.some(s => s.conn === 'x' && s.job === 'm' && s.result.status === 'fulfilled'));
  });

  it('leaves a suspect to the request timeout when nothing on its endpoint completed in the last 3s', async () => {
    const h = setup();
    h.answer.set('x', h.ok(5));
    h.answer.set('f', h.ok(5));
    h.send('warm', ['x'], { endpoint: 'e2' });
    h.send('warm', ['f']);
    await advance(4000);
    h.answer.set('x', h.hang());
    for (let n = 0; n < 2000; n++) h.send(`f${n}`, ['f']);
    h.send('m', ['x'], { endpoint: 'e2', posts: 2 });
    await advance(12_000);
    assert.deepEqual(h.closed.map(c => c.conn), ['x']);
    assert.equal(h.closed[0].reason, 'a send went unanswered past the request timeout');
  });

  it('marks the POSTs of new and slow connections as probation', async () => {
    const h = setup();
    h.answer.set('c', h.ok(10));
    h.send('first', ['c']);
    h.send('second', ['c']);
    await advance(100);
    assert.deepEqual(
      h.posts.map(p => p.probation),
      [true, false]
    );
  });

  it('does not close a fast connection for one slow POST in front of a deep queue', async () => {
    const h = setup();
    let n = 0;
    h.answer.set('c', post => setTimeout(post.resolve, n++ === 0 ? 900 : 1));
    for (let i = 0; i < 100; i++) h.send(`m${i}`, ['c']);
    await advance(3000);
    assert.deepEqual(h.closed, []);
    assert.equal(h.settled.filter(s => s.result.status === 'fulfilled' && s.result.value === undefined).length, 100);
  });

  it('does not count messages another connection still holds towards the drain', async () => {
    const h = setup({ maxDrainMs: 10_000 });
    h.answer.set('a', h.ok(1000));
    h.answer.set('b', h.ok(1000));
    for (let n = 0; n < 15; n++) h.send(`m${n}`, ['a', 'b']);
    await advance(1500);
    // Each holds all 15, but neither alone.
    assert.deepEqual(h.closed, []);
  });

  it('makes a probation frame wait for room, but lets one start when none of its class is in flight', async () => {
    // F = 4000, probation F/4 = 1000.
    const h = setup({ slots: 8 }, 16_000);
    h.answer.set('n1', h.ok(100));
    h.answer.set('n2', h.ok(100));
    h.send('big', ['n1', 'n2'], { frame: 1500 });
    await advance(50);
    assert.deepEqual(h.order(), ['n1:big.0']);
    assert.equal(h.budget.frames, 1500);
    await advance(100);
    assert.deepEqual(h.order(), ['n1:big.0', 'n2:big.0']);
  });

  it('starts smaller frames ahead of a waiting one only for its first second', async () => {
    // F = 1000 for the healthy class.
    const h = setup({ slots: 8 }, 4000);
    h.answer.set('a', h.ok(800));
    for (const conn of ['b', 'c', 'd']) h.answer.set(conn, h.ok(5));
    h.send('warm', ['a', 'b', 'c', 'd']);
    await advance(10_000, 50);
    // Under twice its own p99, so the attempt is not cut.
    h.answer.set('a', h.ok(1500));
    h.send('m', ['a'], { frame: 600 });
    await advance(10);
    h.send('m', ['b'], { frame: 600 });
    await advance(10);
    h.send('m', ['c'], { frame: 100 });
    await advance(1100);
    h.send('m', ['d'], { frame: 100 });
    await advance(100);
    const started = () => h.posts.filter(p => p.job === 'm').map(p => p.conn);
    assert.deepEqual(started(), ['a', 'c']);
    await advance(2000);
    assert.deepEqual(started(), ['a', 'c', 'b', 'd']);
  });

  it('moves a connection to the front of its ring once its current message has waited 10s for the next chunk', async () => {
    const h = setup({ slots: 1 });
    const others = Array.from({ length: 16 }, (_, n) => `o${n}`);
    h.answer.set('x', h.ok(5));
    for (const conn of others) h.answer.set(conn, h.ok(900));
    h.send('warm', ['x', ...others]);
    await advance(60_000, 50);
    // Under twice their own p99, so these attempts are not cut.
    for (const conn of others) h.answer.set(conn, h.ok(1700));
    h.send('chunked', ['x'], { posts: 2 });
    for (const conn of others) h.send(`m-${conn}`, [conn]);
    await advance(30_000, 50);
    const chunk = (index: number) => h.posts.find(p => p.job === 'chunked' && p.index === index)!.at;
    // Behind sixteen 1.7s POSTs it would wait 27s.
    assert.ok(chunk(1) - chunk(0) <= 13_000, `waited ${chunk(1) - chunk(0)}ms`);
  });

  it('closes slow connections over the budget, the one that frees most first, until under it', async () => {
    const h = setup({ slots: 8 });
    for (const conn of ['s1', 's2', 'fast']) h.answer.set(conn, h.ok(conn === 'fast' ? 5 : 1500));
    h.send('warm', ['s1', 's2', 'fast']);
    await advance(10_000, 50);
    h.answer.set('s1', h.hang());
    h.answer.set('s2', h.hang());
    h.send('small', ['s1'], { bytes: 1000 });
    h.send('large', ['s2'], { bytes: 5000 });
    h.send('shared', ['fast', 's1'], { bytes: 9000 });
    h.stall(1000);
    h.scheduler.closeSlow(h.budget.stored - 4000);
    assert.deepEqual(h.closed.map(c => c.conn), ['s2']);
  });

  it('ranks slow connections for a close at most once per 250ms', async () => {
    const h = setup({ slots: 8 });
    for (const conn of ['s1', 's2', 's3', 'fast']) h.answer.set(conn, h.ok(conn === 'fast' ? 5 : 1500));
    h.send('warm', ['s1', 's2', 's3', 'fast']);
    await advance(10_000, 50);
    // Ref counts only POSTs completed in the last 10s.
    h.send('recent', ['fast']);
    await advance(50);
    for (const conn of ['s1', 's2', 's3']) h.answer.set(conn, h.hang());
    h.send('a', ['s1'], { bytes: 1000 });
    h.send('b', ['s2'], { bytes: 5000 });
    h.send('c', ['s3'], { bytes: 100 });
    const own = mock.method(h.scheduler, 'own');
    h.stall(1000);
    h.scheduler.closeSlow(h.budget.stored - 4000);
    h.scheduler.closeSlow(h.budget.stored - 500);
    assert.deepEqual(h.closed.map(c => c.conn), ['s2', 's1']);
    assert.equal(own.mock.callCount(), 3);
    mock.timers.tick(250);
    h.scheduler.closeSlow(h.budget.stored - 50);
    assert.deepEqual(h.closed.map(c => c.conn), ['s2', 's1', 's3']);
    assert.equal(own.mock.callCount(), 4);
  });

  it('leaves a suspect out of the budget close', async () => {
    const h = setup({ slots: 8 });
    h.answer.set('x', h.ok(5));
    h.send('warm', ['x'], { endpoint: 'e2' });
    await advance(1000);
    h.answer.set('x', h.hang());
    h.send('m', ['x'], { endpoint: 'e2', bytes: 5000, posts: 2 });
    await advance(3000);
    assert.ok(h.posts.filter(p => p.conn === 'x').at(-1)!.probation, 'its attempt runs in the slow ring');
    h.scheduler.closeSlow(0);
    assert.deepEqual(h.closed, []);
  });

  it('takes Ref from the latest POST of each connection, not from its smoothed latency', async () => {
    const h = setup({ slots: 8 });
    const fleet = Array.from({ length: 10 }, (_, n) => `f${n}`);
    for (const conn of [...fleet, 'x']) h.answer.set(conn, h.ok(1400));
    h.send('warm', [...fleet, 'x']);
    for (let n = 0; n < 5; n++) h.send(`slow${n}`, [...fleet, 'x']);
    await advance(30_000, 50);
    // Each fleet connection's L is still about 1s after one fast POST.
    for (const conn of fleet) h.answer.set(conn, h.ok(5));
    h.send('fast', fleet);
    await advance(1500, 50);
    h.answer.set('x', h.hang());
    h.send('m', ['x'], { bytes: 5000 });
    await advance(50);
    h.stall(1000);
    h.scheduler.closeSlow(0);
    assert.deepEqual(h.closed.map(c => c.conn), ['x']);
  });

  it('does not count a connection as slow by L once its latest POST was under the slow line', async () => {
    const h = setup({ slots: 8, maxDrainMs: 10_000 });
    h.answer.set('f', h.ok(5));
    h.answer.set('c', h.ok(1500));
    h.send('warm', ['f', 'c']);
    for (let n = 0; n < 5; n++) h.send(`slow${n}`, ['c']);
    await advance(20_000, 50);
    h.answer.set('c', h.ok(5));
    h.send('fast', ['c', 'f']);
    await advance(100);
    h.answer.set('c', h.hang());
    for (let n = 0; n < 200; n++) h.send(`q${n}`, ['c', 'f'], { bytes: 5000 });
    await advance(50);
    h.scheduler.closeSlow(0);
    assert.deepEqual(h.closed, [], 'its L is over 1s, its latest POST is not');
  });

  it('counts every POST a connection slow by L holds towards its drain, shared or not', async () => {
    const h = setup({ maxDrainMs: 10_000 });
    h.answer.set('f', h.ok(5));
    h.answer.set('a', h.ok(1500));
    h.answer.set('b', h.ok(1500));
    h.send('warm', ['f', 'a', 'b']);
    for (let n = 0; n < 3; n++) h.send(`w${n}`, ['a', 'b']);
    await advance(15_000, 50);
    assert.equal(h.closed.length, 0);
    for (let n = 0; n < 40; n++) h.send(`m${n}`, ['a', 'b']);
    for (let n = 0; n < 200; n++) h.send(`f${n}`, ['f']);
    await advance(12_000, 50);
    assert.deepEqual(h.closed.map(c => c.conn).sort(), ['a', 'b']);
    assert.match(h.closed[0].reason, /alone holds would take/);
  });

  it('closes for the budget only a connection slow by L for 10s whose close frees a 64th of the excess', async () => {
    const h = setup({ slots: 8 });
    h.answer.set('f', h.ok(5));
    for (const conn of ['big', 'small']) h.answer.set(conn, h.ok(1500));
    h.send('warm', ['f', 'big', 'small']);
    await advance(8000, 50);
    for (let n = 0; n < 20; n++) {
      h.send(`a${n}`, ['big'], { bytes: 10_000 });
      h.send(`b${n}`, ['small'], { bytes: 10 });
    }
    h.send('recent', ['f']);
    await advance(50);
    h.scheduler.closeSlow(h.budget.stored - 640_000);
    assert.equal(h.closed.length, 0, 'slow for under 10s');
    await advance(10_000, 50);
    h.send('recent', ['f']);
    await advance(50);
    mock.timers.tick(250);
    // minFree is 640_000 / 64, which small's 10 byte jobs do not reach together.
    h.scheduler.closeSlow(h.budget.stored - 640_000);
    assert.deepEqual(h.closed.map(c => c.conn), ['big']);
  });

  it('closes slow connections together when only together they free enough, at once while a body waits for room', async () => {
    const h = setup({ slots: 8 });
    const slow = Array.from({ length: 10 }, (_, n) => `s${n}`);
    // Two fast connections keep Ref, the 2nd fastest of 12, low.
    const fast = ['f0', 'f1'];
    for (const conn of fast) h.answer.set(conn, h.ok(5));
    for (const conn of slow) h.answer.set(conn, h.ok(1500));
    h.send('warm', [...fast, ...slow]);
    await advance(12_000, 50);
    h.send('recent', fast);
    await advance(1000, 50);
    for (let n = 0; n < 20; n++) h.send(`m${n}`, slow, { bytes: 100_000 });
    h.scheduler.closeSlow(0);
    assert.equal(h.closed.length, 0, 'no body is waiting, and they have been slow for under 10s');
    h.stall(1000);
    mock.timers.tick(250);
    h.scheduler.closeSlow(h.budget.stored - 1_000_000);
    assert.deepEqual(h.closed.map(c => c.conn).sort(), [...slow].sort());
  });

  it('keeps the last 8 slots from slow connections, so a new connection starts at once', async () => {
    const h = setup({ slots: 16, requestTimeoutMs: 60_000 });
    const fast = ['f0', 'f1', 'f2'];
    const slow = Array.from({ length: 20 }, (_, n) => `s${n}`);
    for (const conn of fast) h.answer.set(conn, h.ok(5));
    for (const conn of slow) h.answer.set(conn, h.ok(1500));
    h.send('warm', [...fast, ...slow]);
    await advance(12_000, 50);
    for (let n = 0; n < 1000; n++) h.send(`r${n}`, fast);
    await advance(2000, 50);
    for (const conn of slow) h.answer.set(conn, h.hang());
    h.send('hang', slow);
    await advance(100);
    assert.ok(h.posts.filter(p => p.job === 'hang').length <= 8);
    h.answer.set('new', h.ok(5));
    h.send('first', ['new']);
    assert.equal(h.posts.at(-1)!.conn, 'new');
  });

  it('gives slow connections up to 4 slots beside healthy ones that keep the rest busy', async () => {
    const h = setup({ slots: 64 });
    const busy = Array.from({ length: 60 }, (_, n) => `b${n}`);
    const hung = ['h0', 'h1', 'h2', 'h3'];
    for (const conn of [...busy, ...hung]) h.answer.set(conn, h.ok(300));
    h.send('warm', [...busy, ...hung]);
    await advance(20_000, 50);
    for (const conn of hung) h.answer.set(conn, h.hang());
    for (let n = 0; n < 100; n++) h.send(`m${n}`, busy);
    h.send('m', hung, { posts: 2 });
    const hangAt = Date.now();
    await advance(20_000, 50);
    assert.deepEqual(h.closed.map(c => c.conn).sort(), hung);
    for (const close of h.closed) {
      assert.match(close.reason, /unanswered/);
      assert.ok(close.at - hangAt <= 15_000, `closed ${close.at - hangAt}ms after the hang`);
    }
  });

  it('moves a waiting slow-ring entry to its own ring once its connection is no longer slow', async () => {
    const h = setup({ slots: 6, requestTimeoutMs: 60_000 });
    const slow = ['s0', 's1', 's2', 's3'];
    // m's 700ms POSTs put the slow line at 1.4s once x's fast POST leaves Ref's window; the slow connections are
    // over that line, and under the hung rule's 2s.
    h.answer.set('m', h.ok(700));
    for (const conn of slow) h.answer.set(conn, h.ok(1600));
    h.send('warm', ['m', ...slow]);
    for (let n = 0; n < 60; n++) h.send(`m${n}`, ['m']);
    await advance(15_000, 50);
    // x's fast POST holds Ref, and so the slow line, low for 10s.
    h.answer.set('x', h.ok(5));
    h.send('warm', ['x']);
    await advance(1100);
    for (const conn of slow) h.answer.set(conn, h.hang());
    h.answer.set('x', h.hang());
    // The slow connections hold the slow ring's 4 slots.
    h.send('hang', slow);
    h.send('x', ['x'], { posts: 2 });
    await advance(7000, 50);
    const x = (h.scheduler as unknown as { conns: Map<string, { slow: boolean; state: string }> }).conns.get('e x')!;
    assert.ok(x.slow && x.state === 'ready', 'a suspect waiting in the slow ring');
    const xPosts = () => h.posts.filter(p => p.conn === 'x' && p.job === 'x');
    const cut = xPosts().length;
    await advance(10_000, 50);
    assert.ok(xPosts().length > cut, 'started again once the line rose past its u');
    assert.equal(xPosts()[cut].probation, false);
  });

  it('classifies failures', () => {
    const cases: Array<[object, string]> = [
      [{ $metadata: { httpStatusCode: 410 } }, 'gone'],
      [{ $metadata: { httpStatusCode: 429 } }, 'gateway'],
      [{ $metadata: { httpStatusCode: 502 } }, 'gateway'],
      [{ code: 'ECONNREFUSED' }, 'gateway'],
      [{ name: 'TimeoutError', message: 'the request socket did not establish a connection with the server' }, 'gateway'],
      [{ name: 'TimeoutError', code: 'ECONNRESET', message: 'socket hang up' }, 'reset'],
      [{ name: 'TimeoutError', code: 'EPIPE' }, 'reset'],
      [{ name: 'TimeoutError', code: 'ETIMEDOUT' }, 'gateway'],
      [{ code: 'ENOTFOUND' }, 'gateway'],
      [{ code: 'EAI_AGAIN' }, 'gateway'],
      [{ name: 'CredentialsProviderError' }, 'gateway'],
      [{ name: 'TimeoutError', message: 'the request socket timed out' }, 'timeout'],
      [{ $metadata: { httpStatusCode: 403 } }, 'forbidden'],
      [{ $metadata: { httpStatusCode: 400 } }, 'rejected'],
      [{ $metadata: { httpStatusCode: 413 } }, 'rejected'],
      [{ name: 'ExpiredTokenException', $metadata: { httpStatusCode: 403 } }, 'expired'],
    ];
    for (const [error, expected] of cases) assert.equal(classifyFailure(failure(error)), expected, JSON.stringify(error));
  });
});
