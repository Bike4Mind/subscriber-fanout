import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { CLOSE_TTL_MS, CloseStore, PendingCloses, TombstoneStore, Tombstones } from '../src/closes.js';

const flush = () => new Promise(resolve => setImmediate(resolve));

async function advance(ms: number, step = 100) {
  for (let t = 0; t < ms; t += step) {
    mock.timers.tick(step);
    await flush();
  }
}

const status = (httpStatusCode: number) => Object.assign(new Error(`status ${httpStatusCode}`), { $metadata: { httpStatusCode } });
const on = (...connectionIds: string[]) => connectionIds.map(connectionId => ({ endpoint: 'e', connectionId }));

type TombstoneRow = Awaited<ReturnType<TombstoneStore['load']>>[number];

function tombstoneStore(stored: TombstoneRow[] = []) {
  const rows = new Map(stored.map(row => [`${row.endpoint} ${row.connectionId}`, row]));
  let failing = false;
  const store: TombstoneStore = {
    load: async () => [...rows.values()].filter(row => row.expiresAt > Date.now()),
    save: async row => {
      if (failing) throw new Error('write failed');
      rows.set(`${row.endpoint} ${row.connectionId}`, row);
    },
  };
  return { store, rows, fail: () => (failing = true) };
}

function setup(answer: (connectionId: string, attempt: number) => Promise<unknown>, stored: Array<{ endpoint: string; connectionId: string; firstAttemptAt: number }> = []) {
  const rows = new Map(stored.map(row => [`${row.endpoint} ${row.connectionId}`, row]));
  const events: string[] = [];
  const store: CloseStore = {
    load: async () => [...rows.values()],
    save: async closings => {
      events.push(`save ${closings.length}`);
      for (const row of closings) if (!rows.has(`${row.endpoint} ${row.connectionId}`)) rows.set(`${row.endpoint} ${row.connectionId}`, row);
    },
    remove: async row => {
      events.push(`remove ${row.connectionId}`);
      rows.delete(`${row.endpoint} ${row.connectionId}`);
    },
  };
  const graves = tombstoneStore();
  const save = graves.store.save;
  graves.store.save = async row => {
    await save(row);
    events.push(`tombstone ${row.connectionId}`);
  };
  const tombstones = new Tombstones(graves.store);
  const attempts = new Map<string, number[]>();
  const closed: string[] = [];
  const forbidden: string[] = [];
  const expired: unknown[] = [];
  const closes = new PendingCloses(
    {
      delete: connectionId => {
        const times = attempts.get(connectionId) ?? [];
        times.push(Date.now());
        attempts.set(connectionId, times);
        return answer(connectionId, times.length);
      },
      closed: (_endpoint, connectionId) => void closed.push(connectionId),
      forbidden: (_endpoint, connectionId) => void forbidden.push(connectionId),
      expired: error => void expired.push(error),
    },
    tombstones,
    store
  );
  return { closes, rows, attempts, closed, forbidden, expired, tombstones, graves, events };
}

beforeEach(() => {
  mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  mock.method(console, 'warn', () => {});
  mock.method(console, 'error', () => {});
  mock.method(console, 'info', () => {});
});
afterEach(() => mock.reset());

describe('PendingCloses', () => {
  it('keeps a connection blocked and persisted until the gateway confirms its close', async () => {
    const h = setup(async (_id, attempt) => {
      if (attempt < 3) throw status(503);
    });
    h.closes.add(on('c'));
    await flush();
    assert.ok(h.closes.blocked('e c'));
    assert.ok(h.rows.has('e c'));
    await advance(5000);
    assert.equal(h.attempts.get('c')?.length, 3);
    assert.deepEqual(h.closed, ['c']);
    assert.ok(!h.closes.blocked('e c'));
    assert.ok(!h.rows.has('e c'));
  });

  it('tombstones a confirmed close until 2h after its first attempt, before clearing its record', async () => {
    const h = setup(async (_id, attempt) => {
      if (attempt < 2) throw status(503);
    });
    mock.timers.tick(1000);
    h.closes.add(on('c'));
    await advance(3000);
    assert.deepEqual(h.closed, ['c']);
    assert.deepEqual(h.events, ['save 1', 'tombstone c', 'remove c']);
    assert.deepEqual(h.graves.rows.get('e c'), { endpoint: 'e', connectionId: 'c', reason: 'closed', expiresAt: 1000 + CLOSE_TTL_MS });
    assert.ok(h.tombstones.has('e c'));
  });

  it('keeps the record of a confirmed close whose tombstone could not be stored', async () => {
    const h = setup(async () => undefined);
    h.graves.fail();
    h.closes.add(on('c'));
    await advance(500);
    assert.deepEqual(h.closed, ['c']);
    assert.ok(h.rows.has('e c'));
  });

  it('backs off from 1s, doubling to 60s, on throttling, server errors and timeouts', async () => {
    const h = setup(async () => {
      throw Object.assign(new Error('TimeoutError'), { name: 'TimeoutError' });
    });
    h.closes.add(on('c'));
    await advance(240_000, 250);
    const times = h.attempts.get('c')!;
    const gaps = times.slice(1).map((t, i) => t - times[i]);
    const expected = [1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000];
    assert.ok(gaps.length >= expected.length, `${gaps.length} gaps`);
    gaps.slice(0, expected.length).forEach((gap, i) => {
      assert.ok(gap >= expected[i] * 0.75 - 250 && gap <= expected[i] * 1.25 + 250, `gap ${i} was ${gap}`);
    });
  });

  it('treats 410 as confirmed, and stops retrying another 4xx but 403 and 429 while keeping the connection blocked', async () => {
    const h = setup(async connectionId => {
      throw status(connectionId === 'gone' ? 410 : connectionId === 'forbidden' ? 403 : 400);
    });
    h.closes.add(on('gone', 'refused', 'forbidden'));
    await advance(10_000);
    assert.deepEqual(h.closed, ['gone']);
    assert.equal(h.attempts.get('refused')?.length, 1);
    assert.ok(h.closes.blocked('e refused'));
    assert.ok(h.attempts.get('forbidden')!.length > 1);
    assert.equal(h.forbidden.length, h.attempts.get('forbidden')!.length);
  });

  it('reports expired credentials and keeps retrying', async () => {
    const h = setup(async (_id, attempt) => {
      if (attempt < 2) throw Object.assign(new Error('The security token included in the request is expired'), { name: 'ExpiredTokenException' });
    });
    h.closes.add(on('c'));
    await advance(3000);
    assert.equal(h.expired.length, 1);
    assert.deepEqual(h.closed, ['c']);
  });

  it('stores a batch of closes in one write, and records closes still to be made without blocking them', async () => {
    const h = setup(async () => new Promise(() => {}));
    h.closes.add(on('a', 'b', 'c'));
    h.closes.record(on('later'));
    await flush();
    assert.deepEqual(h.events, ['save 3', 'save 1']);
    assert.ok(h.rows.has('e later'));
    assert.ok(!h.closes.blocked('e later'));
  });

  it('gives up after 2h and unsubscribes without confirmation', async () => {
    const h = setup(async () => {
      throw status(403);
    });
    h.closes.add(on('c'));
    await advance(CLOSE_TTL_MS - 60_000, 60_000);
    assert.ok(h.closes.blocked('e c'));
    await advance(61_000, 1000);
    assert.deepEqual(h.closed, ['c']);
    assert.ok(!h.rows.has('e c'));
    assert.equal(h.graves.rows.size, 0);
  });

  it('resumes the closes persisted before a restart', async () => {
    mock.timers.tick(10_000);
    const h = setup(async () => undefined, [{ endpoint: 'e', connectionId: 'old', firstAttemptAt: 5000 }]);
    await h.closes.load();
    assert.ok(h.closes.blocked('e old') || h.closed.includes('old'));
    await advance(100);
    assert.deepEqual(h.closed, ['old']);
  });

  it('has at most 16 deletes in flight', async () => {
    const pending: Array<() => void> = [];
    const h = setup(() => new Promise<void>(resolve => pending.push(resolve)));
    for (let n = 0; n < 40; n++) h.closes.add(on(`c${n}`));
    await flush();
    assert.equal(pending.length, 16);
    pending.splice(0).forEach(resolve => resolve());
    await advance(100);
    assert.equal(pending.length, 16);
    assert.equal(h.closed.length, 16);
  });
});

describe('Tombstones', () => {
  it('expires at the time given, 2h after removal by default', async () => {
    const g = tombstoneStore();
    const tombstones = new Tombstones(g.store);
    await tombstones.add('e', 'gone', 'gone');
    await tombstones.add('e', 'closed', 'closed', 60_000);
    assert.ok(tombstones.has('e gone') && tombstones.has('e closed'));
    assert.equal(g.rows.get('e gone')?.expiresAt, CLOSE_TTL_MS);
    mock.timers.tick(60_000);
    assert.ok(tombstones.has('e gone'));
    assert.ok(!tombstones.has('e closed'));
    mock.timers.tick(CLOSE_TTL_MS - 60_000);
    assert.ok(!tombstones.has('e gone'));
  });

  it('loads the unexpired tombstones stored before a restart', async () => {
    mock.timers.tick(10_000);
    const g = tombstoneStore([
      { endpoint: 'e', connectionId: 'live', reason: 'gone', expiresAt: 20_000 },
      { endpoint: 'e', connectionId: 'old', reason: 'gone', expiresAt: 5000 },
    ]);
    const tombstones = new Tombstones(g.store);
    await tombstones.load();
    assert.ok(tombstones.has('e live'));
    assert.ok(!tombstones.has('e old'));
    assert.equal(tombstones.size, 1);
  });

  it('asks for a repeated pull at most once a minute per tombstone', async () => {
    const tombstones = new Tombstones(tombstoneStore().store);
    await tombstones.add('e', 'c', 'gone');
    assert.ok(tombstones.repull('e c'));
    mock.timers.tick(59_000);
    assert.ok(!tombstones.repull('e c'));
    mock.timers.tick(1000);
    assert.ok(tombstones.repull('e c'));
  });
});
