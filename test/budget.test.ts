import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BUDGET_CEILING, BUDGET_FLOOR, Budget, pauseEstimate, sizeBudget } from '../src/budget.js';

const MB = 1024 * 1024;

describe('sizeBudget', () => {
  const heap = { startCount: 0, perSubscription: 0, count: 0 };

  it('is a third of the heap left after what is in use', () => {
    assert.deepEqual(sizeBudget({ ...heap, heapLimit: 1024 * MB, base: 304 * MB }), { limit: 240 * MB, free: 240 * MB, used: 304 * MB });
  });

  it('stays between the floor and the ceiling, and reports the shortfall under the floor', () => {
    assert.equal(sizeBudget({ ...heap, heapLimit: 4096 * MB, base: 100 * MB }).limit, BUDGET_CEILING);
    const tight = sizeBudget({ ...heap, heapLimit: 259 * MB, base: 235 * MB });
    assert.equal(tight.limit, BUDGET_FLOOR);
    assert.equal(tight.free, 8 * MB);
  });

  it('counts the heap of subscriptions added or removed since startup', () => {
    const at = (count: number) =>
      sizeBudget({ heapLimit: 1024 * MB, base: 424 * MB, startCount: 10_000, perSubscription: 10 * 1024, count }).limit;
    assert.equal(at(10_000), 200 * MB);
    assert.ok(Math.abs(at(20_240) - (200 * MB - (10_240 * 10 * 1024) / 3)) < 1);
    assert.ok(at(0) > at(10_000));
  });
});

describe('Budget', () => {
  it('charges an event once while anything holds it, per stream, and counts bodies apart from the heap', () => {
    const budget = new Budget(1000);
    const [event, stream] = [{}, {}];
    budget.hold(event, 100, stream);
    budget.hold(event, 100, stream);
    budget.add(30, stream);
    budget.bodies += 500;
    assert.equal(budget.heap, 130);
    assert.equal(budget.stored, 630);
    assert.equal(budget.streamBytes(stream), 130);
    assert.equal(budget.release(event), 0);
    assert.equal(budget.release(event), 100);
    budget.add(-30, stream);
    assert.equal(budget.heap, 0);
    assert.equal(budget.streamBytes(stream), 0);
  });
});

describe('Budget.heaviest', () => {
  it('picks the stream holding the most, only if it holds at least an eighth of the budget and its largest event', () => {
    const budget = new Budget(800);
    const [a, b, c] = [{ largest: 0 }, { largest: 300 }, { largest: 0 }];
    budget.hold({}, 90, a);
    budget.hold({}, 250, b);
    assert.equal(budget.heaviest([a, b, c], s => s.largest), undefined);
    budget.hold({}, 60, b);
    assert.equal(budget.heaviest([a, b, c], s => s.largest), b);
    budget.hold({}, 400, a);
    assert.equal(budget.heaviest([a, b, c], s => s.largest), a);
    budget.add(5000);
    assert.equal(budget.heaviest([c], s => s.largest), undefined);
  });
});

describe('pauseEstimate', () => {
  it('gives about 4s for 50 clients of 2MB documents at 50ms per POST over a 64MB budget', () => {
    const { ms, drain, perPost } = pauseEstimate({
      stored: 70 * MB,
      limit: 64 * MB,
      charge: 6 * MB,
      posts: 35,
      medianMs: 50,
      connections: 50,
      slots: 48,
    });
    assert.equal(drain, 14 * MB);
    assert.ok(Math.abs(perPost - (6 * MB) / 35) < 1);
    assert.ok(Math.abs(ms - (14 * 35 * 50 * 50) / (6 * 48)) < 1, `${ms}`);
  });
});

describe('Budget.attach', () => {
  it('charges a shared object once, against the first stream, until the last event attached to it is released', () => {
    const budget = new Budget(10_000);
    const [s1, s2] = [{}, {}];
    const [e1, e2] = [{}, {}];
    const shared = {};
    let freed = 0;
    budget.hold(e1, 100, s1);
    budget.hold(e2, 100, s2);
    assert.ok(budget.attach(e1, shared, 1000, () => freed++));
    assert.ok(budget.attach(e2, shared, 1000, () => freed++));
    assert.ok(!budget.attach({}, shared, 1000));
    assert.equal(budget.stored, 1200);
    assert.equal(budget.streamBytes(s1), 1100);
    assert.equal(budget.streamBytes(s2), 100);
    assert.equal(budget.freedBy(e1), 100);
    assert.equal(budget.release(e1), 100);
    assert.equal(budget.freedBy(e2), 1100);
    assert.equal(budget.release(e2), 1100);
    assert.equal(budget.stored, 0);
    assert.equal(freed, 1);
  });
});
