import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { Bodies } from '../src/bodies.js';
import { Budget } from '../src/budget.js';

beforeEach(() => mock.timers.enable({ apis: ['Date'], now: 0 }));
afterEach(() => mock.timers.reset());

const make = (bytes: number) => () => Buffer.alloc(bytes);
const nothing = () => {};

describe('Bodies', () => {
  it('builds one body per owner and projection, charges its size, and frees it with its last delivery', () => {
    const budget = new Budget(10_000);
    const bodies = new Bodies(budget);
    const [event, stream] = [{}, {}];
    let builds = 0;
    const build = () => (builds++, Buffer.alloc(50));
    const a = bodies.take(event, '{}', 500, stream, nothing, build);
    const b = bodies.take(event, '{}', 500, stream, nothing, build);
    const other = bodies.take(event, '{"x":0}', 500, stream, nothing, make(20));
    assert.ok(a && a !== 'wait' && b && b !== 'wait' && other && other !== 'wait');
    assert.equal(a, b);
    assert.notEqual(a, other);
    assert.equal(builds, 1);
    assert.equal(budget.bodies, 70);
    bodies.release(event, '{}', a);
    assert.equal(budget.bodies, 70);
    bodies.release(event, '{}', b);
    bodies.release(event, '{"x":0}', other);
    assert.equal(budget.bodies, 0);
    assert.ok(!bodies.has(event));
  });

  it('builds while what is stored is under the budget, even past it after', () => {
    const budget = new Budget(1000);
    const bodies = new Bodies(budget);
    budget.add(900);
    assert.notEqual(bodies.take({}, '', 2000, {}, nothing, make(2000)), 'wait');
    assert.equal(budget.stored, 2900);
  });

  it('over the budget, builds the first waiter once its body fits beside the other bodies, in the order they asked', () => {
    const budget = new Budget(1000);
    const bodies = new Bodies(budget);
    const [s1, s2, s3] = [{}, {}, {}];
    const [e1, e2, e3] = [{}, {}, {}];
    budget.add(1200);
    const first = bodies.take(e1, '', 300, s1, nothing, make(300));
    assert.ok(first && first !== 'wait');
    const woken: string[] = [];
    const wakeS2 = () => woken.push('s2');
    const wakeS3 = () => woken.push('s3');
    assert.equal(bodies.take(e2, '', 800, s2, wakeS2, make(800)), 'wait');
    assert.equal(bodies.take(e3, '', 100, s3, wakeS3, make(100)), 'wait', 'a later builder waits behind the first');
    bodies.release(e1, '', first);
    bodies.wake();
    assert.deepEqual(woken, ['s2']);
    assert.equal(bodies.take(e3, '', 100, s3, wakeS3, make(100)), 'wait');
    assert.notEqual(bodies.take(e2, '', 800, s2, wakeS2, make(800)), 'wait');
    assert.deepEqual(woken, ['s2', 's3']);
    assert.notEqual(bodies.take(e3, '', 100, s3, wakeS3, make(100)), 'wait');
    assert.equal(budget.bodies, 900);
  });

  it('wakes every waiter once what is stored is under the budget', () => {
    const budget = new Budget(1000);
    const bodies = new Bodies(budget);
    budget.add(1000);
    const held = bodies.take({}, '', 500, {}, nothing, make(500));
    assert.ok(held && held !== 'wait');
    let woken = 0;
    for (const json of [600, 700]) assert.equal(bodies.take({}, '', json, {}, () => woken++, make(json)), 'wait');
    budget.add(-600);
    bodies.wake();
    assert.equal(woken, 2);
  });

  it('reports how long the first waiter has waited, and lets the next go when it is cancelled', () => {
    const budget = new Budget(1000);
    const bodies = new Bodies(budget);
    budget.add(1000);
    bodies.take({}, '', 1000, {}, nothing, make(1000));
    assert.equal(bodies.waitMs(), 0);
    const [s1, s2] = [{}, {}];
    let woken = 0;
    bodies.take({}, '', 500, s1, nothing, make(500));
    mock.timers.tick(300);
    bodies.take({}, '', 10, s2, () => woken++, make(10));
    mock.timers.tick(700);
    assert.equal(bodies.waitMs(), 1000);
    budget.bodies -= 1000;
    bodies.cancel(s1);
    assert.equal(bodies.waitMs(), 700);
    assert.equal(woken, 1);
  });

  it('sends nothing for a body that cannot be made, and charges nothing', () => {
    const budget = new Budget(1000);
    const bodies = new Bodies(budget);
    assert.equal(
      bodies.take({}, '', 100, {}, nothing, () => undefined),
      undefined
    );
    assert.equal(budget.bodies, 0);
  });
});
