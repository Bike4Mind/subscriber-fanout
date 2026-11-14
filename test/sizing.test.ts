import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { sizeEvent } from '../src/sizing.js';

const { BSON } = mongoose.mongo;
const MB = 1024 * 1024;

describe('sizeEvent', () => {
  it('counts one-byte strings once, shared by the normalized copy, and their JSON size', () => {
    const size = sizeEvent({ fullDocument: { pad: 'x'.repeat(MB) } });
    assert.ok(size.heap >= MB && size.heap < 1.1 * MB, `heap ${size.heap}`);
    assert.ok(size.json >= MB && size.json < 1.01 * MB, `json ${size.json}`);
  });

  it('counts strings with characters over 0xFF at two bytes each, and three in UTF-8', () => {
    const size = sizeEvent({ fullDocument: { pad: '中'.repeat(MB) } });
    assert.ok(size.heap >= 2 * MB && size.heap < 2.1 * MB, `heap ${size.heap}`);
    assert.ok(size.json >= 3 * MB && size.json < 3.01 * MB, `json ${size.json}`);
    assert.ok(size.bson >= 3 * MB, `bson ${size.bson}`);
  });

  it('counts the JSON escapes of control characters and quotes', () => {
    assert.ok(sizeEvent({ pad: '\u0001'.repeat(MB) }).json >= 6 * MB);
    assert.ok(sizeEvent({ pad: '"'.repeat(MB) }).json >= 2 * MB);
  });

  it('copies out a Binary that pins the buffer it was read from', () => {
    const uuid = new BSON.UUID();
    const read = BSON.deserialize(BSON.serialize({ pad: 'x'.repeat(100_000), fullDocument: { u: uuid } }));
    const before = read.fullDocument.u.buffer.buffer.byteLength;
    assert.ok(before > 100_000);
    sizeEvent(read);
    assert.equal(read.fullDocument.u.buffer.buffer.byteLength, 16);
    assert.equal(read.fullDocument.u.toHexString(), uuid.toHexString());
  });
});
