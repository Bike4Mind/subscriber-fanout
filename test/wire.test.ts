import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CHUNK_DATA_SIZE, Frames, MAX_WS_PAYLOAD_BYTES, REQUEST_BYTES, messagePrefix, messageTail } from '../src/wire.js';

// Reference chunking: the whole message serialized, then cut at UTF-8 boundaries.
function wholeMessageChunks(serialized: string) {
  const buf = Buffer.from(serialized, 'utf8');
  if (buf.length <= MAX_WS_PAYLOAD_BYTES) return [serialized];
  const chunks: string[] = [];
  let offset = 0;
  while (offset < buf.length) {
    let end = Math.min(offset + CHUNK_DATA_SIZE, buf.length);
    while (end > offset && (buf[end] & 0xc0) === 0x80) end--;
    chunks.push(buf.subarray(offset, end).toString('utf8'));
    offset = end;
  }
  return chunks;
}

function update(text: string) {
  return { collectionName: 'items', operationType: 'update', data: { _id: 'a1', id: 'a1', text, n: 1, nested: { q: '"\\\n' } } };
}

describe('wire', () => {
  for (const [label, clientId, text] of [
    ['ascii', 'client-1', 'x'.repeat(50)],
    ['undefined id', undefined, 'y'.repeat(50)],
    ['multibyte', 'cé', '中\u{1f600}'.repeat(20)],
  ] as const) {
    it(`serializes a message byte for byte as before (${label})`, () => {
      const u = update(text);
      const whole = JSON.stringify({ action: 'data_update', subscriptionId: clientId, ...u });
      const frames = new Frames(messagePrefix(clientId), messageTail(u));
      assert.equal(frames.count, 1);
      assert.equal(frames.frame(0).toString(), whole);
    });
  }

  for (const [label, text] of [
    ['ascii', 'x'.repeat(400 * 1024)],
    ['multibyte across chunk boundaries', '中é\u{1f600}a'.repeat(60 * 1024)],
  ] as const) {
    it(`cuts chunks at the same boundaries as the whole message (${label})`, () => {
      const u = update(text);
      const whole = JSON.stringify({ action: 'data_update', subscriptionId: 'client-1', ...u });
      const frames = new Frames(messagePrefix('client-1'), messageTail(u));
      const expected = wholeMessageChunks(whole);
      assert.equal(frames.count, expected.length);
      const parsed = Array.from({ length: frames.count }, (_, i) => JSON.parse(frames.frame(i).toString()));
      assert.deepEqual(
        parsed.map(p => p._data),
        expected
      );
      assert.ok(parsed.every(p => p._chunked && p._chunkId === parsed[0]._chunkId && p._totalChunks === frames.count));
      assert.deepEqual(
        parsed.map(p => p._chunkIndex),
        expected.map((_, i) => i)
      );
      assert.notEqual(new Frames(messagePrefix('client-1'), messageTail(u)).frame(0), frames.frame(0), 'a fresh _chunkId per send');
      assert.equal(parsed.map(p => p._data).join(''), whole);
    });
  }

  it('charges each POST the exact length of its frame plus the request', () => {
    for (const text of ['x'.repeat(50), '"\\'.repeat(200 * 1024), '中'.repeat(100 * 1024)]) {
      const frames = new Frames(messagePrefix('client-1'), messageTail(update(text)));
      for (let i = 0; i < frames.count; i++) assert.equal(frames.bytes(i), frames.frame(i).length + REQUEST_BYTES);
    }
  });

  it('allocates prefixes, tails and frames outside the shared Buffer pool', () => {
    const prefix = messagePrefix('client-1');
    const small = messageTail(update('x'.repeat(50)));
    const large = messageTail(update('x'.repeat(300 * 1024)));
    const own = (b: Buffer) => b.byteOffset === 0 && b.buffer.byteLength === b.length;
    assert.ok(own(prefix) && own(small));
    assert.ok(own(new Frames(prefix, small).frame(0)));
    const chunks = new Frames(prefix, large);
    for (let i = 0; i < chunks.count; i++) assert.ok(own(chunks.frame(i)));
  });

  it('sends a large message whole when chunking is off', () => {
    const u = update('z'.repeat(300 * 1024));
    const frames = new Frames(messagePrefix('c'), messageTail(u), false);
    assert.equal(frames.count, 1);
    assert.equal(frames.frame(0).toString(), JSON.stringify({ action: 'data_update', subscriptionId: 'c', ...u }));
  });
});
