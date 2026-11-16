import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import type dns from 'node:dns';
import { cachingLookup, onConnected } from '../src/connect.js';

describe('onConnected', () => {
  it('fires once the POST to the connection has a connected socket, new or reused, and only for that endpoint and id', async () => {
    const server = http.createServer((request, response) => setTimeout(() => response.end('ok'), 50));
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;
    const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
    const post = (path: string) =>
      new Promise<number>((resolve, reject) => {
        const request = http.request({ host: '127.0.0.1', port, path, method: 'POST', agent }, response => {
          response.resume();
          response.on('end', () => resolve(Date.now()));
        });
        request.on('error', reject);
        request.end('{}');
      });
    try {
      const fired: string[] = [];
      const endpoint = `http://127.0.0.1:${port}/stage`;
      const off = onConnected(endpoint, 'a b', () => fired.push('a b'));
      const other = onConnected(`http://127.0.0.1:${port}/other`, 'a b', () => fired.push('other'));
      await post('/stage/@connections/a%20b');
      assert.deepEqual(fired, ['a b']);
      await post('/stage/@connections/a%20b');
      assert.deepEqual(fired, ['a b', 'a b'], 'a reused socket fires at assignment');
      off();
      await post('/stage/@connections/a%20b');
      assert.deepEqual(fired, ['a b', 'a b']);
      other();
    } finally {
      agent.destroy();
      server.close();
    }
  });
});

describe('cachingLookup', () => {
  beforeEach(() => mock.timers.enable({ apis: ['Date'], now: 0 }));
  afterEach(() => mock.timers.reset());

  const addresses: dns.LookupAddress[] = [
    { address: '10.0.0.1', family: 4 },
    { address: '10.0.0.2', family: 4 },
  ];

  function resolver() {
    const calls: Array<(error: NodeJS.ErrnoException | null, addresses: dns.LookupAddress[]) => void> = [];
    return { calls, resolve: (_host: string, _options: { all: true }, callback: (typeof calls)[number]) => void calls.push(callback) };
  }

  const one = (lookup: ReturnType<typeof cachingLookup>) =>
    new Promise<string>((resolve, reject) => lookup('gw', {}, (error, address) => (error ? reject(error) : resolve(address as string))));

  it('keeps one lookup in flight per host, caches it for 30s and rotates through its addresses', async () => {
    const r = resolver();
    const lookup = cachingLookup(r.resolve);
    const first = [one(lookup), one(lookup)];
    assert.equal(r.calls.length, 1);
    r.calls[0](null, addresses);
    assert.deepEqual(await Promise.all(first), ['10.0.0.1', '10.0.0.2']);
    assert.equal(await one(lookup), '10.0.0.1');
    assert.equal(r.calls.length, 1);
    mock.timers.tick(30_000);
    const refreshed = one(lookup);
    assert.equal(r.calls.length, 2);
    r.calls[1](null, [{ address: '10.0.0.3', family: 4 }]);
    assert.equal(await refreshed, '10.0.0.3');
  });

  it('serves the stale entry for up to 5 minutes when a lookup fails', async () => {
    const r = resolver();
    const lookup = cachingLookup(r.resolve);
    const first = one(lookup);
    r.calls[0](null, addresses.slice(0, 1));
    await first;
    mock.timers.tick(60_000);
    const stale = one(lookup);
    r.calls[1](Object.assign(new Error('timeout'), { code: 'EAI_AGAIN' }), []);
    assert.equal(await stale, '10.0.0.1');
    mock.timers.tick(5 * 60_000);
    const failed = one(lookup);
    r.calls[2](Object.assign(new Error('timeout'), { code: 'EAI_AGAIN' }), []);
    await assert.rejects(failed, { code: 'EAI_AGAIN' });
  });
});
