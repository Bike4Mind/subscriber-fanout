import diagnostics_channel from 'node:diagnostics_channel';
import dns from 'node:dns';
import type { ClientRequest } from 'node:http';
import type { LookupFunction, Socket } from 'node:net';
import type { TLSSocket } from 'node:tls';

const DNS_TTL_MS = 30 * 1000;
const DNS_STALE_MS = 5 * 60 * 1000;
const CONNECTIONS_PATH = '/@connections/';

const hooks: Map<string, () => void> = new Map();
const bases: Map<string, string> = new Map();
let subscribed = false;

// How a request to the endpoint names it: its Host header and the path before /@connections/.
function base(endpoint: string) {
  let value = bases.get(endpoint);
  if (value === undefined) {
    const url = new URL(endpoint);
    bases.set(endpoint, (value = url.host + url.pathname.replace(/\/+$/, '')));
  }
  return value;
}

function connected(socket: Socket) {
  if ((socket as TLSSocket).encrypted) {
    const protocol = (socket as TLSSocket).getProtocol();
    return !socket.connecting && protocol !== null && protocol !== 'unknown';
  }
  return !socket.connecting;
}

function onRequest(message: unknown) {
  const request = (message as { request?: ClientRequest }).request;
  if (!request || request.method !== 'POST') return;
  const at = request.path.indexOf(CONNECTIONS_PATH);
  if (at < 0) return;
  const id = decodeURIComponent(request.path.slice(at + CONNECTIONS_PATH.length));
  // Taken now, so a cut attempt's late socket cannot fire the hook of the attempt after it.
  const hook = hooks.get(`${request.getHeader('host')}${request.path.slice(0, at)} ${id}`);
  if (!hook) return;
  request.once('socket', (socket: Socket) => {
    if (connected(socket)) return hook();
    socket.once((socket as TLSSocket).encrypted ? 'secureConnect' : 'connect', hook);
  });
}

// Calls hook once the POST to this connection has a connected socket: at assignment for a reused one, at
// connect or secureConnect for a new one. One POST is in flight per connection. Returns the unregister.
export function onConnected(endpoint: string, connectionId: string, hook: () => void) {
  if (!subscribed) {
    diagnostics_channel.subscribe('http.client.request.created', onRequest);
    subscribed = true;
  }
  const key = `${base(endpoint)} ${connectionId}`;
  hooks.set(key, hook);
  return () => {
    if (hooks.get(key) === hook) hooks.delete(key);
  };
}

type Resolve = (
  hostname: string,
  options: { all: true },
  callback: (error: NodeJS.ErrnoException | null, addresses: dns.LookupAddress[]) => void
) => void;

type Entry = { at: number; addresses: dns.LookupAddress[]; next: number };

// An agent lookup that caches each host for 30s, rotating through its addresses, serves a stale entry for up to
// 5 minutes when a lookup fails, and keeps one lookup in flight per host. The libuv pool behind dns.lookup has
// four threads, which a burst of new sockets would otherwise queue on.
export function cachingLookup(resolve: Resolve = dns.lookup): LookupFunction {
  const cache: Map<string, Entry> = new Map();
  const inFlight: Map<string, Array<(entry?: Entry, error?: Error) => void>> = new Map();
  const rotate = (entry: Entry) => {
    const { addresses, next } = entry;
    entry.next = (next + 1) % addresses.length;
    return [...addresses.slice(next), ...addresses.slice(0, next)];
  };
  return (hostname, options, callback) => {
    const answer = (entry?: Entry, error?: Error) => {
      const family = options.family === 4 || options.family === 'IPv4' ? 4 : options.family === 6 || options.family === 'IPv6' ? 6 : 0;
      const addresses = entry ? rotate(entry).filter(a => !family || a.family === family) : [];
      if (addresses.length === 0) {
        const failure = error ?? Object.assign(new Error(`no address for ${hostname}`), { code: 'ENOTFOUND' });
        return callback(failure, options.all ? [] : '');
      }
      if (options.all) callback(null, addresses);
      else callback(null, addresses[0].address, addresses[0].family);
    };
    const entry = cache.get(hostname);
    const waiting = inFlight.get(hostname);
    if (entry && (Date.now() - entry.at < DNS_TTL_MS || waiting)) return process.nextTick(answer, entry);
    if (waiting) return void waiting.push(answer);
    inFlight.set(hostname, [answer]);
    resolve(hostname, { all: true }, (error, addresses) => {
      const callbacks = inFlight.get(hostname)!;
      inFlight.delete(hostname);
      if (!error && addresses.length > 0) cache.set(hostname, { at: Date.now(), addresses, next: 0 });
      const current = cache.get(hostname);
      const usable = current && Date.now() - current.at < DNS_STALE_MS ? current : undefined;
      for (const done of callbacks) done(usable, error ?? undefined);
    });
  };
}
