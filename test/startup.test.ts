import { afterEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import mongoose from 'mongoose';
import { SubscriberFanout } from '../src/fanout.js';

const SECRET = 'Zq7Mk3';
const FAST = 'serverSelectionTimeoutMS=300';
const ROOT = fileURLToPath(new URL('..', import.meta.url));

// Each string hides SECRET where the driver may quote it and cannot reach a server.
// The second column is the target the log must still show.
const cases: Array<[string, string?]> = [
  [`mongodb://127.0.0.1:1/db?tlsCertificateKeyFilePassword=ab&${SECRET}x=1`, 'mongodb://127.0.0.1:1/db'],
  [`mongodb://127.0.0.1:1/db?proxyHost=127.0.0.1&proxyPassword=${SECRET}`],
  [`mongodb://127.0.0.1:1/db?${FAST}&proxyHost=127.0.0.1&proxyPort=1&proxyUsername=u&proxyPassword=${SECRET}`],
  [
    `mongodb://AKIA${SECRET}:${SECRET}@127.0.0.1:1/db?authMechanism=MONGODB-AWS&authSource=admin&authMechanismProperties=AWS_SESSION_TOKEN:${SECRET}`,
  ],
  [
    `mongodb://AKIA${SECRET}:${SECRET}@127.0.0.1:1/db?${FAST}&authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:${SECRET}`,
    'mongodb://***:***@127.0.0.1:1/db',
  ],
  [`mongodb://user:${SECRET}@/tmp/subscriber-fanout-test.sock/db`],
  [
    `mongodb://user:${SECRET}@%2Ftmp%2Fsubscriber-fanout-test.sock/db?${FAST}`,
    'mongodb://***:***@%2Ftmp%2Fsubscriber-fanout-test.sock/db',
  ],
  [`mongodb://user:${SECRET}@[::1:1/db`],
  [`mongodb://user:${SECRET}@[::1]:1/db?${FAST}`, 'mongodb://***:***@[::1]:1/db'],
  [`mongodb://user:${SECRET}@127.0.0.1:1/db?${FAST}`, 'mongodb://***:***@127.0.0.1:1/db'],
];

function runIndex(uri: string) {
  return new Promise<{ code: number | null; output: string }>(resolve => {
    const env = { ...process.env, MONGODB_URI: uri, STAGE: 'test', WEBSOCKET_MANAGEMENT_ENDPOINT: 'https://test.invalid' };
    const child = execFile(process.execPath, ['--import', 'tsx', 'src/index.ts'], { cwd: ROOT, env, timeout: 30000 });
    let output = '';
    child.stdout?.on('data', chunk => (output += chunk));
    child.stderr?.on('data', chunk => (output += chunk));
    child.on('close', code => resolve({ code, output }));
  });
}

describe('startup with a connection string that carries secrets', () => {
  afterEach(() => mongoose.disconnect());

  it('never logs a secret from start(), and still logs the target', async t => {
    for (const [uri, target] of cases) {
      const logged: string[] = [];
      for (const level of ['debug', 'info', 'log', 'warn', 'error'] as const) {
        t.mock.method(console, level, (...args: unknown[]) => logged.push(args.map(String).join(' ')));
      }
      const fanout = new SubscriberFanout({ mongoUri: uri, stage: 'test', managementEndpoint: 'https://test.invalid' });
      await assert.rejects(fanout.start(), (error: Error) => {
        logged.push(String(error.stack));
        return true;
      });
      await mongoose.disconnect();
      t.mock.restoreAll();

      const output = logged.join('\n');
      assert.ok(!output.toLowerCase().includes(SECRET.toLowerCase()), `${uri}:\n${output}`);
      if (target) assert.ok(output.includes(`connecting to MongoDB at ${target}\n`), `${uri}:\n${output}`);
    }
  });

  it('never prints a secret on the exit path', async () => {
    const runs = await Promise.all(cases.map(([uri]) => runIndex(uri)));
    runs.forEach(({ code, output }, i) => {
      assert.equal(code, 1, `${cases[i][0]}:\n${output}`);
      assert.ok(output.includes('failed to connect to MongoDB'), `${cases[i][0]}:\n${output}`);
      assert.ok(!output.toLowerCase().includes(SECRET.toLowerCase()), `${cases[i][0]}:\n${output}`);
    });
  });
});
