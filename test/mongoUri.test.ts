import { test } from 'node:test';
import assert from 'node:assert/strict';
import { describeConnectError, describeMongoUri } from '../src/mongoUri.ts';

const passwords = ['p=w;d|x y', 'pässwörd', 'p%40ss', 'плохой пароль'];

test('never echoes a well-formed password, whatever its characters', () => {
  for (const pw of passwords) {
    const out = describeMongoUri(`mongodb://user:${pw}@host1:27017/db?authSource=admin`);
    assert.equal(out, 'mongodb://***:***@host1:27017/db');
  }
});

test('handles a multi-host replica set with ports', () => {
  const out = describeMongoUri('mongodb://user:se=cret@h1:27017,h2:27018,h3:27019/db?replicaSet=rs0');
  assert.equal(out, 'mongodb://***:***@h1:27017,h2:27018,h3:27019/db');
});

test('keeps a credential-free string readable', () => {
  assert.equal(describeMongoUri('mongodb://127.0.0.1:27017/bike4mind'), 'mongodb://127.0.0.1:27017/bike4mind');
  assert.equal(describeMongoUri('mongodb://[::1]:27017/db'), 'mongodb://[::1]:27017/db');
  assert.equal(describeMongoUri('mongodb+srv://cluster.example.net/db?retryWrites=true'), 'mongodb+srv://cluster.example.net/db');
});

test('drops the query string, which can carry options with secrets', () => {
  const out = describeMongoUri('mongodb://h/db?authMechanismProperties=AWS_SESSION_TOKEN:tok');
  assert.equal(out, 'mongodb://h/db');
});

test('shows nothing after an @ that sits in the query string or fragment', () => {
  const cases = [
    'mongodb://u:pw@h/db?tlsCertificateKeyFilePassword=ab@SECRET/x',
    'mongodb://u:pw@h/db?authMechanismProperties=AWS_SESSION_TOKEN:abc@SECRET',
    'mongodb://u:pw@h/db#frag@SECRET',
  ];
  for (const uri of cases) {
    const out = describeMongoUri(uri);
    assert.ok(!out.includes('SECRET') && !out.includes('pw'), `${uri} -> ${out}`);
  }
});

test('falls back to a placeholder when the string is not in the expected form', () => {
  const cases = [
    'mongodb://user:secret',
    'mongodb://user:a/b@h/db',
    'mongodb://user:a?b@h/db',
    'mongodb://user:abc@TAILSECRET@h/db',
    'mongodb://user:p@secret/word@h/db',
    'mongodb://admin:Xk9@secret?f8@h/db',
    'user:secret@host/db',
    'secret://user:secret@host/db',
  ];
  for (const uri of cases) {
    const out = describeMongoUri(uri);
    assert.ok(!out.toLowerCase().includes('secret'), `${uri} -> ${out}`);
    assert.ok(!out.includes('user'), `${uri} -> ${out}`);
  }
});

test('scrubs the password from driver messages for a well-formed string', () => {
  const cases: [string, string][] = [
    ['mongodb://user:S3CRET@h/db', 'MongoParseError: something about "mongodb://user:S3CRET@h/db"'],
    ['mongodb://user:p%40ss@h/db', 'Password p@ss contains unescaped characters'],
    ['mongodb://user:TAIL:SECRET@h/db', 'Unable to parse TAIL:SECRET with URL'],
    ['mongodb://user:p w@h/db', 'invalid password "p%20w"'],
    ['mongodb://h/db?authMechanismProperties=AWS_SESSION_TOKEN:SECRETTOK', 'option value SECRETTOK is invalid'],
    ['mongodb://h/db?tlsCertificateKeyFilePassword=se%40cret', 'option se@cret is invalid'],
    ['mongodb://h/db?tlsCertificateKeyFilePassword=se+cret', 'option "se cret" is invalid'],
  ];
  for (const [uri, message] of cases) {
    const out = describeConnectError(new Error(message), uri);
    assert.ok(!out.toLowerCase().includes('secret'), `${uri}: ${out}`);
    assert.ok(!out.includes('p@ss') && !out.includes('p%20w') && !out.includes('se@cret') && !out.includes('se cret'), `${uri}: ${out}`);
  }
});

test('keeps only the error class when the string is not in the expected form', () => {
  const cases: [string, string][] = [
    ['mongodb://user:S3CRET@', 'Protocol and host list are required in "mongodb://user:S3CRET@"'],
    ['mongodb://user:S3CRET', 'Unable to parse user:S3CRET with URL'],
    ['mongodb://user:p@secret/word@h/db', 'getaddrinfo ENOTFOUND secret'],
    ['mongodb://user:abc@TAILSECRET@h/db', 'getaddrinfo ENOTFOUND tailsecret'],
  ];
  for (const [uri, message] of cases) {
    const error = new Error(message);
    error.name = 'MongoParseError';
    assert.equal(describeConnectError(error, uri), 'MongoParseError');
  }
});

test('leaves diagnostics readable when the password is short or absent', () => {
  const err = (m: string) => Object.assign(new Error(m), { name: 'MongoServerError' });
  assert.equal(
    describeConnectError(err('connect ECONNREFUSED 10.0.0.1:27017 after 30000ms'), 'mongodb://a:1@10.0.0.1:27017/db'),
    'MongoServerError: connect ECONNREFUSED 10.0.0.1:27017 after 30000ms',
  );
  assert.equal(
    describeConnectError(err('bad auth : Authentication failed.'), 'mongodb://admin:e@h/db'),
    'MongoServerError: bad auth : Authentication failed.',
  );
  assert.equal(describeConnectError(err('getaddrinfo ENOTFOUND h1'), 'mongodb://h1:27017,h2:27017/db'), 'MongoServerError: getaddrinfo ENOTFOUND h1');
  assert.equal(describeConnectError('boom', 'mongodb://h/db'), 'unknown error');
});
