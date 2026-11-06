import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { buildMatcher, buildProjector, normalizeBson, transformAndPrefixQuery } from '../src/matcher.js';

const { ObjectId, Long, Int32, Double, Decimal128, Binary } = mongoose.mongo;

describe('normalizeBson', () => {
  it('tags strings and ObjectIds and unwraps numeric wrappers', () => {
    const id = new ObjectId();
    assert.equal(normalizeBson(id), `o:${id.toHexString()}`);
    assert.equal(normalizeBson('x'), 's:x');
    assert.equal(normalizeBson(`o:${id.toHexString()}`), `s:o:${id.toHexString()}`);
    assert.equal(normalizeBson(Long.fromNumber(42)), 42);
    assert.equal(normalizeBson(new Int32(7)), 7);
    assert.equal(normalizeBson(new Double(1.5)), 1.5);
    assert.equal(normalizeBson(Decimal128.fromString('2.25')), 2.25);
  });

  it('keeps Dates, Buffers, Binary and RegExps as they are', () => {
    const date = new Date();
    const buffer = Buffer.from('hi');
    const binary = new Binary(Buffer.from('bin'));
    const regex = /abc/i;
    assert.equal(normalizeBson(date), date);
    assert.equal(normalizeBson(buffer), buffer);
    assert.equal(normalizeBson(binary), binary);
    assert.equal(normalizeBson(regex), regex);
  });

  it('recurses into plain objects and arrays', () => {
    const id = new ObjectId();
    const date = new Date(0);
    assert.deepEqual(normalizeBson({ a: [id, { b: Long.fromNumber(3), c: date }], d: null, e: 'x' }), {
      a: [`o:${id.toHexString()}`, { b: 3, c: date }],
      d: null,
      e: 's:x',
    });
  });
});

describe('transformAndPrefixQuery', () => {
  it('prefixes plain fields and leaves operators in place', () => {
    assert.deepEqual(transformAndPrefixQuery({ userId: 'u', count: { $gt: 1 } }, 'fullDocument'), {
      'fullDocument.userId': 'u',
      'fullDocument.count': { $gt: 1 },
    });
  });

  it('casts a string _id to ObjectId', () => {
    const id = new ObjectId();
    const out = transformAndPrefixQuery({ _id: id.toHexString() }, 'fullDocument');
    assert.ok(out['fullDocument._id'] instanceof ObjectId);
    assert.equal((out['fullDocument._id'] as InstanceType<typeof ObjectId>).toHexString(), id.toHexString());
  });

  it('recurses through $and / $or arrays', () => {
    assert.deepEqual(transformAndPrefixQuery({ $and: [{ a: 1 }, { $or: [{ b: 2 }, { c: 3 }] }] }, 'fullDocument'), {
      $and: [{ 'fullDocument.a': 1 }, { $or: [{ 'fullDocument.b': 2 }, { 'fullDocument.c': 3 }] }],
    });
  });
});

describe('buildMatcher', () => {
  it('matches ObjectIds in the query against ObjectIds in the event', () => {
    const userId = new ObjectId();
    const match = buildMatcher({ userId });
    assert.equal(match(normalizeBson({ operationType: 'insert', fullDocument: { userId: new ObjectId(userId.toHexString()) } })), true);
    assert.equal(match(normalizeBson({ operationType: 'insert', fullDocument: { userId: new ObjectId() } })), false);
  });

  it('treats a missing field as null', () => {
    const match = buildMatcher({ receiverId: 'r', deletedAt: null });
    assert.equal(match(normalizeBson({ fullDocument: { receiverId: 'r' } })), true);
    assert.equal(match(normalizeBson({ fullDocument: { receiverId: 'r', deletedAt: new Date() } })), false);
  });

  it('only matches a delete event (no fullDocument) when the query cannot exclude it', () => {
    const deleteEvent = normalizeBson({ operationType: 'delete', documentKey: { _id: new ObjectId() } });
    assert.equal(buildMatcher({})(deleteEvent), true);
    assert.equal(buildMatcher({ userId: new ObjectId() })(deleteEvent), false);
  });

  it('rejects queries mingo cannot evaluate', () => {
    assert.throws(() => buildMatcher({ userId: 'u', $comment: 'x' }));
  });

  it('never equates a string with an ObjectId of the same hex', () => {
    const id = new ObjectId();
    assert.equal(buildMatcher({ userId: id.toHexString() })(normalizeBson({ fullDocument: { userId: id } })), false);
    assert.equal(buildMatcher({ userId: id })(normalizeBson({ fullDocument: { userId: id.toHexString() } })), false);
    assert.equal(buildMatcher({ userId: id })(normalizeBson({ fullDocument: { userId: `o:${id.toHexString()}` } })), false);
  });

  it('keeps string comparison and ordering operators working', () => {
    const event = normalizeBson({ fullDocument: { name: 'bob', tags: ['x', 'y'] } });
    assert.equal(buildMatcher({ name: { $in: ['bob', 'eve'] } })(event), true);
    assert.equal(buildMatcher({ name: { $gt: 'alice', $lt: 'carol' } })(event), true);
    assert.equal(buildMatcher({ name: { $ne: 'bob' } })(event), false);
    assert.equal(buildMatcher({ tags: { $all: ['x', 'y'] } })(event), true);
    assert.equal(buildMatcher({ tags: 'y' })(event), true);
    assert.equal(buildMatcher({ name: { $type: 'string' } })(event), true);
  });

  it('keeps a __proto__ key as an own property', () => {
    const normalized = normalizeBson(JSON.parse('{"fullDocument":{"__proto__":{"admin":true},"a":1}}')) as {
      fullDocument: Record<string, unknown>;
    };
    assert.equal(Object.getPrototypeOf(normalized.fullDocument), Object.prototype);
    assert.ok(Object.prototype.hasOwnProperty.call(normalized.fullDocument, '__proto__'));
    assert.equal((normalized.fullDocument as { admin?: boolean }).admin, undefined);
  });

  it('sends regex and server-side JS operators to the dedicated stream', () => {
    assert.throws(() => buildMatcher({ name: { $regex: 'a', $options: 'i' } }), /regex/);
    assert.throws(() => buildMatcher({ name: /a/ }), /regular expression/);
    assert.throws(() => buildMatcher({ $and: [{ tags: { $in: [/x/] } }] }), /regular expression/);
    assert.throws(() => buildMatcher({ $where: 'true' }), /\$where/);
    assert.throws(() => buildMatcher({ $expr: { $eq: ['$a', 1] } }), /\$expr/);
    assert.doesNotThrow(() => buildMatcher({ name: { $in: ['a'] }, $or: [{ a: 1 }] }));
  });
});

describe('buildProjector', () => {
  it('is the identity with no fields', () => {
    const doc = { _id: 1, a: 1 };
    assert.equal(buildProjector({})(doc), doc);
  });

  it('strips exclusions inside array elements and nested objects', () => {
    const project = buildProjector({ 'securityQuestions.answer': false, 'mfa.totpSecret': false });
    assert.deepEqual(
      project({
        _id: 1,
        email: 'e',
        securityQuestions: [
          { question: 'q1', answer: 'a1' },
          { question: 'q2', answer: 'a2' },
        ],
        mfa: { totpSecret: 's', enabled: true },
      }),
      { _id: 1, email: 'e', securityQuestions: [{ question: 'q1' }, { question: 'q2' }], mfa: { enabled: true } }
    );
  });

  it('flattens nested field specs the way the old pipeline did', () => {
    assert.deepEqual(buildProjector({ mfa: { totpSecret: 0 } })({ _id: 1, mfa: { totpSecret: 's', x: 1 } }), {
      _id: 1,
      mfa: { x: 1 },
    });
  });

  it('keeps _id in inclusion mode', () => {
    assert.deepEqual(buildProjector({ email: true })({ _id: 1, email: 'e', name: 'n' }), { _id: 1, email: 'e' });
  });

  it('leaves the shared source document untouched and keeps BSON values by reference', () => {
    const id = new ObjectId();
    const createdAt = new Date();
    const doc = {
      _id: id,
      createdAt,
      mfa: { totpSecret: 's', enabled: true },
      securityQuestions: [{ question: 'q', answer: 'a' }],
    };
    const projected = buildProjector({ 'mfa.totpSecret': false, 'securityQuestions.answer': false })(doc);
    assert.deepEqual(projected, { _id: id, createdAt, mfa: { enabled: true }, securityQuestions: [{ question: 'q' }] });
    assert.equal(projected._id, id);
    assert.equal(projected.createdAt, createdAt);
    assert.deepEqual(doc, {
      _id: id,
      createdAt,
      mfa: { totpSecret: 's', enabled: true },
      securityQuestions: [{ question: 'q', answer: 'a' }],
    });
  });
});

describe('transformAndPrefixQuery operands', () => {
  it('keeps ObjectId and Date operands intact', () => {
    const id = new ObjectId();
    const date = new Date(0);
    const out = transformAndPrefixQuery({ userId: id, createdAt: { $gt: date }, tags: { $in: [id] } }, 'fullDocument');
    assert.equal(out['fullDocument.userId'], id);
    assert.equal((out['fullDocument.createdAt'] as { $gt: Date }).$gt, date);
    assert.equal((out['fullDocument.tags'] as { $in: unknown[] }).$in[0], id);
  });
});

describe('transformAndPrefixQuery scoping', () => {
  it('leaves paths inside $elemMatch and embedded documents relative', () => {
    const userId = new ObjectId();
    assert.deepEqual(
      transformAndPrefixQuery({ users: { $elemMatch: { userId, permissions: 'read' } }, meta: { a: 1 } }, 'fullDocument'),
      { 'fullDocument.users': { $elemMatch: { userId, permissions: 'read' } }, 'fullDocument.meta': { a: 1 } }
    );
  });
});
