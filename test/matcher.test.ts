import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import { buildMatcher, buildProjector, normalizeBson, transformAndPrefixQuery } from '../src/matcher.js';

const { ObjectId, Long, Int32, Double, Decimal128, Binary } = mongoose.mongo;

describe('normalizeBson', () => {
  it('unwraps ObjectId and numeric wrappers', () => {
    const id = new ObjectId();
    assert.equal(normalizeBson(id), id.toHexString());
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
      a: [id.toHexString(), { b: 3, c: date }],
      d: null,
      e: 'x',
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
