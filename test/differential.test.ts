import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import mongoose from 'mongoose';
import { MongoMemoryServer } from 'mongodb-memory-server';
import { flatten } from 'flat';
import { pickBy } from 'lodash-es';
import {
  InvalidFilterError,
  buildMatcher,
  buildProjector,
  normalizeBson,
  transformAndPrefixQuery,
} from '../src/matcher.js';

const { MongoClient, ObjectId, Long, Decimal128, Int32, BSON } = mongoose.mongo;

// 'in-process' agrees with the server, 'server' falls back to a dedicated stream,
// 'invalid' is rejected by both, 'fails-closed' never matches in process.
type Route = 'in-process' | 'server' | 'invalid' | 'fails-closed';
type Doc = Record<string, unknown>;
const DELETE = null;
const LOOKUP_GONE = Symbol('update whose document was deleted before the lookup');

const X = new ObjectId();
const Y = new ObjectId();
const D1 = new Date('2026-01-01T00:00:00Z');
const D2 = new Date('2026-06-01T00:00:00Z');
const BIG = Long.fromString('9007199254740993');

const filterCases: Array<[Doc, Doc | typeof DELETE | typeof LOOKUP_GONE, Route?]> = [
  // ObjectId and string equality, _id casting, null and missing
  [{ userId: X }, { userId: X }],
  [{ userId: X }, { userId: X.toHexString() }],
  [{ userId: X.toHexString() }, { userId: X }],
  [{ _id: X.toHexString() }, { _id: X }],
  [{ _id: 'plain' }, { _id: 'plain' }],
  [{ receiverId: X, deletedAt: null }, { receiverId: X }],
  [{ receiverId: X, deletedAt: null }, { receiverId: X, deletedAt: D1 }],
  [{ deletedAt: { $ne: null } }, {}],
  [{ tags: null }, { tags: [1, null] }],
  [{ 'members.deletedAt': null }, { members: [{ deletedAt: D1 }, { n: 1 }] }, 'server'],
  [{ tags: ['x', 'y'] }, { tags: ['x', 'y'] }],
  [{ tags: ['x', 'y'] }, { tags: [['x', 'y']] }],
  [{ tags: ['x', 'y'] }, { tags: ['y', 'x'] }],
  [{ meta: { a: 1 } }, { meta: { a: 1 } }, 'server'],
  [{ hasOwnProperty: 'x' }, { hasOwnProperty: 'x' }],
  // Delete events carry no fullDocument
  [{}, DELETE],
  [{ userId: X }, DELETE],
  [{ deletedAt: null }, DELETE],
  [{ _id: X.toHexString() }, DELETE],
  [{ v: { $ne: 1 } }, DELETE],
  [{ v: { $exists: false } }, DELETE],
  // Dates and ranges across types
  [{ at: D1 }, { at: new Date(D1) }],
  [{ at: { $gte: D1 } }, { at: D2 }],
  [{ at: { $gte: D2 } }, { at: D1 }],
  [{ at: { $gt: D1 } }, { at: 5 }],
  [{ v: { $gt: 1 } }, { v: '5' }],
  [{ v: { $gt: 1 } }, { v: D2 }],
  [{ v: { $gt: 1, $lte: new Int32(3) } }, { v: 2 }],
  [{ v: { $gt: 1 } }, { v: [0, 5] }],
  [{ v: { $gt: 1 } }, { v: [[5]] }],
  [{ v: { $gt: 'a' } }, { v: 'b' }, 'server'],
  // Object-form $elemMatch, including the sharing scopes
  [{ users: { $elemMatch: { userId: 'u1', permissions: 'read' } } }, { users: [{ userId: 'u1', permissions: ['read'] }] }],
  [
    { users: { $elemMatch: { userId: 'u1', permissions: 'read' } } },
    { users: [{ userId: 'u1', permissions: ['share'] }, { userId: 'u2', permissions: ['read'] }] },
  ],
  [{ groups: { $elemMatch: { groupId: { $in: [X, Y] }, permissions: 'read' } } }, { groups: [{ groupId: Y, permissions: ['read'] }] }],
  [{ users: { $elemMatch: { userId: 'X' } } }, { users: ['X'] }],
  [{ users: { $elemMatch: { userId: 'X' } } }, { users: [[{ userId: 'X' }]] }],
  [{ users: { $elemMatch: { x: { $exists: true } } } }, { users: ['a', 5] }],
  [{ users: { $elemMatch: { x: { $exists: false } } } }, { users: ['a', 5, [1]] }],
  [{ users: { $elemMatch: { x: { $ne: 5 } } } }, { users: ['a'] }],
  [{ users: { $elemMatch: { x: { $ne: 5 } } } }, { users: [[{ x: 1 }]] }],
  [{ users: { $elemMatch: {} } }, { users: ['a'] }],
  [{ users: { $elemMatch: {} } }, { users: [[{}]] }],
  [{ v: { $elemMatch: { $nor: [{ x: 1 }] } } }, { v: [[{ x: 1 }], 5] }],
  [{ v: { $elemMatch: { $gte: 1 } } }, { v: [[5]] }, 'server'],
  [{ 'a.users': { $elemMatch: { x: 1 } } }, { a: [{ users: [{ x: 1 }] }] }, 'server'],
  // Paths are matched against fullDocument, not the event
  [{ userId: 'u1' }, { userId: [['u1']] }],
  [{ userId: { $ne: 'u1' } }, { userId: [['u1']] }],
  [{ $nor: [{ userId: 'u1' }] }, { userId: [['u1']] }],
  [{ 'v.x': 1 }, { v: [[{ x: 1 }]] }],
  [{ 'v.x': { $ne: 1 } }, { v: [[{ x: 1 }]] }],
  [{ 'v.x': 1 }, { v: [{ x: [1] }] }],
  [{ 'v.x': 1 }, { v: [{ x: [[1]] }] }],
  [{ 'v.x.y': 1 }, { v: [{ x: [{ y: 1 }] }] }],
  [{ 'v.x.y': 1 }, { v: [{ x: [[{ y: 1 }]] }] }],
  [{ 'v.x': { $gt: 1 } }, { v: [{ x: [5] }, { x: [0] }] }],
  [{ 'v.x': { $not: { $gt: 1 } } }, { v: [{ x: [5] }, { x: [0] }] }],
  [{ 'v.x': { $not: { $gt: 1 } } }, { v: [{ x: [[5]] }] }],
  [{ 'v.x': [1] }, { v: [{ x: [[1]] }] }, 'server'],
  [{ v: { $size: 1 } }, { v: [[1, 2]] }],
  [{ v: { $exists: true } }, { v: null }],
  [{ 'v.x': { $size: 2 } }, { v: [{ x: 1 }, { x: 5 }] }, 'server'],
  [{ 'v.x': { $exists: true } }, { v: [[]] }, 'server'],
  [{ 'tags.0': { $ne: 'x' } }, { tags: [{ 0: 'x' }] }, 'server'],
  // $in / $nin
  [{ sessionId: { $in: [X, Y] } }, { sessionId: Y }],
  [{ sessionId: { $in: [X, Y] } }, { sessionId: Y.toHexString() }],
  [{ v: { $in: [null] } }, {}],
  [{ v: { $in: [null] } }, { v: [] }],
  [{ v: { $in: [null] } }, { v: [null] }],
  [{ tags: { $in: ['a'] } }, { tags: ['b', 'a'] }],
  [{ tags: { $in: ['a'] } }, { tags: [['a']] }],
  [{ v: { $in: [1, 'a', true] } }, { v: Long.fromNumber(1) }],
  [{ v: { $in: [1] } }, { v: Decimal128.fromString('1.00') }],
  [{ v: { $in: [1] } }, { v: true }],
  [{ at: { $in: [D1] } }, { at: new Date(D1) }],
  [{ v: { $nin: [1, 'a'] } }, { v: 2 }],
  [{ v: { $nin: [1, 'a'] } }, { v: [2, 'a'] }],
  [{ v: { $nin: [null] } }, {}],
  [{ 'recipients.pending': { $in: ['me@x'] } }, { recipients: { pending: ['me@x'] } }],
  [{ 'recipients.pending': { $in: ['me@x'] } }, { recipients: [{ pending: ['b'] }, { pending: ['me@x'] }] }],
  [{ 'v.x': { $in: [1] } }, { v: [[{ x: 1 }]] }],
  [{ $and: [{ sessionId: X }, { sessionId: { $in: [Y, X] } }] }, { sessionId: X }],
  [{ $and: [{ sessionId: X }, { sessionId: { $in: [Y, X] } }] }, { sessionId: Y }],
  [{ $and: [{ sessionId: X }, { sessionId: { $in: [Y] } }] }, { sessionId: X }],
  [{ $and: [{ sessionId: X }, { sessionId: { $in: [Y] } }] }, { sessionId: [X, Y] }],
  [{ sessionId: X, $and: [{ sessionId: { $in: [X] } }] }, { sessionId: [Y, X] }],
  [{ $and: [{ 'a.b': 1 }, { 'a.b': { $in: [2, 1] } }] }, { a: [{ b: 2 }, { b: 1 }] }],
  [{ $and: [{ at: D1 }, { at: { $in: [D1] } }] }, { at: new Date(D1) }],
  [{ $and: [{ sessionId: X }, { sessionId: { $in: [X.toHexString()] } }] }, { sessionId: X }],
  [{ $and: [{ tag: 'a' }, { tag: { $in: ['b', 'a'] } }] }, { tag: ['a'] }],
  [{ $and: [{ v: 1 }, { v: { $in: [new Int32(1)] } }] }, { v: 1.0 }],
  [
    { $or: [{ 'recipients.pending': { $in: ['me@x'] } }, { $and: [{ type: 'project' }, { documentId: { $in: ['p1'] } }] }] },
    { type: 'project', documentId: 'p1' },
  ],
  [
    { $or: [{ userId: 'u' }, { sharedWith: 'u' }, { visibility: 'public' }, { notebookId: { $in: [X] } }] },
    { sharedWith: ['x', 'u'] },
  ],
  // $all
  [{ userId: { $all: ['u1'] } }, { userId: 'u1' }],
  [{ v: { $all: [null] } }, {}],
  [{ v: { $all: [null] } }, { v: null }],
  [{ tags: { $all: ['x', 'y'] } }, { tags: ['y', 'x', 'z'] }],
  [{ tags: { $all: ['x', 'y'] } }, { tags: ['x'] }],
  [{ v: { $all: [] } }, { v: [1] }],
  [{ v: { $not: { $all: [] } } }, { v: [1] }],
  [{ v: { $not: { $all: ['a', 'b'] } } }, { v: ['a'] }],
  [{ tags: { $all: [{ $elemMatch: { $gt: 1 } }] } }, { tags: [0, 2] }, 'server'],
  // NaN, Long and Decimal128
  [{ v: { $gte: 1 } }, { v: NaN }],
  [{ v: { $lte: 1 } }, { v: NaN }],
  [{ v: { $lt: 5 } }, { v: [NaN, 6] }],
  [{ v: { $not: { $gt: 5 } } }, { v: NaN }],
  [{ v: { $ne: 5 } }, { v: NaN }],
  [{ v: 9007199254740992 }, { v: BIG }],
  [{ v: { $ne: 9007199254740992 } }, { v: BIG }],
  [{ v: { $gt: 9007199254740992 } }, { v: BIG }, 'fails-closed'],
  [{ v: { $not: { $gt: 9007199254740992 } } }, { v: BIG }],
  [{ $nor: [{ v: { $lt: 1 } }] }, { v: Decimal128.fromString('0.1') }],
  [{ v: 1152921504606846976 }, { v: Long.fromString('1152921504606846976') }],
  [{ v: 0.1 }, { v: Decimal128.fromString('0.1') }],
  [{ v: 2.25 }, { v: Decimal128.fromString('2.25') }],
  [{ v: { $gt: 2 } }, { v: Decimal128.fromString('2.25') }],
  [{ v: { $lt: 1 } }, { v: Decimal128.fromString('0.1') }, 'fails-closed'],
  [{ v: { $ne: 0.1 } }, { v: Decimal128.fromString('0.1') }],
  // Filters Mongo rejects
  [{ v: { $not: 5 } }, { v: 4 }, 'invalid'],
  [{ v: { $not: {} } }, { v: 4 }, 'invalid'],
  [{ v: { $not: { $gt: 1, a: 1 } } }, { v: 4 }, 'invalid'],
  [{ $and: [] }, { v: 4 }, 'invalid'],
  [{ $nor: [] }, { v: 4 }, 'invalid'],
  [{ $or: {} }, { v: 4 }, 'invalid'],
  [{ $and: [5] }, { v: 4 }, 'invalid'],
  [{ v: { $size: -1 } }, { v: [] }, 'invalid'],
  [{ v: { $size: 1.5 } }, { v: [1] }, 'invalid'],
  [{ v: { $not: { $size: '1' } } }, { v: [1] }, 'invalid'],
  [{ $and: [{}] }, { v: 4 }],
  // Operators left to the server
  [{ n: { $mod: [2, 1] } }, { n: 3 }, 'server'],
  [{ v: { $not: /x/ } }, { v: 'y' }, 'server'],
  [{ v: { $not: { $regex: 'x' } } }, { v: 'y' }, 'server'],
  [{ v: { $exists: '' } }, { v: 1 }, 'server'],
  [{ v: { $in: [Infinity] } }, { v: null }],
  [{ v: { $nin: [Infinity] } }, { v: Infinity }],
  [{ v: { $size: 2147483647 } }, { v: [1] }],
  [{ v: { $size: 2147483648 } }, { v: [1] }, 'invalid'],
  [{ v: { $not: { $size: 2147483648 } } }, { v: [1] }, 'invalid'],
  [{ v: { $not: { $size: 4294967297 } } }, { v: [1] }, 'invalid'],
  [{ v: { $not: { $size: 1e20 } } }, { v: [1] }, 'invalid'],
  [{ v: Infinity }, { v: Decimal128.fromString('Infinity') }],
  [{ v: { $ne: Infinity } }, { v: Decimal128.fromString('Infinity') }],
  [{ v: { $nin: [-Infinity] } }, { v: Decimal128.fromString('-Infinity') }],
  [{ v: { $gt: 1e308 } }, { v: Decimal128.fromString('Infinity') }],
  [{ 'v.kind': { $ne: 'admin' } }, { v: { $ref: 'users', $id: X, kind: 'admin' } }],
  [{ 'v.kind': 'admin' }, { v: { $ref: 'users', $id: X, $db: 'other', kind: 'admin' } }],
  [{ v: { $elemMatch: { kind: 'admin' } } }, { v: [{ $ref: 'users', $id: X, kind: 'admin' }] }],
  [{ $nor: [{ status: 'x' }] }, { status: 'y', price: Decimal128.fromString('19.99') }],
  [{ status: { $not: { $in: ['x'] } } }, { status: 'y', price: Decimal128.fromString('19.99') }],
  [{ $nor: [{ price: 5 }] }, { status: 'y', price: Decimal128.fromString('19.99') }, 'fails-closed'],
  [{}, LOOKUP_GONE],
  [{ userId: X }, LOOKUP_GONE],
  [{ deletedAt: null }, LOOKUP_GONE],
  [{ v: { $ne: 1 } }, LOOKUP_GONE],
];

const projectionCases: Array<[Doc, Doc, Route?]> = [
  [{ 'a.b': false }, { a: [{ b: 1, c: 2 }, 5, [{ b: 1, c: 2 }, [{ b: 1 }]], { b: { b: 1 } }] }],
  [{ 'a.b': true }, { a: [{ b: 1, c: 2 }, 5, [{ b: 1, c: 2 }, 7, [{ b: 1 }]], { c: 1 }], z: 1 }],
  [{ 'a.b': 1 }, { a: 5 }],
  [{ 'a.b': 1 }, { a: { c: 1 } }],
  [{ 'a.b': 1 }, { a: [5, [6]] }],
  [{ 'a.b.c': 1 }, { a: [{ b: [{ c: 1, d: 1 }, 3] }] }],
  [{ 'a.b.c': 0 }, { a: [{ b: [{ c: 1, d: 1 }, [{ c: 1 }]] }] }],
  [{ 'a.b': 1, 'a.c': 1 }, { a: { b: 1, c: 2, d: 3 } }],
  [{ _id: 0, a: 1 }, { a: 1, b: 2 }],
  [{ a: 0, 'b.c': 1 }, { a: 1, b: { c: 1, d: 1 }, e: 1 }],
  [{ mfa: { totpSecret: 0 } }, { mfa: { totpSecret: 's', x: 1 } }],
  [
    { 'securityQuestions.answer': false, 'mfa.totpSecret': false },
    { securityQuestions: [{ question: 'q', answer: 'a' }], mfa: { totpSecret: 's', enabled: true } },
  ],
  [{ a: 2, b: -1 }, { a: 1, b: 2, c: 3 }],
  [{ 'v.0': 0 }, { v: [1, 2] }, 'server'],
  [{ 'v.0': 1 }, { v: { 0: 1, 1: 2 } }, 'server'],
  [{ a: 1, 'a.b': 1 }, { a: { b: 1 } }, 'server'],
  [{ 'v.kind': 0 }, { v: { $ref: 'users', $id: X, kind: 'admin', n: 1 } }],
  [{ 'v.kind': 1 }, { v: { $ref: 'users', $id: X, $db: 'other', kind: 'admin' } }],
];

let server: MongoMemoryServer;
let client: mongoose.mongo.MongoClient;

before(async () => {
  server = await MongoMemoryServer.create();
  client = new MongoClient(server.getUri());
  await client.connect();
});

after(async () => {
  await client.close();
  await server.stop();
});

describe('differential against the server', () => {
  it('matches each filter case the way $match did on the old change stream', async () => {
    const events = client.db('differential').collection<{ _id: number } & Doc>('events');
    await events.insertMany(
      filterCases.map(([, doc], _id) =>
        doc === DELETE
          ? { _id, operationType: 'delete' }
          : doc === LOOKUP_GONE
            ? { _id, operationType: 'update', fullDocument: null }
            : { _id, operationType: 'insert', fullDocument: doc }
      )
    );
    const mismatches: string[] = [];
    for (const [_id, [query, , route = 'in-process']] of filterCases.entries()) {
      const remote = await events
        .countDocuments({ $and: [{ _id }, transformAndPrefixQuery(query, 'fullDocument')] })
        .then(count => count === 1, () => 'error' as const);
      let local: boolean | 'server' | 'invalid';
      try {
        // As the driver delivers it, so Longs and Decimals arrive the way they would live.
        local = buildMatcher(query)(normalizeBson(await events.findOne({ _id })));
      } catch (error) {
        local = error instanceof InvalidFilterError ? 'invalid' : 'server';
      }
      const agrees =
        route === 'in-process'
          ? local === remote
          : route === 'invalid'
            ? local === 'invalid' && remote === 'error'
            : route === 'fails-closed'
              ? local === false
              : local === 'server';
      if (!agrees) mismatches.push(`#${_id} ${JSON.stringify(query)} ${route}: local ${local}, server ${remote}`);
    }
    assert.deepEqual(mismatches, []);
  });

  it('projects each case the way the old $project stages did', async () => {
    const docs = client.db('differential').collection<{ _id: number } & Doc>('projected');
    await docs.insertMany(projectionCases.map(([, doc], _id) => ({ ...doc, _id })));
    const mismatches: string[] = [];
    for (const [_id, [fields, , route = 'in-process']] of projectionCases.entries()) {
      const flat: Record<string, unknown> = flatten(fields);
      const stages = [pickBy(flat, v => !v), pickBy(flat, v => !!v)]
        .filter(stage => Object.keys(stage).length > 0)
        .map(stage => ({ $project: stage }));
      const remote = await docs
        .aggregate([{ $match: { _id } }, ...stages])
        .toArray()
        .then(([projected]) => projected, () => 'error');
      let local: unknown;
      try {
        local = buildProjector(fields)((await docs.findOne({ _id }))!);
      } catch {
        local = 'server';
      }
      // A DBRef and the plain document it stands for serialise the same.
      const sameBson = () => Buffer.from(BSON.serialize(local as Doc)).equals(BSON.serialize(remote as Doc));
      const agrees = route === 'server' ? local === 'server' : isDeepStrictEqual(local, remote) || sameBson();
      if (!agrees) {
        mismatches.push(`#${_id} ${JSON.stringify(fields)} ${route}: local ${JSON.stringify(local)}, server ${JSON.stringify(remote)}`);
      }
    }
    assert.deepEqual(mismatches, []);
  });
});
