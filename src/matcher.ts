import mongoose, { AnyObject } from 'mongoose';
import { Aggregator, Query } from 'mingo';
import { flatten } from 'flat';
import { isPlainObject, pickBy } from 'lodash-es';

export type Matcher = (normalizedChange: unknown) => boolean;
export type Projector = (fullDocument: AnyObject) => AnyObject;

const LOGICAL_OPERATORS = new Set(['$and', '$or', '$nor']);

// Only top-level field names (and those inside $and/$or/$nor) are paths into the
// document. Anything under a field ($elemMatch, embedded docs) or under another
// operator is relative to that value and must stay unprefixed.
export function transformAndPrefixQuery(query: Record<string, unknown>, prefix: string): Record<string, unknown> {
  const transformObject = (obj: Record<string, unknown>): Record<string, unknown> => {
    const transformed: Record<string, unknown> = {};
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        if (key.startsWith('$')) {
          transformed[key] =
            LOGICAL_OPERATORS.has(key) && Array.isArray(obj[key])
              ? (obj[key] as Array<unknown>).map(item =>
                  isPlainObject(item) ? transformObject(item as Record<string, unknown>) : item
                )
              : obj[key];
        } else if (key === '_id' && typeof obj[key] === 'string') {
          // Handle _id specially - can be string or ObjectId
          try {
            transformed[`${prefix}.${key}`] = new mongoose.Types.ObjectId(obj[key] as string);
          } catch (error) {
            transformed[`${prefix}.${key}`] = obj[key];
          }
        } else {
          transformed[`${prefix}.${key}`] = obj[key];
        }
      }
    }
    return transformed;
  };

  return transformObject(query);
}

function isPlainContainer(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

// A '__proto__' key must land as an own property, never as prototype assignment.
function setOwn(target: Record<string, unknown>, key: string, value: unknown) {
  Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true });
}

// mingo compares by value on plain JS types only, so BSON wrappers have to be
// unwrapped on both the query and the event for equality to behave like Mongo.
// Strings and ObjectIds get distinct prefixes so no document value can collide
// with an ObjectId operand. Both end up as JS strings, so ordering comparisons
// against them are left to the server (see assertCondition).
export function normalizeBson(value: unknown): unknown {
  if (typeof value === 'string') return `s:${value}`;
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(normalizeBson);
  if (value instanceof Date || value instanceof RegExp || ArrayBuffer.isView(value)) return value;

  const bson = value as { _bsontype?: string; toHexString?: () => string; toNumber?: () => number };
  switch (bson._bsontype) {
    case undefined:
      break;
    case 'ObjectId':
    case 'ObjectID':
      return `o:${bson.toHexString!()}`;
    case 'Long':
      return bson.toNumber!();
    case 'Int32':
    case 'Double':
      return Number(value.valueOf());
    case 'Decimal128':
      return Number(value.toString());
    default:
      return value;
  }

  if (!isPlainContainer(value)) return value;

  const normalized: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value)) {
    // $type names a BSON type, it is not a value to compare against.
    setOwn(normalized, key, key === '$type' ? inner : normalizeBson(inner));
  }
  return normalized;
}

// Copies the plain objects and arrays mingo's $project edits in place; BSON
// values, Dates and Buffers are shared since projection never changes them.
function clonePlain(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(clonePlain);
  if (!isPlainContainer(value)) return value;
  const copy: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value)) {
    setOwn(copy, key, clonePlain(inner));
  }
  return copy;
}

// Only operators whose mingo semantics match Mongo's for the operands allowed
// below are evaluated in process; everything else ($regex, $where, $expr, $type,
// geo, bitwise, $comment, anything unknown) keeps a dedicated server-side stream.
// An allowlist, because a new or rare operator should fall back, not mismatch.
const IN_PROCESS_OPERATORS = new Set([
  '$eq',
  '$ne',
  '$gt',
  '$gte',
  '$lt',
  '$lte',
  '$in',
  '$nin',
  '$exists',
  '$size',
  '$all',
  '$elemMatch',
  '$not',
  '$mod',
]);
const RANGE_OPERATORS = new Set(['$gt', '$gte', '$lt', '$lte']);

function serverSide(reason: string, path: string): never {
  throw new Error(`${reason} at ${path || '<root>'} is matched server-side`);
}

function bsonType(value: unknown): string | undefined {
  return value !== null && typeof value === 'object' ? (value as { _bsontype?: string })._bsontype : undefined;
}

function isNumberLike(value: unknown) {
  const type = bsonType(value);
  return typeof value === 'number' || type === 'Int32' || type === 'Double';
}

function isOperatorObject(value: unknown): value is Record<string, unknown> {
  if (!isPlainContainer(value)) return false;
  const keys = Object.keys(value);
  return keys.length > 0 && keys.every(key => key.startsWith('$'));
}

// A value compared for equality. Mongo compares embedded documents field-order
// sensitively and Long/Decimal128 exactly, and mingo does neither; normalised
// ObjectIds and strings are both JS strings, so only equality is safe for them.
function assertOperand(value: unknown, path: string, underDottedPath: boolean): void {
  if (value instanceof RegExp) serverSide('regular expression', path);
  // On a dotted path through an array of documents, Mongo matches null when any
  // element lacks the field; mingo does not.
  if (value === null && underDottedPath) serverSide('null under a dotted path', path);
  if (Array.isArray(value)) {
    value.forEach((item, i) => assertOperand(item, `${path}[${i}]`, underDottedPath));
    return;
  }
  if (isPlainContainer(value)) serverSide('embedded document equality', path);
  const type = bsonType(value);
  if (type !== undefined && type !== 'ObjectId' && type !== 'ObjectID' && type !== 'Int32' && type !== 'Double') {
    serverSide(`${type} operand`, path);
  }
}

function assertCondition(condition: unknown, path: string, underDottedPath: boolean): void {
  if (!isOperatorObject(condition)) {
    assertOperand(condition, path, underDottedPath);
    return;
  }
  for (const [operator, operand] of Object.entries(condition)) {
    if (!IN_PROCESS_OPERATORS.has(operator)) serverSide(operator, path);
    switch (operator) {
      case '$not':
        assertCondition(operand, path, underDottedPath);
        break;
      case '$elemMatch':
        if (isOperatorObject(operand)) assertCondition(operand, path, underDottedPath);
        else if (isPlainContainer(operand)) assertFilter(operand, path, underDottedPath);
        else serverSide('$elemMatch operand', path);
        break;
      case '$in':
      case '$nin':
      case '$all':
        if (!Array.isArray(operand)) serverSide(`${operator} operand`, path);
        operand.forEach((item, i) => {
          if (operator === '$all' && isOperatorObject(item)) assertCondition(item, path, underDottedPath);
          // An array element is whole-array equality in Mongo, element-wise in mingo.
          else if (Array.isArray(item)) serverSide(`array inside ${operator}`, `${path}[${i}]`);
          else assertOperand(item, `${path}[${i}]`, underDottedPath);
        });
        break;
      case '$exists':
        break;
      default:
        // Mongo brackets ordering by BSON type and orders strings by UTF-8 bytes;
        // mingo does neither, so only numbers and Dates are ordered in process.
        if (RANGE_OPERATORS.has(operator) && !isNumberLike(operand) && !(operand instanceof Date)) {
          serverSide(`${operator} on a non-number, non-Date operand`, path);
        }
        assertOperand(operand, path, underDottedPath);
    }
  }
}

function assertFilter(filter: Record<string, unknown>, basePath: string, underDottedPath: boolean): void {
  for (const [key, value] of Object.entries(filter)) {
    const path = basePath ? `${basePath}.${key}` : key;
    if (key === '$and' || key === '$or' || key === '$nor') {
      if (!Array.isArray(value)) serverSide(`${key} operand`, path);
      value.forEach(item => {
        if (!isPlainContainer(item)) serverSide(`${key} operand`, path);
        assertFilter(item, basePath, underDottedPath);
      });
    } else if (key.startsWith('$')) {
      serverSide(key, path);
    } else {
      assertCondition(value, path, underDottedPath || key.includes('.'));
    }
  }
}

function assertEvaluableInProcess(query: Record<string, unknown>): void {
  assertFilter(query, '', false);
}

export function buildMatcher(query: Record<string, unknown>): Matcher {
  assertEvaluableInProcess(query ?? {});
  const compiled = new Query(normalizeBson(transformAndPrefixQuery(query ?? {}, 'fullDocument')) as AnyObject);
  return normalizedChange => compiled.test(normalizedChange as AnyObject);
}

// Mirrors the old change stream pipeline: exclusions in one $project, then
// inclusions in a second, both applied to fullDocument only.
export function buildProjector(fields: Record<string, unknown> | undefined): Projector {
  if (Object.keys(fields ?? {}).length === 0) return doc => doc;

  const flat: Record<string, number | boolean> = flatten(fields);
  const stages: AnyObject[] = [];
  const exclusions = pickBy(flat, v => !v);
  if (Object.keys(exclusions).length > 0) stages.push({ $project: exclusions });
  const inclusions = pickBy(flat, v => !!v);
  if (Object.keys(inclusions).length > 0) stages.push({ $project: inclusions });

  const aggregator = new Aggregator(stages);
  // The event is shared by every subscription on the stream, so project a copy.
  const project: Projector = doc => aggregator.run([clonePlain(doc)])[0] as AnyObject;
  // Aggregator compiles lazily, so surface a bad projection now rather than per event.
  project({});
  return project;
}

export type SubscriptionFilter = { matcher: Matcher; projector: Projector };

export function buildSubscriptionFilter(subscription: {
  query: Record<string, unknown>;
  fields?: Record<string, unknown>;
}): SubscriptionFilter {
  return { matcher: buildMatcher(subscription.query), projector: buildProjector(subscription.fields) };
}
