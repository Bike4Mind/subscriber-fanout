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
// ObjectIds keep a tag so a plain string never compares equal to one.
export function normalizeBson(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(normalizeBson);
  if (value instanceof Date || value instanceof RegExp || ArrayBuffer.isView(value)) return value;

  const bson = value as { _bsontype?: string; toHexString?: () => string; toNumber?: () => number };
  switch (bson._bsontype) {
    case undefined:
      break;
    case 'ObjectId':
    case 'ObjectID':
      return `ObjectId(${bson.toHexString!()})`;
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
    setOwn(normalized, key, normalizeBson(inner));
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

// Server-side JS and regular expressions stay on the server: a regex here would
// run on the event loop for every change, and $where would execute in process.
const SERVER_ONLY_OPERATORS = new Set(['$regex', '$options', '$where', '$expr', '$function', '$jsonSchema', '$text']);

function assertEvaluableInProcess(value: unknown, path = ''): void {
  if (value instanceof RegExp) throw new Error(`regular expression at ${path || '<root>'} is matched server-side`);
  if (Array.isArray(value)) {
    value.forEach((item, i) => assertEvaluableInProcess(item, `${path}[${i}]`));
    return;
  }
  if (!isPlainContainer(value)) return;
  for (const [key, inner] of Object.entries(value)) {
    if (SERVER_ONLY_OPERATORS.has(key)) throw new Error(`${key} at ${path || '<root>'} is matched server-side`);
    assertEvaluableInProcess(inner, path ? `${path}.${key}` : key);
  }
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
