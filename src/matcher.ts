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

// mingo compares by value on plain JS types only, so BSON wrappers have to be
// unwrapped on both the query and the event for equality to behave like Mongo.
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
      return bson.toHexString!();
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

  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return value;

  const normalized: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value)) {
    normalized[key] = normalizeBson(inner);
  }
  return normalized;
}

export function buildMatcher(query: Record<string, unknown>): Matcher {
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
  const project: Projector = doc => aggregator.run([doc])[0] as AnyObject;
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
