import { createHash } from 'node:crypto';
import mongoose, { AnyObject } from 'mongoose';
import { Context, Query } from 'mingo';
import { flatten } from 'flat';
import { isPlainObject } from 'lodash-es';

export type Matcher = (normalizedChange: unknown) => boolean;
export type Projector = (fullDocument: AnyObject) => AnyObject;

const LOGICAL_OPERATORS = new Set(['$and', '$or', '$nor']);

// Only top-level field names (and those inside $and/$or/$nor) are paths into the
// document. Anything under a field ($elemMatch, embedded docs) or under another
// operator is relative to that value and must stay unprefixed.
export function transformAndPrefixQuery(query: Record<string, unknown>, prefix: string): Record<string, unknown> {
  const prefixed = (key: string) => (prefix ? `${prefix}.${key}` : key);
  const transformObject = (obj: Record<string, unknown>): Record<string, unknown> => {
    const transformed: Record<string, unknown> = {};
    for (const key in obj) {
      if (Object.hasOwn(obj, key)) {
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
            transformed[prefixed(key)] = new mongoose.Types.ObjectId(obj[key] as string);
          } catch (error) {
            transformed[prefixed(key)] = obj[key];
          }
        } else {
          transformed[prefixed(key)] = obj[key];
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

// Stands in for a Long or Decimal128 with no exact double value, which neither
// equals nor orders against any JS number operand.
class Inexact {}
const INEXACT = new Inexact();

// Whether the Decimal128 string denotes exactly the double n.
function decimalEquals(text: string, n: number): boolean {
  const parts = /^(-?\d+)(?:\.(\d+))?(?:E([+-]\d+))?$/.exec(text);
  if (!parts || !Number.isFinite(n)) return false;
  let scaled = n;
  let binaryDigits = 0n;
  while (!Number.isInteger(scaled)) {
    scaled *= 2;
    binaryDigits++;
  }
  const fraction = parts[2] ?? '';
  const exponent = BigInt(Number(parts[3] ?? 0) - fraction.length);
  const decimal = BigInt(parts[1] + fraction) * 2n ** binaryDigits;
  const binary = BigInt(scaled);
  return exponent >= 0n ? decimal * 10n ** exponent === binary : decimal === binary * 10n ** -exponent;
}

// The driver reads a subdocument led by $ref and $id as a DBRef; to Mongo it is a document.
function asDocument(value: unknown): unknown {
  if (bsonType(value) !== 'DBRef') return value;
  const ref = value as mongoose.mongo.DBRef;
  return { $ref: ref.collection, $id: ref.oid, ...(ref.db != null ? { $db: ref.db } : {}), ...ref.fields };
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
    case 'Long': {
      const n = bson.toNumber!();
      return BigInt(n).toString() === String(value) ? n : INEXACT;
    }
    case 'Int32':
    case 'Double':
      return Number(value.valueOf());
    case 'Decimal128': {
      const text = String(value);
      const n = Number(text);
      return text === 'Infinity' || text === '-Infinity' || decimalEquals(text, n) ? n : INEXACT;
    }
    case 'DBRef':
      return normalizeBson(asDocument(value));
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

function containsInexact(value: unknown): boolean {
  if (value === INEXACT) return true;
  if (Array.isArray(value)) return value.some(containsInexact);
  return isPlainContainer(value) && Object.values(value).some(containsInexact);
}

function hasNegation(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(hasNegation);
  if (!isPlainContainer(value)) return false;
  return Object.entries(value).some(([key, inner]) => key === '$not' || key === '$nor' || hasNegation(inner));
}

// The top-level fields a $not or $nor reads.
function negatedFields(filter: Record<string, unknown>, negated = false, fields = new Set<string>()): Set<string> {
  for (const [key, value] of Object.entries(filter)) {
    if (LOGICAL_OPERATORS.has(key)) {
      (value as Record<string, unknown>[]).forEach(item => negatedFields(item, negated || key === '$nor', fields));
    } else if (negated || hasNegation(value)) {
      fields.add(key.split('.')[0]);
    }
  }
  return fields;
}

// Operators whose mingo semantics match Mongo's for the operands allowed below; an allowlist, so any other
// operator falls back to a dedicated server-side stream instead of mismatching.
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
]);
const RANGE_OPERATORS = new Set(['$gt', '$gte', '$lt', '$lte']);

// A filter Mongo would reject, which matches nothing.
export class InvalidFilterError extends Error {}

function invalid(reason: string, path: string): never {
  throw new InvalidFilterError(`${reason} at ${path || '<root>'}`);
}

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

// To Mongo a numeric segment is both an array index and a field name, and mingo
// only treats it as an index; mingo also refuses __proto__ when it resolves a path.
function isPlainSegment(segment: string) {
  return segment !== '' && !segment.startsWith('$') && segment !== '__proto__' && !/^\d+$/.test(segment);
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
    // mingo also tests the array against nested arrays a dotted path reaches.
    if (underDottedPath) serverSide('array equality under a dotted path', path);
    value.forEach((item, i) => assertOperand(item, `${path}[${i}]`, underDottedPath));
    return;
  }
  if (isPlainContainer(value)) serverSide('embedded document equality', path);
  const type = bsonType(value);
  if (type !== undefined && type !== 'ObjectId' && type !== 'ObjectID' && type !== 'Int32' && type !== 'Double') {
    serverSide(`${type} operand`, path);
  }
  if (isNumberLike(value) && Number.isNaN(Number(value!.valueOf()))) serverSide('NaN operand', path);
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
        if (operand instanceof RegExp) serverSide('regular expression', path);
        if (!isOperatorObject(operand)) invalid('$not needs a regex or an operator document', path);
        assertCondition(operand, path, underDottedPath);
        break;
      case '$elemMatch':
        // Applied to nested arrays, mingo's operator form compares their elements too.
        if (isOperatorObject(operand) && !Object.keys(operand).some(key => LOGICAL_OPERATORS.has(key))) {
          serverSide('$elemMatch on values', path);
        }
        // On a dotted path, mingo hands $elemMatch every array the path reaches as one.
        if (underDottedPath) serverSide('$elemMatch on a dotted path', path);
        if (!isPlainContainer(operand)) serverSide('$elemMatch operand', path);
        assertFilter(operand, path, underDottedPath);
        break;
      case '$in':
      case '$nin':
      case '$all':
        if (!Array.isArray(operand)) serverSide(`${operator} operand`, path);
        operand.forEach((item, i) => {
          if (operator === '$all' && isOperatorObject(item)) serverSide('operator inside $all', `${path}[${i}]`);
          // An array element is whole-array equality in Mongo, element-wise in mingo.
          else if (Array.isArray(item)) serverSide(`array inside ${operator}`, `${path}[${i}]`);
          else assertOperand(item, `${path}[${i}]`, underDottedPath);
        });
        break;
      case '$exists':
        // Through arrays, mingo counts a path that only reaches empty arrays as present.
        if (underDottedPath) serverSide('$exists on a dotted path', path);
        // Mongo reads the operand by BSON truthiness, which differs from JS for '' and wrappers.
        if (typeof operand !== 'boolean' && !(typeof operand === 'number' && !Number.isNaN(operand))) {
          serverSide('$exists operand', path);
        }
        break;
      case '$size':
        // mingo counts the values a dotted path collects as an array.
        if (underDottedPath) serverSide('$size on a dotted path', path);
        assertOperand(operand, path, underDottedPath);
        const size = Number(operand!.valueOf());
        if (!isNumberLike(operand) || !Number.isInteger(size) || size < 0 || size > 2147483647) {
          invalid('$size needs a non-negative 32-bit integer', path);
        }
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
      if (!Array.isArray(value) || value.length === 0) invalid(`${key} needs a non-empty array`, path);
      value.forEach(item => {
        if (!isPlainContainer(item)) invalid(`${key} entries need to be documents`, path);
        assertFilter(item, basePath, underDottedPath);
      });
    } else if (key.startsWith('$')) {
      serverSide(key, path);
    } else {
      if (!key.split('.').every(isPlainSegment)) serverSide('numeric or reserved path segment', path);
      assertCondition(value, path, underDottedPath || key.includes('.'));
    }
  }
}

function assertEvaluableInProcess(query: Record<string, unknown>): void {
  assertFilter(query, '', false);
}

type Test = (document: AnyObject) => boolean;

// The values Mongo compares an operand with: what the path reaches through arrays
// of documents, plus the elements of an array it ends on.
function pathValues(value: unknown, segments: string[], out: unknown[] = []): unknown[] {
  if (segments.length === 0) {
    if (Array.isArray(value)) out.push(...value);
    else out.push(value);
  } else if (Array.isArray(value)) {
    value.forEach(item => isPlainContainer(item) && pathValues(item, segments, out));
  } else if (isPlainContainer(value) && Object.hasOwn(value, segments[0])) {
    pathValues(value[segments[0]], segments.slice(1), out);
  } else {
    out.push(undefined);
  }
  return out;
}

function anyValue(selector: string, test: (value: unknown) => boolean): Test {
  const segments = selector.split('.');
  return document => pathValues(document, segments).some(test);
}

const negate =
  (test: Test): Test =>
  document =>
    !test(document);

function memberKey(value: unknown) {
  return value instanceof Date ? `d:${value.getTime()}` : (value ?? null);
}

// Keys are normalised, so strings, ObjectIds, numbers and Dates cannot collide.
function membership(selector: string, members: Set<unknown>): Test {
  return anyValue(selector, value => members.has(memberKey(value)));
}

// Subscriptions often carry the same scope list, so equal $in lists share one Set.
const memberSets = new Map<string, WeakRef<Set<unknown>>>();
const forgetMembers = new FinalizationRegistry<string>(key => {
  if (!memberSets.get(key)?.deref()) memberSets.delete(key);
});

function memberSet(operand: unknown[]): Set<unknown> {
  const members = new Set(operand.map(memberKey));
  // Typed, because JSON.stringify writes null for Infinity and NaN.
  const typed = [...members].map(member => `${typeof member}:${String(member)}`).sort();
  const key = createHash('sha256').update(JSON.stringify(typed)).digest('base64');
  const shared = memberSets.get(key)?.deref();
  if (shared) return shared;
  memberSets.set(key, new WeakRef(members));
  forgetMembers.register(members, key);
  return members;
}

// Mongo orders numbers only against numbers and Dates only against Dates.
function ordered(compare: (value: number, bound: number) => boolean) {
  return (selector: string, operand: unknown, _options: unknown): Test =>
    operand instanceof Date
      ? anyValue(selector, value => value instanceof Date && compare(value.getTime(), operand.getTime()))
      : anyValue(selector, value => typeof value === 'number' && compare(value, operand as number));
}

const OPERATORS = Context.init({
  query: {
    $mongoEq: (selector: string, operand: unknown, _options: unknown): Test =>
      membership(selector, new Set([memberKey(operand)])),
    $mongoNe: (selector: string, operand: unknown, _options: unknown): Test =>
      negate(membership(selector, new Set([memberKey(operand)]))),
    $mongoGt: ordered((value, bound) => value > bound),
    $mongoGte: ordered((value, bound) => value >= bound),
    $mongoLt: ordered((value, bound) => value < bound),
    $mongoLte: ordered((value, bound) => value <= bound),
    $mongoIn: (selector: string, members: unknown, _options: unknown): Test =>
      membership(selector, members as Set<unknown>),
    $mongoNin: (selector: string, members: unknown, _options: unknown): Test =>
      negate(membership(selector, members as Set<unknown>)),
    // Mongo defines $all as an $and of equality conditions on the field.
    $mongoAll: (selector: string, operand: unknown[], _options: unknown): Test => {
      const tests = operand.map(item => membership(selector, new Set([memberKey(item)])));
      return document => tests.length > 0 && tests.every(test => test(document));
    },
    // Mongo tests only embedded documents here, reading an array element as a
    // document keyed by index; mingo also tests scalars and descends into arrays.
    $mongoElemMatch: (selector: string, criteria: AnyObject, _options: unknown): Test => {
      const query = new Query(criteria, { context: OPERATORS });
      return document => {
        const value = Object.hasOwn(document, selector) ? document[selector] : undefined;
        return (
          Array.isArray(value) &&
          value.some(item => (isPlainContainer(item) || Array.isArray(item)) && query.test({ ...item }))
        );
      };
    },
  },
});

// mingo resolves its built-in operators ahead of custom ones with the same name.
const MONGO_OPERATORS = new Map([
  ['$eq', '$mongoEq'],
  ['$ne', '$mongoNe'],
  ['$gt', '$mongoGt'],
  ['$gte', '$mongoGte'],
  ['$lt', '$mongoLt'],
  ['$lte', '$mongoLte'],
  ['$in', '$mongoIn'],
  ['$nin', '$mongoNin'],
  ['$all', '$mongoAll'],
]);

// Whole-array equality and $exists, $size and $not stay with mingo.
function toMingoCondition(condition: unknown): Record<string, unknown> {
  if (!isOperatorObject(condition)) return Array.isArray(condition) ? { $eq: condition } : { $mongoEq: condition };
  const compiled: Record<string, unknown> = {};
  for (const [operator, operand] of Object.entries(condition)) {
    if (operator === '$not') compiled.$not = toMingoCondition(operand);
    else if (operator === '$elemMatch') compiled.$mongoElemMatch = toMingoFilter(operand as Record<string, unknown>);
    else if ((operator === '$eq' || operator === '$ne') && Array.isArray(operand)) compiled[operator] = operand;
    // mingo copies the filter but keeps a Set by reference, so the list itself is not retained.
    else if (operator === '$in' || operator === '$nin') {
      compiled[MONGO_OPERATORS.get(operator)!] = memberSet(operand as unknown[]);
    } else compiled[MONGO_OPERATORS.get(operator) ?? operator] = operand;
  }
  return compiled;
}

function toMingoFilter(filter: Record<string, unknown>): Record<string, unknown> {
  const compiled: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(filter)) {
    setOwn(
      compiled,
      key,
      LOGICAL_OPERATORS.has(key)
        ? (value as Record<string, unknown>[]).map(toMingoFilter)
        : toMingoCondition(value)
    );
  }
  return compiled;
}

const isObjectId = (value: unknown) => bsonType(value) === 'ObjectId' || bsonType(value) === 'ObjectID';

// On raw values: comparing ObjectId bytes is far cheaper than normalising them.
function sameMember(a: unknown, b: unknown): boolean {
  if (isObjectId(a) || isObjectId(b)) {
    return isObjectId(a) && isObjectId(b) && (a as mongoose.Types.ObjectId).equals(b as mongoose.Types.ObjectId);
  }
  return memberKey(normalizeBson(a)) === memberKey(normalizeBson(b));
}

// An $in beside an equality on the same path adds nothing when its list holds the
// equality value. Runs before normalisation, so a dropped list is read only that far.
function dropImpliedIn(filter: Record<string, unknown>): Record<string, unknown> {
  const conjuncts = [filter, ...(Array.isArray(filter.$and) ? (filter.$and as Record<string, unknown>[]) : [])];
  const equalities = new Map<string, unknown>();
  for (const conjunct of conjuncts) {
    for (const [path, condition] of Object.entries(conjunct)) {
      if (!path.startsWith('$') && condition !== null && !isOperatorObject(condition) && !Array.isArray(condition)) {
        equalities.set(path, condition);
      }
    }
  }
  const prune = (conjunct: Record<string, unknown>) =>
    Object.fromEntries(
      Object.entries(conjunct).filter(([path, condition]) => {
        if (!isOperatorObject(condition) || Object.keys(condition).length !== 1 || !equalities.has(path)) return true;
        return !(condition.$in as unknown[] | undefined)?.some(item => sameMember(item, equalities.get(path)));
      })
    );
  const pruned = prune(filter);
  if (Array.isArray(filter.$and)) pruned.$and = (filter.$and as Record<string, unknown>[]).map(prune);
  return pruned;
}

export function buildMatcher(query: Record<string, unknown>): Matcher {
  assertEvaluableInProcess(query ?? {});
  const normalized = normalizeBson(dropImpliedIn(transformAndPrefixQuery(query ?? {}, ''))) as Record<string, unknown>;
  const compiled = new Query(toMingoFilter(normalized), { context: OPERATORS });
  // A negation over a number with no exact JS value could match where Mongo would not.
  const negated = [...negatedFields(query ?? {})];
  return normalizedChange => {
    // Matched against fullDocument alone, which a delete event lacks.
    const fullDocument = (normalizedChange as { fullDocument?: AnyObject | null }).fullDocument ?? {};
    if (negated.some(field => Object.hasOwn(fullDocument, field) && containsInexact(fullDocument[field]))) return false;
    return compiled.test(fullDocument);
  };
}

type ProjectionTree = Map<string, ProjectionTree | true>;

function addProjectionPath(tree: ProjectionTree, path: string) {
  const segments = path.split('.');
  let node = tree;
  segments.forEach((segment, i) => {
    const next = node.get(segment);
    const last = i === segments.length - 1;
    if (next === true || (last && next)) serverSide('projection path collision', path);
    if (last) node.set(segment, true);
    else node = next ?? node.set(segment, new Map()).get(segment) as ProjectionTree;
  });
}

// Like Mongo, a projected path reaches into arrays at any depth.
function exclude(value: unknown, tree: ProjectionTree): unknown {
  if (Array.isArray(value)) return value.map(item => exclude(item, tree));
  const document = asDocument(value);
  if (!isPlainContainer(document)) return value;
  const projected: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(document)) {
    const node = tree.get(key);
    if (node !== true) setOwn(projected, key, node ? exclude(inner, node) : inner);
  }
  return projected;
}

// An inclusion drops the scalars it passes through on the way to its leaf.
function include(value: unknown, tree: ProjectionTree): unknown {
  if (Array.isArray(value)) return value.map(item => include(item, tree)).filter(item => item !== undefined);
  const document = asDocument(value);
  if (!isPlainContainer(document)) return undefined;
  const projected: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(document)) {
    const node = tree.get(key);
    const kept = node === true ? inner : node ? include(inner, node) : undefined;
    if (kept !== undefined) setOwn(projected, key, kept);
  }
  return projected;
}

// Applies the fields as a dedicated stream's pipeline does: exclusions in one $project, then
// inclusions in a second, both to fullDocument only.
export function buildProjector(fields: Record<string, unknown> | undefined): Projector {
  if (Object.keys(fields ?? {}).length === 0) return doc => doc;

  const exclusions: ProjectionTree = new Map();
  const inclusions: ProjectionTree = new Map();
  for (const [path, flag] of Object.entries(flatten(fields) as Record<string, unknown>)) {
    if (typeof flag !== 'boolean' && !(typeof flag === 'number' && Number.isFinite(flag))) {
      serverSide('projection value', path);
    }
    if (!path.split('.').every(isPlainSegment)) serverSide('numeric or reserved projection path segment', path);
    addProjectionPath(flag ? inclusions : exclusions, path);
  }
  if (inclusions.size > 0 && !inclusions.has('_id')) inclusions.set('_id', true);

  // The event is shared by every subscription on the stream, so both build new containers.
  return doc => {
    const excluded = exclusions.size > 0 ? exclude(doc, exclusions) : doc;
    return (inclusions.size > 0 ? include(excluded, inclusions) : excluded) as AnyObject;
  };
}

export type SubscriptionFilter = { matcher: Matcher; projector: Projector };

export const NO_MATCH_FILTER: SubscriptionFilter = { matcher: () => false, projector: doc => doc };

export function buildSubscriptionFilter(subscription: {
  query: Record<string, unknown>;
  fields?: Record<string, unknown>;
}): SubscriptionFilter {
  return { matcher: buildMatcher(subscription.query), projector: buildProjector(subscription.fields) };
}
