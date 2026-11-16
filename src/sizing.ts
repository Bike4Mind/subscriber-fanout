// Estimates for one change event, from a single walk before normalizeBson:
// heap - the raw event plus its normalized copy;
// json - the serialized document, which bounds any projection's message body;
// bson - its BSON size, for batch sizing.
export type EventSize = { heap: number; json: number; bson: number };

// The normalized copy shares the event's strings and copies only its objects.
const STRING_HEAP = 1;
const VALUE_HEAP = 40;
const ENTRY_HEAP = 48;
const OBJECT_HEAP = 64;
// A Binary deserialized from a getMore reply is a view of the whole reply, which it keeps alive.
const PINNING_BACKING = 4096;

type Sized = { heap: number; json: number; bson: number };

function sizeString(value: string, size: Sized) {
  let utf8 = 0;
  let escapes = 0;
  let wide = false;
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c < 0x80) {
      utf8++;
      if (c < 0x20) escapes += 5;
      else if (c === 34 || c === 92) escapes++;
    } else if (c < 0x800) {
      utf8 += 2;
      if (c > 0xff) wide = true;
    } else {
      wide = true;
      // A surrogate pair is 4 bytes of UTF-8, 2 per code unit.
      utf8 += c >= 0xd800 && c <= 0xdfff ? 2 : 3;
    }
  }
  size.heap += STRING_HEAP * (16 + (wide ? 2 * value.length : value.length));
  size.json += utf8 + escapes + 2;
  size.bson += utf8 + 5;
}

function sizeValue(value: unknown, size: Sized) {
  if (typeof value === 'string') return sizeString(value, size);
  if (value === null || typeof value !== 'object') {
    size.heap += VALUE_HEAP;
    size.json += 8;
    size.bson += 8;
    return;
  }
  if (value instanceof Date) {
    size.heap += VALUE_HEAP;
    size.json += 26;
    size.bson += 8;
    return;
  }
  const bson = value as { _bsontype?: string; buffer?: Uint8Array };
  if (bson._bsontype === 'Binary' && bson.buffer) {
    const view = bson.buffer;
    if (view.buffer.byteLength > 2 * view.byteLength && view.buffer.byteLength > PINNING_BACKING) {
      const copy = Buffer.alloc(view.byteLength);
      copy.set(view);
      bson.buffer = copy;
    }
    size.heap += OBJECT_HEAP + view.byteLength;
    size.json += Math.ceil((view.byteLength * 4) / 3) + 4;
    size.bson += view.byteLength + 5;
    return;
  }
  if (bson._bsontype !== undefined) {
    size.heap += OBJECT_HEAP + VALUE_HEAP;
    size.json += 40;
    size.bson += 16;
    return;
  }
  size.heap += 2 * OBJECT_HEAP;
  size.json += 2;
  size.bson += 5;
  if (Array.isArray(value)) {
    for (const item of value) {
      size.heap += 2 * ENTRY_HEAP;
      size.json += 1;
      size.bson += 4;
      sizeValue(item, size);
    }
    return;
  }
  for (const key of Object.keys(value)) {
    size.heap += 2 * ENTRY_HEAP;
    size.json += key.length + 4;
    size.bson += key.length + 2;
    sizeValue((value as Record<string, unknown>)[key], size);
  }
}

// Also copies out any Binary that pins a larger buffer, so holding the event holds only what it needs.
export function sizeEvent(change: unknown): EventSize {
  const size = { heap: 0, json: 0, bson: 0 };
  sizeValue(change, size);
  return size;
}
