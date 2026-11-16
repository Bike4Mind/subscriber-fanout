import crypto from 'node:crypto';
import { IDataSubscriptionUpdateAction } from './wireTypes.js';

// API Gateway caps WebSocket frames at 128KB, so anything over 120KB goes out in chunks.
export const MAX_WS_PAYLOAD_BYTES = 120 * 1024;
export const CHUNK_DATA_SIZE = 60 * 1024;
// Measured per in-flight POST (ClientRequest, socket, SDK request and middleware state).
export const REQUEST_BYTES = 24 * 1024;
const SHORT_ESCAPES: Record<number, number> = { 8: 0x62, 9: 0x74, 10: 0x6e, 12: 0x66, 13: 0x72 };

// Node 24 hands out Buffers under 32KB as slices of a shared 64KB slab, which a held slice keeps alive whole.
function unpooled(text: string) {
  const out = Buffer.allocUnsafeSlow(Buffer.byteLength(text));
  out.write(text);
  return out;
}

type Update = Pick<IDataSubscriptionUpdateAction, 'collectionName' | 'operationType' | 'data'>;

// Everything after the subscriber's id, shared by every subscriber of one (event, projection).
export function messageTail({ collectionName, operationType, data }: Update) {
  return unpooled(JSON.stringify({ collectionName, operationType, data }).slice(1));
}

// With the tail, byte for byte what JSON.stringify gives for the whole message, an undefined id included.
export function messagePrefix(subscriptionId: string | undefined) {
  return unpooled(JSON.stringify({ action: 'data_update', subscriptionId }).slice(0, -1) + ',');
}

// The POSTs one send of a message takes. Chunks are cut from prefix and tail without joining them, at the
// same UTF-8 boundaries as the whole message would be, and share one _chunkId per send.
export class Frames {
  readonly count: number;
  private readonly bounds: number[] = [];
  private readonly chunkId?: string;
  private readonly lengths: number[] = [];

  constructor(
    private readonly prefix: Buffer,
    private readonly tail: Buffer,
    chunking = true
  ) {
    const total = prefix.length + tail.length;
    if (!chunking || total <= MAX_WS_PAYLOAD_BYTES) {
      this.count = 1;
      return;
    }
    this.chunkId = crypto.randomUUID();
    let offset = 0;
    this.bounds.push(0);
    while (offset < total) {
      let end = Math.min(offset + CHUNK_DATA_SIZE, total);
      while (end > offset && (this.byteAt(end) & 0xc0) === 0x80) end--;
      this.bounds.push(end);
      offset = end;
    }
    this.count = this.bounds.length - 1;
  }

  // Bytes the POST holds while it is in flight: its frame's exact length plus the request's objects.
  bytes(index: number) {
    return this.frameLength(index) + REQUEST_BYTES;
  }

  frame(index: number): Buffer {
    if (this.chunkId === undefined) {
      const whole = Buffer.allocUnsafeSlow(this.prefix.length + this.tail.length);
      this.prefix.copy(whole, 0);
      this.tail.copy(whole, this.prefix.length);
      return whole;
    }
    const head = this.head(index);
    const out = Buffer.allocUnsafeSlow(this.frameLength(index));
    head.copy(out, 0);
    let o = head.length;
    for (let i = this.bounds[index]; i < this.bounds[index + 1]; i++) {
      const b = this.byteAt(i);
      if (b === 0x22 || b === 0x5c) {
        out[o++] = 0x5c;
        out[o++] = b;
      } else if (b < 0x20) {
        const short = SHORT_ESCAPES[b];
        out[o++] = 0x5c;
        if (short) out[o++] = short;
        else o += out.write(`u00${b.toString(16).padStart(2, '0')}`, o, 'latin1');
      } else out[o++] = b;
    }
    out[o++] = 0x22;
    out[o++] = 0x7d;
    return out;
  }

  private frameLength(index: number) {
    if (this.chunkId === undefined) return this.prefix.length + this.tail.length;
    let n = this.lengths[index];
    if (n === undefined) {
      n = this.head(index).length + 2;
      for (let i = this.bounds[index]; i < this.bounds[index + 1]; i++) {
        const b = this.byteAt(i);
        n += b === 0x22 || b === 0x5c ? 2 : b < 0x20 ? (SHORT_ESCAPES[b] ? 2 : 6) : 1;
      }
      this.lengths[index] = n;
    }
    return n;
  }

  private head(index: number) {
    return Buffer.from(
      `{"_chunked":true,"_chunkId":${JSON.stringify(this.chunkId)},"_chunkIndex":${index},"_totalChunks":${this.count},"_data":"`
    );
  }

  private byteAt(position: number) {
    const p = this.prefix.length;
    return (position < p ? this.prefix[position] : this.tail[position - p]) ?? 0;
  }
}
