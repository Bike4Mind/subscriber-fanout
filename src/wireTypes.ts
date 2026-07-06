// Wire contract types for the fanout.
//
// These describe the two things this service touches on the wire:
//   1. the shape of documents it reads from Mongo (IMongoDocument), and
//   2. the frames it emits to WebSocket clients (IMessageDataToClient).
//
// They are intentionally vendored here (rather than imported from a shared
// package) so this repo builds standalone from a bare clone with no private
// registry access. They mirror the corresponding definitions on the consumer
// side; keep them structurally compatible. All are type-only and compile away
// to nothing at runtime.

/** Base fields every Mongo-backed document exposes. */
export interface IMongoDocument {
  id: string;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * A change-stream update fanned out to a subscriber. `data` carries the changed
 * document (or just its ids on delete); extra fields are passed through.
 */
export interface IDataSubscriptionUpdateAction {
  action: 'data_update';
  subscriptionId: string;
  collectionName: string;
  operationType: string;
  clientId?: string;
  data: {
    _id: string;
    id: string;
    [key: string]: unknown;
  };
}

/** Keepalive frame sent to subscribers on the periodic ping. */
export interface IPongAction {
  action: 'pong';
  clientId?: string;
}

/** Any frame this service sends to a client. */
export type IMessageDataToClient = IDataSubscriptionUpdateAction | IPongAction;
