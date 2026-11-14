import mongoose from 'mongoose';

// A connection the fanout closed, found gone or removed as another gateway's, so a subscriber list that still
// names it is not served. A gateway connection lives at most 2h.
export type IFanoutTombstone = {
  _id: string;
  endpoint: string;
  connectionId: string;
  reason: string;
  expiresAt: Date;
};

export const FanoutTombstoneSchema = new mongoose.Schema<IFanoutTombstone>({
  _id: { type: String, required: true },
  endpoint: { type: String, required: true },
  connectionId: { type: String, required: true },
  reason: { type: String, required: true },
  expiresAt: { type: Date, required: true },
});

FanoutTombstoneSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

export const FanoutTombstone: mongoose.Model<IFanoutTombstone> =
  mongoose.models.FanoutTombstone ??
  mongoose.model<IFanoutTombstone>('FanoutTombstone', FanoutTombstoneSchema, 'subscriberFanoutTombstones');
