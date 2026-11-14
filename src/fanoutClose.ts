import mongoose from 'mongoose';

// A connection the fanout closed whose close the gateway has not confirmed yet.
export type IFanoutClose = {
  endpoint: string;
  connectionId: string;
  firstAttemptAt: Date;
};

export const FanoutCloseSchema = new mongoose.Schema<IFanoutClose>({
  endpoint: { type: String, required: true },
  connectionId: { type: String, required: true },
  firstAttemptAt: { type: Date, required: true },
});

FanoutCloseSchema.index({ endpoint: 1, connectionId: 1 }, { unique: true });
// The gateway caps a connection at 2h, after which there is nothing left to close.
FanoutCloseSchema.index({ firstAttemptAt: 1 }, { expireAfterSeconds: 2 * 60 * 60 });

export const FanoutClose: mongoose.Model<IFanoutClose> =
  mongoose.models.FanoutClose ?? mongoose.model<IFanoutClose>('FanoutClose', FanoutCloseSchema, 'subscriberFanoutCloses');
