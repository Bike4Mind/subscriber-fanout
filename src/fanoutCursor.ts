import mongoose from 'mongoose';

export type IFanoutCursor = {
  collectionName: string;
  // JSON of the last change stream event _id seen on this collection
  resumeToken: string;
  updatedAt: Date;
};

export const FanoutCursorSchema = new mongoose.Schema<IFanoutCursor>(
  {
    collectionName: { type: String, required: true, unique: true },
    resumeToken: { type: String, required: true },
  },
  {
    timestamps: { createdAt: false, updatedAt: true },
  }
);

export const FanoutCursor: mongoose.Model<IFanoutCursor> =
  mongoose.models.FanoutCursor ??
  mongoose.model<IFanoutCursor>('FanoutCursor', FanoutCursorSchema, 'subscriberFanoutCursors');
