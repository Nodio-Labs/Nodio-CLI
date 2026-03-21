const mongoose = require('mongoose');

const nodeSchema = new mongoose.Schema(
  {
    nodeId: { type: String, required: true, unique: true, index: true },
    url: { type: String, required: true },
    capacityBytes: { type: Number, required: true, min: 1 },
    freeBytes: { type: Number, required: true, min: 0 },
    status: { type: String, enum: ['online', 'offline'], default: 'online', index: true },
    lastHeartbeatAt: { type: Date, default: Date.now, index: true }
  },
  { timestamps: true }
);

const fileSchema = new mongoose.Schema(
  {
    fileId: { type: String, required: true, unique: true, index: true },
    originalName: { type: String, required: true },
    sizeBytes: { type: Number, required: true, min: 0 },
    shardCount: { type: Number, required: true, min: 1 },
    cipher: { type: String, required: true, default: 'aes-256-gcm' },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} }
  },
  { timestamps: true }
);

const shardSchema = new mongoose.Schema(
  {
    shardId: { type: String, required: true, unique: true, index: true },
    fileId: { type: String, required: true, index: true },
    order: { type: Number, required: true, min: 0 },
    sizeBytes: { type: Number, required: true, min: 0 },
    checksum: { type: String, required: true }
  },
  { timestamps: true }
);

const shardPlacementSchema = new mongoose.Schema(
  {
    shardId: { type: String, required: true, index: true },
    fileId: { type: String, required: true, index: true },
    nodeId: { type: String, required: true, index: true },
    status: { type: String, enum: ['available', 'lost'], default: 'available', index: true }
  },
  { timestamps: true }
);

shardPlacementSchema.index({ shardId: 1, nodeId: 1 }, { unique: true });

const replicationTaskSchema = new mongoose.Schema(
  {
    shardId: { type: String, required: true, index: true },
    fileId: { type: String, required: true, index: true },
    sourceNodeId: { type: String, required: true, index: true },
    targetNodeId: { type: String, required: true, index: true },
    status: {
      type: String,
      enum: ['pending', 'in_progress', 'completed', 'failed'],
      default: 'pending',
      index: true
    },
    errorMessage: { type: String, default: null },
    attempts: { type: Number, default: 0 }
  },
  { timestamps: true }
);

replicationTaskSchema.index(
  { shardId: 1, targetNodeId: 1, status: 1 },
  {
    unique: true,
    partialFilterExpression: {
      status: { $in: ['pending', 'in_progress'] }
    }
  }
);

module.exports = {
  NodeModel: mongoose.model('Node', nodeSchema),
  FileModel: mongoose.model('File', fileSchema),
  ShardModel: mongoose.model('Shard', shardSchema),
  ShardPlacementModel: mongoose.model('ShardPlacement', shardPlacementSchema),
  ReplicationTaskModel: mongoose.model('ReplicationTask', replicationTaskSchema)
};
