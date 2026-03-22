const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { sha256Hex } = require('../common/crypto');

class LocalShardStore {
  constructor(baseDir) {
    this.baseDir = baseDir;
    this.indexFile = path.join(baseDir, 'index.json');
    this.shardsDir = path.join(baseDir, 'shards');
    this.identityFile = path.join(baseDir, 'node-identity.json');
  }

  async init() {
    await fs.mkdir(this.shardsDir, { recursive: true });
    try {
      await fs.access(this.indexFile);
    } catch {
      await fs.writeFile(this.indexFile, JSON.stringify({ shards: {} }, null, 2), 'utf-8');
    }
  }

  async readIndex() {
    const raw = await fs.readFile(this.indexFile, 'utf-8');
    const parsed = JSON.parse(raw);
    if (!parsed.shards || typeof parsed.shards !== 'object') {
      return { shards: {} };
    }
    return parsed;
  }

  async writeIndex(index) {
    await fs.writeFile(this.indexFile, JSON.stringify(index, null, 2), 'utf-8');
  }

  async readIdentity() {
    try {
      const raw = await fs.readFile(this.identityFile, 'utf-8');
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  }

  async writeIdentity(identity) {
    await fs.writeFile(this.identityFile, JSON.stringify(identity, null, 2), 'utf-8');
  }

  async getIdentity() {
    return this.readIdentity();
  }

  async getOrCreateNodeKey() {
    const identity = await this.readIdentity();
    if (identity.nodeKey) {
      return identity.nodeKey;
    }

    const nodeKey = crypto.randomUUID();
    await this.writeIdentity({ ...identity, nodeKey, updatedAt: new Date().toISOString() });
    return nodeKey;
  }

  async getSavedNodeId() {
    const identity = await this.readIdentity();
    return identity.nodeId || null;
  }

  async saveAssignedNodeId(nodeId) {
    if (!nodeId) {
      return;
    }

    const identity = await this.readIdentity();
    await this.writeIdentity({
      ...identity,
      nodeId,
      updatedAt: new Date().toISOString()
    });
  }

  shardPath(shardId) {
    return path.join(this.shardsDir, `${shardId}.bin`);
  }

  async saveShard(shardId, dataBuffer) {
    const filePath = this.shardPath(shardId);
    await fs.writeFile(filePath, dataBuffer);

    const index = await this.readIndex();
    index.shards[shardId] = {
      shardId,
      sizeBytes: dataBuffer.byteLength,
      checksum: sha256Hex(dataBuffer),
      updatedAt: new Date().toISOString()
    };
    await this.writeIndex(index);
    return index.shards[shardId];
  }

  async readShard(shardId) {
    const filePath = this.shardPath(shardId);
    return fs.readFile(filePath);
  }

  async hasShard(shardId) {
    try {
      await fs.access(this.shardPath(shardId));
      return true;
    } catch {
      return false;
    }
  }

  async deleteShard(shardId) {
    const filePath = this.shardPath(shardId);

    try {
      await fs.unlink(filePath);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }

    const index = await this.readIndex();
    if (index.shards[shardId]) {
      delete index.shards[shardId];
      await this.writeIndex(index);
    }

    return { shardId, deleted: true };
  }

  async listShardIds() {
    const index = await this.readIndex();
    return Object.keys(index.shards);
  }

  async usedBytes() {
    const index = await this.readIndex();
    return Object.values(index.shards).reduce((sum, shard) => sum + Number(shard.sizeBytes || 0), 0);
  }
}

module.exports = {
  LocalShardStore
};
