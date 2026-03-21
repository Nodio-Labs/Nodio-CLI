const fs = require('fs/promises');
const path = require('path');
const { sha256Hex } = require('../common/crypto');

class LocalShardStore {
  constructor(baseDir) {
    this.baseDir = baseDir;
    this.indexFile = path.join(baseDir, 'index.json');
    this.shardsDir = path.join(baseDir, 'shards');
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
