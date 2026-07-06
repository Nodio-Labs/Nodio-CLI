const express = require('express');
const axios = require('axios');
const { LocalShardStore } = require('./storage');
const { getOrCreateDeviceKey } = require('./deviceIdentity');

function normalizeUrl(url) {
  return String(url || '').replace(/\/+$/, '');
}

class NodioNodeRuntime {
  constructor(options) {
    this.nodeId = options.nodeId || null;
    this.serverUrl = normalizeUrl(options.serverUrl);
    this.publicUrl = normalizeUrl(options.publicUrl);
    this.port = Number(options.port);
    this.capacityBytes = Number(options.capacityBytes);
    this.heartbeatIntervalMs = Number(options.heartbeatIntervalMs || 30000);
    this.relayPollIntervalMs = Number(options.relayPollIntervalMs || 1000);
    this.shardStore = new LocalShardStore(options.storageDir);
    this.heartbeatTimer = null;
    this.relayPollTimer = null;
    this.relayPullInFlight = false;
  }

  async start() {
    await this.shardStore.init();

    if (!this.nodeId) {
      this.nodeId = await this.shardStore.getSavedNodeId();
    }

    this.nodeSecret = await this.shardStore.getSavedNodeSecret();

    await this.startShardServer();
    await this.registerNode();
    await this.sendHeartbeat();

    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.sendHeartbeat();
      } catch (error) {
        console.error('[heartbeat]', error.message);
      }
    }, this.heartbeatIntervalMs);

    this.relayPollTimer = setInterval(async () => {
      try {
        await this.pollRelayTasks();
      } catch (error) {
        console.error('[relay-pull]', error.message);
      }
    }, this.relayPollIntervalMs);
  }

  async startShardServer() {
    const app = express();
    app.use('/shards/:shardId', express.raw({ type: '*/*', limit: '200mb' }));

    const requireNodeSecret = (req, res, next) => {
      const authHeader = req.headers.authorization;
      const token = authHeader?.replace('Bearer ', '');
      if (!token || token !== this.nodeSecret) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
      return next();
    };

    app.put('/shards/:shardId', requireNodeSecret, async (req, res) => {
      try {
        const { shardId } = req.params;
        if (!Buffer.isBuffer(req.body)) {
          return res.status(400).json({ error: 'binary payload is required' });
        }
        const record = await this.shardStore.saveShard(shardId, req.body);
        res.json({ ok: true, shard: record });
      } catch (error) {
        res.status(500).json({ error: error.message });
      }
    });

    app.get('/shards/:shardId', requireNodeSecret, async (req, res) => {
      try {
        const { shardId } = req.params;
        if (!(await this.shardStore.hasShard(shardId))) {
          return res.status(404).json({ error: 'shard not found' });
        }

        const data = await this.shardStore.readShard(shardId);
        res.setHeader('Content-Type', 'application/octet-stream');
        res.send(data);
      } catch (error) {
        res.status(500).json({ error: error.message });
      }
    });

    app.delete('/shards/:shardId', requireNodeSecret, async (req, res) => {
      try {
        const { shardId } = req.params;
        const result = await this.shardStore.deleteShard(shardId);
        res.json({ ok: true, shard: result });
      } catch (error) {
        res.status(500).json({ error: error.message });
      }
    });

    app.get('/health', (_req, res) => {
      res.json({ ok: true, nodeId: this.nodeId });
    });

    await new Promise((resolve, reject) => {
      const server = app.listen({ port: this.port, exclusive: true }, () => {
        console.log(`Nodio node ${this.nodeId} listening on port ${this.port}`);
        resolve();
      });

      server.once('error', (error) => {
        if (error && error.code === 'EADDRINUSE') {
          return reject(new Error(`port ${this.port} is already in use`));
        }
        return reject(error);
      });
    });
  }

  async freeBytes() {
    const used = await this.shardStore.usedBytes();
    return Math.max(this.capacityBytes - used, 0);
  }

  async registerNode() {
    const deviceKey = await getOrCreateDeviceKey();
    const nodeKey = await this.shardStore.getOrCreateNodeKey();
    const knownNodeIds = await this.shardStore.discoverKnownNodeIds();
    const nodeSecret = this.nodeSecret || undefined;

    const response = await axios.post(`${this.serverUrl}/api/nodes/register`, {
      nodeId: this.nodeId,
      deviceKey,
      nodeKey,
      nodeSecret,
      knownNodeIds,
      publicUrl: this.publicUrl,
      url: this.publicUrl,
      capacityBytes: this.capacityBytes,
      freeBytes: await this.freeBytes()
    });

    this.nodeId = response.data.nodeId;
    await this.shardStore.saveAssignedNodeId(this.nodeId);

    if (response.data.nodeSecret) {
      this.nodeSecret = response.data.nodeSecret;
      await this.shardStore.saveAssignedNodeSecret(this.nodeSecret);
    }

    const interval = Number(response.data.heartbeatIntervalMs);
    if (Number.isFinite(interval) && interval > 0) {
      this.heartbeatIntervalMs = interval;
    }

    const relayInterval = Number(response.data.relayPollIntervalMs);
    if (Number.isFinite(relayInterval) && relayInterval > 0) {
      this.relayPollIntervalMs = relayInterval;
    }

    console.log(
      `Registered node ${this.nodeId} | min replicas: ${response.data.minReplicas} | emergency floor: ${response.data.emergencyReplicaFloor}`
    );
  }

  async sendHeartbeat() {
    const shardIds = await this.shardStore.listShardIds();
    const response = await axios.post(`${this.serverUrl}/api/nodes/heartbeat`, {
      nodeId: this.nodeId,
      freeBytes: await this.freeBytes(),
      shardIds
    });

    const tasks = response.data.replicationTasks || [];
    for (const task of tasks) {
      await this.executeReplicationTask(task);
    }

    console.log(
      `[heartbeat] ${new Date().toISOString()} | shards=${shardIds.length} | tasks=${tasks.length}`
    );
  }

  async pollRelayTasks() {
    if (this.relayPullInFlight) {
      return;
    }

    this.relayPullInFlight = true;
    try {
      const response = await axios.post(`${this.serverUrl}/api/nodes/relay-pull`, {
        nodeId: this.nodeId
      });

      const relayTasks = response.data.relayTasks || [];
      for (const task of relayTasks) {
        // eslint-disable-next-line no-await-in-loop
        await this.executeRelayTask(task);
      }

      if (relayTasks.length > 0) {
        console.log(`[relay-pull] ${new Date().toISOString()} | relayTasks=${relayTasks.length}`);
      }
    } finally {
      this.relayPullInFlight = false;
    }
  }

  async executeReplicationTask(task) {
    const sourceShardUrl = `${normalizeUrl(task.sourceUrl)}/shards/${task.shardId}`;
    try {
      const sourceResponse = await axios.get(sourceShardUrl, {
        responseType: 'arraybuffer',
        timeout: 30000,
        headers: {
          Authorization: `Bearer ${task.sourceNodeSecret}`
        }
      });
      const data = Buffer.from(sourceResponse.data);
      await this.shardStore.saveShard(task.shardId, data);

      await axios.post(`${this.serverUrl}/api/replication-tasks/${task.taskId}/complete`, {
        nodeId: this.nodeId,
        success: true
      });

      console.log(`[replication] completed task=${task.taskId} shard=${task.shardId}`);
    } catch (error) {
      const message = error.response?.data?.error || error.message;
      await axios
        .post(`${this.serverUrl}/api/replication-tasks/${task.taskId}/complete`, {
          nodeId: this.nodeId,
          success: false,
          errorMessage: message
        })
        .catch(() => null);

      console.error(`[replication] failed task=${task.taskId} shard=${task.shardId}: ${message}`);
    }
  }

  async executeRelayTask(task) {
    try {
      if (task.taskType === 'store') {
        if (typeof task.dataBase64 !== 'string') {
          throw new Error('relay store payload is missing dataBase64');
        }

        const payload = Buffer.from(task.dataBase64, 'base64');
        await this.shardStore.saveShard(task.shardId, payload);

        await axios.post(`${this.serverUrl}/api/relay-tasks/${task.taskId}/complete`, {
          nodeId: this.nodeId,
          success: true
        });

        console.log(`[relay] stored shard=${task.shardId} task=${task.taskId}`);
        return;
      }

      if (task.taskType === 'fetch') {
        if (!(await this.shardStore.hasShard(task.shardId))) {
          throw new Error('shard not found on donor');
        }

        const payload = await this.shardStore.readShard(task.shardId);
        await axios.post(`${this.serverUrl}/api/relay-tasks/${task.taskId}/complete`, {
          nodeId: this.nodeId,
          success: true,
          resultDataBase64: payload.toString('base64')
        });

        console.log(`[relay] fetched shard=${task.shardId} task=${task.taskId}`);
        return;
      }

      throw new Error(`unsupported relay task type: ${task.taskType}`);
    } catch (error) {
      const message = error.response?.data?.error || error.message;
      await axios
        .post(`${this.serverUrl}/api/relay-tasks/${task.taskId}/complete`, {
          nodeId: this.nodeId,
          success: false,
          errorMessage: message
        })
        .catch(() => null);

      console.error(`[relay] failed task=${task.taskId} shard=${task.shardId}: ${message}`);
    }
  }
}

module.exports = {
  NodioNodeRuntime
};
