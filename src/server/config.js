const path = require('path');
require('dotenv').config({ path: path.resolve(process.cwd(), '.env') });

function getServerConfig() {
  return {
    port: Number(process.env.NODIO_SERVER_PORT || 4000),
    mongoUri: process.env.NODIO_MONGO_URI || 'mongodb://127.0.0.1:27017/nodio',
    heartbeatIntervalMs: Number(process.env.NODIO_HEARTBEAT_INTERVAL_MS || 30000),
    offlineAfterMisses: Number(process.env.NODIO_OFFLINE_AFTER_MISSES || 3),
    minReplicas: Number(process.env.NODIO_MIN_REPLICAS || 5),
    emergencyReplicaFloor: Number(process.env.NODIO_EMERGENCY_REPLICA_FLOOR || 2)
  };
}

module.exports = {
  getServerConfig
};
