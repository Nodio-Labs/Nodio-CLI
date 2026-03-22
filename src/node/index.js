#!/usr/bin/env node
const path = require('path');
const os = require('os');
const { Command } = require('commander');
const { NodioNodeRuntime } = require('./runtime');

const program = new Command();

program
  .name('nodio-node')
  .description('Nodio donor node CLI')
  .option('--node-id <id>', 'unique node ID (optional; auto-assigned and persisted if omitted)')
  .option('--server <url>', 'central server URL', 'http://127.0.0.1:4000')
  .option('--host <host>', 'host/IP exposed to network', '127.0.0.1')
  .option('--port <port>', 'port to expose shard API', '5001')
  .option('--storage-dir <path>', 'local shard storage directory', path.join(os.homedir(), '.nodio-node'))
  .option('--capacity-gb <gb>', 'donated capacity in GB', '10')
  .option('--heartbeat-ms <ms>', 'heartbeat interval in milliseconds', '30000');

program.action(async (options) => {
  const port = Number(options.port);
  const capacityGb = Number(options.capacityGb);
  const heartbeatMs = Number(options.heartbeatMs);

  if (!Number.isInteger(port) || port <= 0) {
    throw new Error('port must be a positive integer');
  }
  if (!Number.isFinite(capacityGb) || capacityGb <= 0) {
    throw new Error('capacity-gb must be greater than 0');
  }
  if (!Number.isFinite(heartbeatMs) || heartbeatMs <= 0) {
    throw new Error('heartbeat-ms must be greater than 0');
  }

  const runtime = new NodioNodeRuntime({
    nodeId: options.nodeId,
    serverUrl: options.server,
    publicUrl: `http://${options.host}:${port}`,
    port,
    storageDir: path.resolve(options.storageDir),
    capacityBytes: Math.floor(capacityGb * 1024 * 1024 * 1024),
    heartbeatIntervalMs: heartbeatMs
  });

  await runtime.start();
});

program.parseAsync(process.argv).catch((error) => {
  console.error('[nodio-node]', error.message);
  process.exit(1);
});
