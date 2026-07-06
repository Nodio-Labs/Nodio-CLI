#!/usr/bin/env node
const path = require('path');
const os = require('os');
const net = require('net');
const fs = require('fs');
const { Command } = require('commander');
const { bin: cloudflaredBin, install: installCloudflared, Tunnel } = require('cloudflared');
const { NodioNodeRuntime } = require('./runtime');

const program = new Command();

function parseCapacityGb(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const raw = String(value).trim().toLowerCase();
  const match = raw.match(/^([0-9]+(?:\.[0-9]+)?)(gb)?$/);
  if (!match) {
    throw new Error('capacity must be a number or <number>gb, e.g. 10 or 10gb');
  }

  return Number(match[1]);
}

async function isPortFree(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.unref();

    server.once('error', () => resolve(false));
    server.once('listening', () => {
      server.close(() => resolve(true));
    });

    server.listen({ port, exclusive: true });
  });
}

async function findFreePort(startPort, endPort) {
  for (let port = startPort; port <= endPort; port += 1) {
    // eslint-disable-next-line no-await-in-loop
    if (await isPortFree(port)) {
      return port;
    }
  }

  throw new Error(`no free port found in range ${startPort}-${endPort}`);
}

function detectAdvertisedHost() {
  const interfaces = os.networkInterfaces();
  for (const addresses of Object.values(interfaces)) {
    if (!Array.isArray(addresses)) {
      continue;
    }

    for (const address of addresses) {
      if (address.family === 'IPv4' && !address.internal) {
        return address.address;
      }
    }
  }

  return '127.0.0.1';
}

function isLoopbackHost(host) {
  const normalized = String(host || '').trim().toLowerCase();
  return normalized === '127.0.0.1' || normalized === 'localhost' || normalized === '::1';
}

function normalizePublicUrl(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

async function startCloudflareTunnel(localPort) {
  if (!cloudflaredBin) {
    throw new Error('cloudflared binary path is unavailable');
  }

  if (!fs.existsSync(cloudflaredBin)) {
    try {
      await installCloudflared(cloudflaredBin);
    } catch (error) {
      throw new Error(`cloudflared installation failed: ${error.message}`);
    }
  }

  const localUrl = `http://127.0.0.1:${localPort}`;
  const tunnel = Tunnel.quick(localUrl);

  return new Promise((resolve, reject) => {
    let settled = false;

    const complete = (error, publicUrl) => {
      if (settled) {
        return;
      }
      settled = true;

      if (error) {
        try {
          tunnel.stop?.();
        } catch {
          // ignore tunnel shutdown errors during startup failures
        }
        reject(error);
        return;
      }

      resolve({ publicUrl, tunnelProcess: tunnel });
    };

    tunnel.once('url', (url) => {
      const publicUrl = normalizePublicUrl(url);
      if (!publicUrl) {
        complete(new Error('cloudflared published an empty tunnel URL'));
        return;
      }
      complete(null, publicUrl);
    });

    tunnel.once('error', (error) => {
      complete(new Error(`cloudflared tunnel error: ${error.message}`));
    });

    tunnel.once('exit', (code) => {
      if (!settled) {
        complete(new Error(`cloudflared exited before publishing a tunnel URL (code ${code})`));
      }
    });
  });
}

program
  .name('nodio-node')
  .description('Nodio donor node CLI')
  .argument('[capacity]', 'optional capacity shorthand, e.g. 10gb')
  .option('--node-id <id>', 'unique node ID (optional; auto-assigned and persisted if omitted)')
  .option('--server <url>', 'central server URL', 'https://api.nodio.me')
  .option('--host <host>', 'host/IP exposed to network (default: auto-detected LAN IP)', 'auto')
  .option('--public-url <url>', 'explicit public URL for donor fetches, e.g. https://donor.example.com')
  .option('--port <port>', 'port to expose shard API (auto-picked when omitted)')
  .option('--storage-dir <path>', 'local shard storage directory (defaults to ~/.nodio-nodes/node-<port>)')
  .option('--capacity-gb <gb>', 'donated capacity in GB', '10')
  .option('--auto-port-start <port>', 'start of auto-port range', '5001')
  .option('--auto-port-end <port>', 'end of auto-port range', '5999')
  .option('--heartbeat-ms <ms>', 'heartbeat interval in milliseconds', '30000')
  .option('--relay-poll-ms <ms>', 'relay task poll interval in milliseconds', '1000');

program.action(async (capacityArg, options) => {
  const autoPortStart = Number(options.autoPortStart);
  const autoPortEnd = Number(options.autoPortEnd);
  const port = options.port
    ? Number(options.port)
    : await findFreePort(autoPortStart, autoPortEnd);

  const parsedCapacityArg = parseCapacityGb(capacityArg);
  const capacityGb = parsedCapacityArg ?? Number(options.capacityGb);
  const heartbeatMs = Number(options.heartbeatMs);
  const relayPollMs = Number(options.relayPollMs);
  const advertisedHost = options.host === 'auto' ? detectAdvertisedHost() : options.host;
  const explicitPublicUrl = normalizePublicUrl(options.publicUrl);
  const storageDir = options.storageDir
    ? path.resolve(options.storageDir)
    : path.join(os.homedir(), '.nodio-nodes', `node-${port}`);

  if (!Number.isInteger(port) || port <= 0) {
    throw new Error('port must be a positive integer');
  }
  if (!Number.isInteger(autoPortStart) || !Number.isInteger(autoPortEnd) || autoPortStart <= 0 || autoPortEnd < autoPortStart) {
    throw new Error('auto-port range is invalid');
  }
  if (!Number.isFinite(capacityGb) || capacityGb <= 0) {
    throw new Error('capacity-gb must be greater than 0');
  }
  if (!Number.isFinite(heartbeatMs) || heartbeatMs <= 0) {
    throw new Error('heartbeat-ms must be greater than 0');
  }
  if (!Number.isFinite(relayPollMs) || relayPollMs <= 0) {
    throw new Error('relay-poll-ms must be greater than 0');
  }

  if (isLoopbackHost(advertisedHost)) {
    console.warn('[nodio-node] warning: loopback host is advertised; only this machine can reach this donor');
  }

  let tunnelProcess = null;
  let advertisedUrl = explicitPublicUrl;

  if (!advertisedUrl) {
    try {
      const tunnel = await startCloudflareTunnel(port);
      tunnelProcess = tunnel.tunnelProcess;
      advertisedUrl = tunnel.publicUrl;
      console.log(`[nodio-node] cloudflare tunnel ready: ${advertisedUrl}`);
    } catch (error) {
      console.warn(`[nodio-node] cloudflare tunnel unavailable, falling back to local URL: ${error.message}`);
    }
  }

  if (!advertisedUrl) {
    advertisedUrl = `http://${advertisedHost}:${port}`;
  }

  if (explicitPublicUrl) {
    console.log(`[nodio-node] using explicit public URL: ${explicitPublicUrl}`);
  }

  const cleanupTunnel = () => {
    if (tunnelProcess && !tunnelProcess.killed) {
      tunnelProcess.kill('SIGTERM');
    }
  };

  process.once('exit', cleanupTunnel);
  process.once('SIGINT', () => {
    cleanupTunnel();
    process.exit(130);
  });
  process.once('SIGTERM', () => {
    cleanupTunnel();
    process.exit(143);
  });

  try {
    const runtime = new NodioNodeRuntime({
      nodeId: options.nodeId,
      serverUrl: options.server,
      publicUrl: advertisedUrl,
      port,
      storageDir,
      capacityBytes: Math.floor(capacityGb * 1024 * 1024 * 1024),
      heartbeatIntervalMs: heartbeatMs,
      relayPollIntervalMs: relayPollMs
    });

    await runtime.start();
  } catch (error) {
    cleanupTunnel();
    throw error;
  }
});

program.parseAsync(process.argv).catch((error) => {
  console.error('[nodio-node]', error.message);
  process.exit(1);
});
