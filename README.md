# Nodio-CLI

Nodio is a CLI-based distributed storage network.

This implementation includes:
- Central server (Express + MongoDB) for node registry, shard metadata, placement planning, heartbeat tracking, and replication task orchestration
- `nodio-node` donor CLI for running storage nodes, sending heartbeats every 30s, storing shard blobs, and executing replication tasks
- `nodio` user CLI for encrypted upload/download with shard distribution and reconstruction

## Requirements

- Node.js 20+
- MongoDB running locally or reachable by URI

## Install

```bash
npm install
```

## Configure

```bash
cp .env.example .env
```

Environment values:
- `NODIO_SERVER_PORT`: central API port (default `4000`)
- `NODIO_MONGO_URI`: MongoDB connection string
- `NODIO_HEARTBEAT_INTERVAL_MS`: expected heartbeat interval (`30000`)
- `NODIO_OFFLINE_AFTER_MISSES`: offline threshold in missed heartbeats (`3`)
- `NODIO_MIN_REPLICAS`: target replicas for placement planning (`5`)
- `NODIO_EMERGENCY_REPLICA_FLOOR`: immediate repair threshold (`2`)

## Start Central Server

```bash
npm run start:server
```

Server API base: `http://127.0.0.1:4000/api`

## Start Donor Node CLI

```bash
npm run start:node -- \
	--node-id node-a \
	--server http://127.0.0.1:4000 \
	--host 127.0.0.1 \
	--port 5001 \
	--storage-dir ./.nodio-node-a \
	--capacity-gb 10
```

Run more nodes by changing `--node-id`, `--port`, and `--storage-dir`.

## User CLI (Phase 2)

Upload (encrypt + shard + distribute):

```bash
npm run start:cli -- upload \
	--file ./example.txt \
	--server http://127.0.0.1:4000 \
	--shard-size-mb 1 \
	--replicas 5
```

The upload command prints:
- `fileId`
- `aes256KeyBase64` (required for download)

Download (fetch + verify + decrypt + reconstruct):

```bash
npm run start:cli -- download \
	--file-id <FILE_ID> \
	--key-base64 <AES_256_KEY_BASE64> \
	--server http://127.0.0.1:4000 \
	--output ./restored-example.txt
```

## Core Behavior Implemented

- Files can be represented as shards with metadata in MongoDB (`files`, `shards`, and `shard placements`)
- Placement planning enforces distinct nodes per shard and defaults to 5 replicas
- User uploads enforce at least 5 replicas per shard
- Node heartbeats every 30 seconds update status and available storage
- If a node misses 3 heartbeat intervals, it is marked offline
- When live replicas of a shard drop below 2, the server immediately creates replication tasks to healthy nodes
- Donor nodes fetch pending replication tasks through heartbeats and self-heal by copying shard data from source nodes
- User downloads verify shard checksums, decrypt with AES-256-GCM metadata, and reconstruct files in shard order

## Implemented API Surface

- `POST /api/nodes/register`
- `POST /api/nodes/heartbeat`
- `POST /api/replication-tasks/:taskId/complete`
- `POST /api/files/register`
- `POST /api/shards/register`
- `POST /api/shards/placement-plan`
- `GET /api/files/:fileId/manifest`

## Next Step

Add authentication between user CLI, central server, and donor nodes for production security.