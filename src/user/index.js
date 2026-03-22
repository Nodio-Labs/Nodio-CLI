#!/usr/bin/env node
const { Command } = require('commander');
const { uploadFile, downloadFile, deleteFile } = require('./commands');

const program = new Command();

program.name('nodio').description('Nodio user CLI');

program
  .command('upload')
  .description('Encrypt, shard, and distribute a file across donor nodes')
  .requiredOption('--file <path>', 'path to local file')
  .option('--server <url>', 'central server URL', 'http://127.0.0.1:4000')
  .option('--file-id <id>', 'custom file ID (optional)')
  .option('--shard-size-mb <mb>', 'plaintext shard size in MB', '1')
  .option('--replicas <count>', 'replicas per shard (minimum 5)', '5')
  .option('--key-base64 <key>', '32-byte AES key in base64 (optional)')
  .action(async (options) => {
    await uploadFile(options);
  });

program
  .command('download')
  .description('Download, verify, decrypt, and reconstruct a file')
  .requiredOption('--file-id <id>', 'file ID to download')
  .requiredOption('--key-base64 <key>', '32-byte AES key in base64 from upload output')
  .option('--server <url>', 'central server URL', 'http://127.0.0.1:4000')
  .option('--output <path>', 'output file path')
  .action(async (options) => {
    await downloadFile(options);
  });

program
  .command('delete')
  .description('Delete a file and its shard replicas from the network')
  .requiredOption('--file-id <id>', 'file ID to delete')
  .option('--server <url>', 'central server URL', 'http://127.0.0.1:4000')
  .action(async (options) => {
    await deleteFile(options);
  });

program.parseAsync(process.argv).catch((error) => {
  console.error('[nodio]', error.message);
  process.exit(1);
});
