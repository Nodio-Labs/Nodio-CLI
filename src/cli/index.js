#!/usr/bin/env node
const path = require('path');
const { spawn } = require('child_process');

const commandMap = {
  'nodio-node': path.join(__dirname, '..', 'node', 'index.js'),
  'nodio-server': path.join(__dirname, '..', 'server', 'index.js'),
  nodio: path.join(__dirname, '..', 'user', 'index.js')
};

function printHelp() {
  console.log('Nodio CLI dispatcher');
  console.log('');
  console.log('Usage:');
  console.log('  nodio-cli <command> [...args]');
  console.log('');
  console.log('Commands:');
  console.log('  nodio-node      Start donor node runtime');
  console.log('  nodio-server    Start central server');
  console.log('  nodio           User CLI (upload/download/delete)');
  console.log('');
  console.log('Example:');
  console.log('  npx nodio-cli nodio-node 10gb');
}

const [command, ...restArgs] = process.argv.slice(2);

if (!command || command === '--help' || command === '-h') {
  printHelp();
  process.exit(0);
}

const scriptPath = commandMap[command];
if (!scriptPath) {
  console.error(`[nodio-cli] Unknown command: ${command}`);
  printHelp();
  process.exit(1);
}

const child = spawn(process.execPath, [scriptPath, ...restArgs], {
  stdio: 'inherit'
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
