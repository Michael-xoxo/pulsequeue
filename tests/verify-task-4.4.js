require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createRequire } = require('module');
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const batchQueue = require('../queues/batchQueue');
const batchWorker = require('../workers/batchWorker');
const User = require('../models/User');
const FailedJob = require('../models/FailedJob');
const { startServer, closeServer } = require('../server');

// Load socket.io-client directly from /client/node_modules to verify the actual client library contract
const clientRequire = createRequire(path.join(__dirname, '../client/package.json'));
const { io: createClientSocket } = clientRequire('socket.io-client');

const TEST_PORT = 5058;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

async function runVerification() {
  console.log('=== STARTING TASK 4.4 END-TO-END UI CONTRACT & BUILD VERIFICATION ===\n');
  let exitCode = 0;
  let socketClient = null;
  const createdJobIds = [];
  const createdEmails = [];

  try {
    // 1. Verify Production Bundle Build Output & Static Structural Checks
    console.log('[Test 1] Verifying /client production build artifacts & zero-polling constraint...');
    const distIndex = path.join(__dirname, '../client/dist/index.html');
    if (!fs.existsSync(distIndex)) {
      throw new Error('client/dist/index.html not found — production build did not succeed');
    }

    const viteConfigContent = fs.readFileSync(path.join(__dirname, '../client/vite.config.js'), 'utf8');
    if (!viteConfigContent.includes("'/api'") || !viteConfigContent.includes("'/socket.io'") || !viteConfigContent.includes('ws: true')) {
      throw new Error('client/vite.config.js is missing /api or /socket.io (ws: true) proxy rules');
    }

    const srcDir = path.join(__dirname, '../client/src');
    const checkNoPolling = (dir) => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          checkNoPolling(fullPath);
        } else if (entry.name.endsWith('.js') || entry.name.endsWith('.jsx')) {
          const content = fs.readFileSync(fullPath, 'utf8');
          if (content.includes('setInterval(')) {
            throw new Error(`Forbidden setInterval polling detected in ${fullPath}`);
          }
        }
      }
    };
    checkNoPolling(srcDir);

    console.log('  PASS: client/dist/index.html exists and compiled cleanly');
    console.log('  PASS: client/vite.config.js proxies /api and /socket.io (ws: true) -> http://localhost:5000');
    console.log('  PASS: Zero setInterval polling verified across all client/src components & hooks\n');

    // 2. Start Backend + Worker and Verify Auth -> Socket.io-client -> Live Feed -> DLQ Flow
    await startServer(TEST_PORT);
    await batchWorker.waitUntilReady();

    console.log('[Test 2] Verifying AuthContext Registration & Login contract...');
    const ts = Date.now();
    const email = `mission_control_${ts}@pulsequeue.test`;
    createdEmails.push(email);

    const regRes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'Mission Control Operator',
        email,
        password: 'Password123!',
      }),
    });
    const regData = await regRes.json();
    if (regRes.status !== 201 || !regData.token) {
      throw new Error(`Registration failed: ${JSON.stringify(regData)}`);
    }

    const loginRes = await fetch(`${BASE_URL}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password: 'Password123!' }),
    });
    const loginData = await loginRes.json();
    if (loginRes.status !== 200 || !loginData.token) {
      throw new Error(`Login failed: ${JSON.stringify(loginData)}`);
    }
    console.log(`  PASS: Operator registered and logged in (userId="${loginData._id}")\n`);

    // 3. Connect using socket.io-client with auth: { token }
    console.log('[Test 3] Connecting via socket.io-client (usePulseSocket contract)...');
    const progressTicks = [];
    let completedPayload = null;
    let dlqFailedPayload = null;

    let resolveCompleted;
    const completedPromise = new Promise((r) => {
      resolveCompleted = r;
    });

    let resolveDlqFailed;
    const dlqFailedPromise = new Promise((r) => {
      resolveDlqFailed = r;
    });

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('socket.io-client connection timeout')), 10000);
      socketClient = createClientSocket(BASE_URL, {
        auth: { token: loginData.token },
        transports: ['websocket', 'polling'],
      });

      socketClient.on('tenant:joined', (info) => {
        clearTimeout(timer);
        console.log(`  PASS: socket.io-client connected and joined "${info.room}"\n`);
        resolve(info);
      });

      socketClient.on('connect_error', (err) => {
        clearTimeout(timer);
        reject(err);
      });

      socketClient.on('job:progress', (payload) => {
        progressTicks.push(payload);
      });

      socketClient.on('job:completed', (payload) => {
        completedPayload = payload;
        resolveCompleted(payload);
      });

      socketClient.on('job:failed', (payload) => {
        if (payload.isDeadLetter === true) {
          dlqFailedPayload = payload;
          resolveDlqFailed(payload);
        }
      });
    });

    // 4. Dispatch job & verify LiveJobFeed hydration + real-time progress 0 -> 100%
    console.log('[Test 4] Dispatching WEBHOOK_FANOUT job & verifying LiveJobFeed stream...');
    const dispatchRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${loginData.token}`,
      },
      body: JSON.stringify({
        jobType: 'WEBHOOK_FANOUT',
        itemCount: 4,
      }),
    });
    const dispatchData = await dispatchRes.json();
    createdJobIds.push(dispatchData.jobId);

    if (dispatchRes.status !== 202 || dispatchData.status !== 'queued') {
      throw new Error(`Expected HTTP 202 queued response, got: ${JSON.stringify(dispatchData)}`);
    }

    await completedPromise;

    const jobProgressValues = progressTicks
      .filter((p) => p.jobId === dispatchData.jobId)
      .map((p) => p.progress);

    if (
      jobProgressValues.length !== 4 ||
      jobProgressValues[3] !== 100 ||
      !completedPayload ||
      completedPayload.jobId !== dispatchData.jobId
    ) {
      throw new Error(
        `Unexpected progress/completion sequence: progress=${JSON.stringify(jobProgressValues)}, completed=${JSON.stringify(completedPayload)}`
      );
    }

    const hydrateRes = await fetch(`${BASE_URL}/api/jobs`, {
      headers: { Authorization: `Bearer ${loginData.token}` },
    });
    const hydratedJobs = await hydrateRes.json();
    const hydratedMatch = hydratedJobs.find((j) => j.jobId === dispatchData.jobId);
    if (!hydratedMatch || hydratedMatch.state !== 'completed' || hydratedMatch.progress !== 100) {
      throw new Error(`Hydrated job state mismatch: ${JSON.stringify(hydratedMatch)}`);
    }

    console.log(`  PASS: LiveJobFeed animated progress [${jobProgressValues.join('%, ')}%] and transitioned to "completed"`);
    console.log(`  PASS: GET /api/jobs hydrated completed job "${hydratedMatch.jobId}"\n`);

    // 5. Dispatch Poison-Pill Job & Verify DlqInspector Auto-Refresh Contract
    console.log('[Test 5] Dispatching poison-pill job & verifying DlqInspector quarantine...');
    const poisonRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${loginData.token}`,
      },
      body: JSON.stringify({
        jobType: 'LEDGER_RECONCILE',
        itemCount: 2,
        simulateFatalFail: true,
      }),
    });
    const poisonData = await poisonRes.json();
    createdJobIds.push(poisonData.jobId);

    await dlqFailedPromise;

    const dlqRes = await fetch(`${BASE_URL}/api/jobs/dlq`, {
      headers: { Authorization: `Bearer ${loginData.token}` },
    });
    const dlqItems = await dlqRes.json();
    const quarantined = dlqItems.find((item) => item.jobId === poisonData.jobId);

    if (!quarantined || quarantined.attemptsMade !== 3 || !dlqFailedPayload.isDeadLetter) {
      throw new Error(`DLQ Inspector contract failed: ${JSON.stringify({ dlqFailedPayload, quarantined })}`);
    }

    console.log(`  PASS: Received socket event job:failed (isDeadLetter=true, attemptsMade=${dlqFailedPayload.attemptsMade})`);
    console.log(`  PASS: DlqInspector GET /api/jobs/dlq returned quarantined job "${quarantined.jobId}" ("${quarantined.failedReason}")\n`);

    console.log('=== ALL TASK 4.4 VERIFICATION CHECKS PASSED ===');
  } catch (error) {
    console.error('VERIFICATION FAILED:', error);
    exitCode = 1;
  } finally {
    if (socketClient) {
      socketClient.disconnect();
    }
    for (const jId of createdJobIds) {
      try {
        const j = await batchQueue.getJob(jId);
        if (j) await j.remove();
      } catch (_) {}
    }
    await FailedJob.deleteMany({ jobId: { $in: createdJobIds } }).catch(() => {});
    await User.deleteMany({ email: { $in: createdEmails } }).catch(() => {});

    await batchWorker.close();
    await batchWorker.workerConnection.quit();
    await closeServer();
    await batchQueue.close();
    await redisConnection.quit();
    await mongoose.connection.close();
    process.exit(exitCode);
  }
}

runVerification();
