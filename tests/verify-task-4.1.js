require('dotenv').config();
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const batchQueue = require('../queues/batchQueue');
const User = require('../models/User');
const { startServer, server, io } = require('../server');

const TEST_PORT = 5055;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

/**
 * Minimal Socket.IO v4 (EIO=4) client handshake helper using native fetch / WebSocket
 */
async function connectSocketIO({ token, useHeader = false }) {
  const headers = {};
  if (useHeader && token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  // Step 1: Engine.IO polling open handshake
  const openRes = await fetch(`${BASE_URL}/socket.io/?EIO=4&transport=polling`, { headers });
  const openText = await openRes.text();
  const jsonStart = openText.indexOf('{');
  const openPacket = JSON.parse(openText.slice(jsonStart));
  const sid = openPacket.sid;

  // Step 2: Send Socket.IO CONNECT packet (40) with auth payload if token passed via auth
  const authPayload = !useHeader && token ? JSON.stringify({ token }) : '';
  const connectBody = `40${authPayload}`;

  await fetch(`${BASE_URL}/socket.io/?EIO=4&transport=polling&sid=${sid}`, {
    method: 'POST',
    headers: {
      ...headers,
      'Content-Type': 'text/plain;charset=UTF-8',
    },
    body: connectBody,
  });

  // Step 3: Poll response packet(s) from Socket.IO
  const pollRes = await fetch(`${BASE_URL}/socket.io/?EIO=4&transport=polling&sid=${sid}`, { headers });
  const pollText = await pollRes.text();

  return { sid, rawResponse: pollText };
}

async function runVerification() {
  console.log('=== STARTING TASK 4.1 VERIFICATION ===\n');
  let exitCode = 0;

  try {
    // 1. Verify Redis PING -> PONG
    console.log('[Test 1] Verifying Redis connection (PING -> PONG)...');
    const pong = await redisConnection.ping();
    if (pong !== 'PONG') {
      throw new Error(`Expected PONG from Redis, received: ${pong}`);
    }
    console.log(`  PASS: Redis ping returned "${pong}"\n`);

    // Start Hybrid HTTP + WebSocket Server
    await startServer(TEST_PORT);

    // Prepare test users (User A and User B)
    const timestamp = Date.now();
    const userACredentials = {
      name: 'Tenant A User',
      email: `tenantA_${timestamp}@pulsequeue.test`,
      password: 'Password123!',
    };
    const userBCredentials = {
      name: 'Tenant B User',
      email: `tenantB_${timestamp}@pulsequeue.test`,
      password: 'Password123!',
    };

    const regARes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(userACredentials),
    });
    const userAData = await regARes.json();
    if (regARes.status !== 201 || !userAData.token) {
      throw new Error(`User A registration failed: ${JSON.stringify(userAData)}`);
    }

    const regBRes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(userBCredentials),
    });
    const userBData = await regBRes.json();
    if (regBRes.status !== 201 || !userBData.token) {
      throw new Error(`User B registration failed: ${JSON.stringify(userBData)}`);
    }

    // Verify login route works
    const loginARes = await fetch(`${BASE_URL}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: userACredentials.email, password: userACredentials.password }),
    });
    const loginAData = await loginARes.json();
    if (loginARes.status !== 200 || !loginAData.token) {
      throw new Error(`User A login failed: ${JSON.stringify(loginAData)}`);
    }

    // 2. Verify Unauthenticated WebSocket connection rejection
    console.log('[Test 2] Attempting unauthenticated WebSocket connection...');
    const unauthSocket = await connectSocketIO({});
    if (!unauthSocket.rawResponse.includes('44') || !unauthSocket.rawResponse.includes('Authentication error')) {
      throw new Error(`Expected unauthenticated socket rejection (44 + Authentication error), got: ${unauthSocket.rawResponse}`);
    }
    console.log(`  PASS: Unauthenticated WebSocket rejected with packet: ${unauthSocket.rawResponse}\n`);

    // 3. Verify Authenticated WebSocket connection joining tenant:<userId> room
    console.log('[Test 3] Connecting authenticated WebSocket for User A...');
    const authSocket = await connectSocketIO({ token: userAData.token });
    const expectedRoom = `tenant:${userAData._id}`;
    const roomSet = io.sockets.adapter.rooms.get(expectedRoom);

    if (!authSocket.rawResponse.includes('40{') || !roomSet || roomSet.size === 0) {
      throw new Error(
        `Authenticated socket failed to join ${expectedRoom}. Raw response: ${authSocket.rawResponse}`
      );
    }
    console.log(`  PASS: Authenticated WebSocket connected and joined room "${expectedRoom}" (active sockets in room: ${roomSet.size})`);
    console.log(`  PASS: Socket handshake response: ${authSocket.rawResponse}\n`);

    // 4. Verify POST /api/jobs/batch returns HTTP 202 with jobId in < 100ms
    console.log('[Test 4] Dispatching POST /api/jobs/batch and measuring producer latency...');
    // Warmup Redis/BullMQ scripts once so connection pool & Lua scripts are cached
    const warmupJob = await batchQueue.add('WARMUP', { userId: userAData._id, jobType: 'WARMUP', itemCount: 1 });
    await warmupJob.remove();

    const startHr = process.hrtime.bigint();
    const batchRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userAData.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 10,
      }),
    });
    const elapsedMs = Number(process.hrtime.bigint() - startHr) / 1e6;
    const batchBody = await batchRes.json();

    if (batchRes.status !== 202 || !batchBody.jobId || batchBody.status !== 'queued') {
      throw new Error(`Expected HTTP 202 with queued jobId, got ${batchRes.status}: ${JSON.stringify(batchBody)}`);
    }
    if (elapsedMs >= 100) {
      throw new Error(`Expected POST /api/jobs/batch latency < 100ms, got ${elapsedMs.toFixed(2)}ms`);
    }
    console.log(`  PASS: POST /api/jobs/batch returned HTTP ${batchRes.status} in ${elapsedMs.toFixed(2)}ms (< 100ms)`);
    console.log(`  PASS: Response payload: ${JSON.stringify(batchBody)}`);

    // Also verify invalid itemCount rejection (failure handling)
    const invalidRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userAData.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 250,
      }),
    });
    if (invalidRes.status !== 400) {
      throw new Error(`Expected HTTP 400 for out-of-range itemCount=250, got ${invalidRes.status}`);
    }
    console.log(`  PASS: Out-of-range itemCount (250) rejected with HTTP ${invalidRes.status}\n`);

    // 5. Verify GET /api/jobs/:id returns waiting state and cross-tenant 404 isolation
    console.log('[Test 5] Verifying GET /api/jobs/:id status & cross-tenant 404 isolation...');
    const statusResA = await fetch(`${BASE_URL}/api/jobs/${batchBody.jobId}`, {
      headers: {
        Authorization: `Bearer ${userAData.token}`,
      },
    });
    const statusBodyA = await statusResA.json();
    if (statusResA.status !== 200 || statusBodyA.state !== 'waiting') {
      throw new Error(`Expected HTTP 200 with state 'waiting', got ${statusResA.status}: ${JSON.stringify(statusBodyA)}`);
    }
    console.log(`  PASS: Owner (User A) GET /api/jobs/${batchBody.jobId} -> HTTP ${statusResA.status}: ${JSON.stringify(statusBodyA)}`);

    const statusResB = await fetch(`${BASE_URL}/api/jobs/${batchBody.jobId}`, {
      headers: {
        Authorization: `Bearer ${userBData.token}`,
      },
    });
    const statusBodyB = await statusResB.json();
    if (statusResB.status !== 404) {
      throw new Error(`Expected HTTP 404 for cross-tenant User B access, got ${statusResB.status}: ${JSON.stringify(statusBodyB)}`);
    }
    console.log(`  PASS: Cross-tenant (User B) GET /api/jobs/${batchBody.jobId} -> HTTP ${statusResB.status}: ${JSON.stringify(statusBodyB)}\n`);

    // Clean up test artifacts
    await User.deleteMany({ email: { $in: [userACredentials.email, userBCredentials.email] } });
    const createdJob = await batchQueue.getJob(batchBody.jobId);
    if (createdJob) {
      await createdJob.remove();
    }

    console.log('=== ALL TASK 4.1 VERIFICATION CHECKS PASSED ===');
  } catch (error) {
    console.error('VERIFICATION FAILED:', error);
    exitCode = 1;
  } finally {
    io.close();
    server.close();
    await batchQueue.close();
    await redisConnection.quit();
    await mongoose.connection.close();
    process.exit(exitCode);
  }
}

runVerification();
