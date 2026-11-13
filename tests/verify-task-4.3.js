require('dotenv').config();
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const batchQueue = require('../queues/batchQueue');
const batchWorker = require('../workers/batchWorker');
const User = require('../models/User');
const FailedJob = require('../models/FailedJob');
const { startServer, closeServer } = require('../server');

const TEST_PORT = 5057;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;
const WS_URL = `ws://127.0.0.1:${TEST_PORT}/socket.io/?EIO=4&transport=websocket`;

/**
 * Pure WebSocket Socket.IO v4 client (zero HTTP polling)
 */
function createSocketClient(token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const events = [];
    const listeners = [];
    let connected = false;

    const timeout = setTimeout(() => {
      if (!connected) {
        ws.close();
        reject(new Error('WebSocket handshake timed out'));
      }
    }, 10000);

    ws.onmessage = (msgEvent) => {
      const raw = String(msgEvent.data);

      // Engine.IO OPEN packet -> send Socket.IO CONNECT with JWT auth token
      if (raw.startsWith('0{')) {
        ws.send(`40${JSON.stringify({ token })}`);
        return;
      }

      // Engine.IO PING -> reply PONG
      if (raw === '2') {
        ws.send('3');
        return;
      }

      // Socket.IO CONNECT_ERROR
      if (raw.startsWith('44')) {
        clearTimeout(timeout);
        reject(new Error(`Socket authentication failed: ${raw}`));
        return;
      }

      // Socket.IO EVENT packet: 42["eventName", payload]
      if (raw.startsWith('42[')) {
        try {
          const [eventName, payload] = JSON.parse(raw.slice(2));
          if (eventName === 'tenant:joined' && !connected) {
            connected = true;
            clearTimeout(timeout);
            resolve({
              ws,
              events,
              waitForEvent(predicate, waitMs = 25000) {
                const existing = events.find(predicate);
                if (existing) return Promise.resolve(existing);

                return new Promise((resWait, rejWait) => {
                  const t = setTimeout(() => {
                    rejWait(new Error(`Timed out waiting for socket event after ${waitMs}ms`));
                  }, waitMs);

                  listeners.push((evt) => {
                    if (predicate(evt)) {
                      clearTimeout(t);
                      resWait(evt);
                      return true;
                    }
                    return false;
                  });
                });
              },
              close() {
                ws.close();
              },
            });
            return;
          }

          const record = { event: eventName, payload, receivedAt: Date.now() };
          events.push(record);

          for (let i = listeners.length - 1; i >= 0; i -= 1) {
            if (listeners[i](record)) {
              listeners.splice(i, 1);
            }
          }
        } catch (_) {}
      }
    };

    ws.onerror = (err) => {
      clearTimeout(timeout);
      reject(err);
    };
  });
}

async function runVerification() {
  console.log('=== STARTING TASK 4.3 VERIFICATION ===\n');
  let exitCode = 0;
  const createdJobIds = [];
  const createdEmails = [];
  let clientA = null;
  let clientB = null;

  try {
    await startServer(TEST_PORT);
    await batchWorker.waitUntilReady();

    // Register User A and User B
    const ts = Date.now();
    const emailA = `userA_43_${ts}@pulsequeue.test`;
    const emailB = `userB_43_${ts}@pulsequeue.test`;
    createdEmails.push(emailA, emailB);

    const regARes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Tenant A', email: emailA, password: 'Password123!' }),
    });
    const userA = await regARes.json();

    const regBRes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Tenant B', email: emailB, password: 'Password123!' }),
    });
    const userB = await regBRes.json();

    // Connect both User A and User B simultaneously via pure WebSockets
    console.log('[Setup] Connecting User A and User B simultaneously via WebSocket...');
    clientA = await createSocketClient(userA.token);
    clientB = await createSocketClient(userB.token);
    console.log(`  PASS: User A connected to room "tenant:${userA._id}"`);
    console.log(`  PASS: User B connected to room "tenant:${userB._id}"\n`);

    // -------------------------------------------------------------------------
    // TEST 1: Real-Time Progress Stream (25, 50, 75, 100) + job:completed
    // -------------------------------------------------------------------------
    console.log('[Test 1] Enqueuing { itemCount: 4 } for User A and listening via WebSocket (0 HTTP polls)...');
    const batchRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 4,
      }),
    });
    const batchBody = await batchRes.json();
    createdJobIds.push(batchBody.jobId);

    // Wait for job:completed over WebSocket
    const completedEvt = await clientA.waitForEvent(
      (e) => e.event === 'job:completed' && e.payload.jobId === batchBody.jobId
    );

    const progressEvents = clientA.events
      .filter((e) => e.event === 'job:progress' && e.payload.jobId === batchBody.jobId)
      .map((e) => e.payload.progress);

    if (
      progressEvents.length !== 4 ||
      progressEvents[0] !== 25 ||
      progressEvents[1] !== 50 ||
      progressEvents[2] !== 75 ||
      progressEvents[3] !== 100
    ) {
      throw new Error(`Expected sequential progress [25, 50, 75, 100], received: ${JSON.stringify(progressEvents)}`);
    }

    console.log(`  PASS: User A received live WebSocket job:progress ticks: [${progressEvents.join('%, ')}%]`);
    console.log(`  PASS: User A received live WebSocket job:completed: ${JSON.stringify(completedEvt.payload)}\n`);

    // -------------------------------------------------------------------------
    // TEST 2: Zero Cross-Tenant Leakage for User B
    // -------------------------------------------------------------------------
    console.log('[Test 2] Verifying Zero Cross-Tenant Socket Leakage on User B...');
    const leakedToB = clientB.events.filter((e) =>
      ['job:progress', 'job:completed', 'job:failed'].includes(e.event)
    );
    if (leakedToB.length !== 0) {
      throw new Error(`Cross-tenant leak detected! User B received ${leakedToB.length} events: ${JSON.stringify(leakedToB)}`);
    }
    console.log(`  PASS: User B received 0 job events during User A's batch execution\n`);

    // -------------------------------------------------------------------------
    // TEST 3: Real-Time DLQ Alert (job:failed with isDeadLetter: true)
    // -------------------------------------------------------------------------
    console.log('[Test 3] Enqueuing { itemCount: 2, simulateFatalFail: true } for User A...');
    const fatalRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 2,
        simulateFatalFail: true,
      }),
    });
    const fatalBody = await fatalRes.json();
    createdJobIds.push(fatalBody.jobId);

    const dlqFailedEvt = await clientA.waitForEvent(
      (e) =>
        e.event === 'job:failed' &&
        e.payload.jobId === fatalBody.jobId &&
        e.payload.isDeadLetter === true
    );

    const allFatalEventsForJob = clientA.events.filter(
      (e) => e.event === 'job:failed' && e.payload.jobId === fatalBody.jobId
    );

    if (
      dlqFailedEvt.payload.attemptsMade !== 3 ||
      dlqFailedEvt.payload.isDeadLetter !== true ||
      !dlqFailedEvt.payload.failedReason.includes('Simulated fatal poison-pill payload')
    ) {
      throw new Error(`Invalid DLQ job:failed payload: ${JSON.stringify(dlqFailedEvt.payload)}`);
    }

    if (clientB.events.length !== 0) {
      throw new Error(`Cross-tenant leak on job:failed! User B received: ${JSON.stringify(clientB.events)}`);
    }

    console.log(
      `  PASS: User A received ${allFatalEventsForJob.length} job:failed WebSocket events; final event has attemptsMade=${dlqFailedEvt.payload.attemptsMade}, isDeadLetter=${dlqFailedEvt.payload.isDeadLetter}`
    );
    console.log(`  PASS: Final job:failed payload: ${JSON.stringify(dlqFailedEvt.payload)}`);
    console.log(`  PASS: User B still received 0 events\n`);

    // -------------------------------------------------------------------------
    // TEST 4: State Hydration Endpoint (GET /api/jobs)
    // -------------------------------------------------------------------------
    console.log('[Test 4] Verifying State Hydration Endpoint (GET /api/jobs) for User A and User B...');
    const listARes = await fetch(`${BASE_URL}/api/jobs`, {
      headers: { Authorization: `Bearer ${userA.token}` },
    });
    const listABody = await listARes.json();

    const listBRes = await fetch(`${BASE_URL}/api/jobs`, {
      headers: { Authorization: `Bearer ${userB.token}` },
    });
    const listBBody = await listBRes.json();

    const hasCompletedJob = listABody.some(
      (j) => j.jobId === batchBody.jobId && j.state === 'completed'
    );
    const hasFailedJob = listABody.some(
      (j) => j.jobId === fatalBody.jobId && j.state === 'failed'
    );

    if (listARes.status !== 200 || !hasCompletedJob || !hasFailedJob) {
      throw new Error(`User A GET /api/jobs missing expected jobs: ${JSON.stringify(listABody)}`);
    }

    if (listBRes.status !== 200 || !Array.isArray(listBBody) || listBBody.length !== 0) {
      throw new Error(`Expected User B GET /api/jobs to return [], got: ${JSON.stringify(listBBody)}`);
    }

    console.log(
      `  PASS: User A GET /api/jobs -> HTTP ${listARes.status} (hydrated ${listABody.length} jobs: completed="${batchBody.jobId}", failed="${fatalBody.jobId}")`
    );
    console.log(
      `  PASS: User B GET /api/jobs -> HTTP ${listBRes.status} (returned ${JSON.stringify(listBBody)} - strict tenant isolation verified)\n`
    );

    console.log('=== ALL TASK 4.3 VERIFICATION CHECKS PASSED ===');
  } catch (error) {
    console.error('VERIFICATION FAILED:', error);
    exitCode = 1;
  } finally {
    if (clientA) clientA.close();
    if (clientB) clientB.close();

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
