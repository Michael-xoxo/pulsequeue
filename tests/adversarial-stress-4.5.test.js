require('dotenv').config();
process.env.NODE_ENV = 'production';
process.env.EMBED_WORKER = 'true';

const path = require('path');
const { createRequire } = require('module');
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const batchQueue = require('../queues/batchQueue');
const batchWorker = require('../workers/batchWorker');
const User = require('../models/User');
const FailedJob = require('../models/FailedJob');
const { startServer, closeServer } = require('../server');

const clientRequire = createRequire(path.join(__dirname, '../client/package.json'));
const { io: createClientSocket } = clientRequire('socket.io-client');

const TEST_PORT = 5065;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

function connectTenantSocket(token) {
  return new Promise((resolve, reject) => {
    const progressEvents = [];
    const completedEvents = [];
    const failedEvents = [];
    const completionWaiters = [];

    const timer = setTimeout(() => {
      reject(new Error('Socket connection timed out'));
    }, 10000);

    const socket = createClientSocket(BASE_URL, {
      auth: { token },
      transports: ['websocket', 'polling'],
    });

    socket.on('tenant:joined', (info) => {
      clearTimeout(timer);
      resolve({
        socket,
        room: info.room,
        progressEvents,
        completedEvents,
        failedEvents,
        waitForCompletedCount(targetCount, timeoutMs = 60000) {
          if (completedEvents.length >= targetCount) {
            return Promise.resolve(completedEvents);
          }
          return new Promise((resWait, rejWait) => {
            const t = setTimeout(() => {
              rejWait(
                new Error(
                  `Timeout waiting for ${targetCount} completed events (received ${completedEvents.length})`
                )
              );
            }, timeoutMs);

            completionWaiters.push(() => {
              if (completedEvents.length >= targetCount) {
                clearTimeout(t);
                resWait(completedEvents);
                return true;
              }
              return false;
            });
          });
        },
      });
    });

    socket.on('connect_error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    socket.on('job:progress', (payload) => {
      progressEvents.push(payload);
    });

    socket.on('job:completed', (payload) => {
      completedEvents.push(payload);
      for (let i = completionWaiters.length - 1; i >= 0; i -= 1) {
        if (completionWaiters[i]()) {
          completionWaiters.splice(i, 1);
        }
      }
    });

    socket.on('job:failed', (payload) => {
      failedEvents.push(payload);
    });
  });
}

async function runStressSuite() {
  console.log('=== STARTING TASK 4.5 ADVERSARIAL CONCURRENCY & PRODUCTION STRESS SUITE ===\n');
  let exitCode = 0;
  let socketA = null;
  let socketB = null;
  const createdJobIds = [];
  const createdEmails = [];
  const unhandledRejections = [];

  const onUnhandledRejection = (reason) => {
    unhandledRejections.push(reason);
  };
  process.on('unhandledRejection', onUnhandledRejection);

  try {
    await startServer(TEST_PORT);

    // Register Tenant A and Tenant B
    const ts = Date.now();
    const emailA = `stress_tenantA_${ts}@pulsequeue.test`;
    const emailB = `stress_tenantB_${ts}@pulsequeue.test`;
    createdEmails.push(emailA, emailB);

    const [regARes, regBRes] = await Promise.all([
      fetch(`${BASE_URL}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Stress Tenant A', email: emailA, password: 'Password123!' }),
      }),
      fetch(`${BASE_URL}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Stress Tenant B', email: emailB, password: 'Password123!' }),
      }),
    ]);

    const userA = await regARes.json();
    const userB = await regBRes.json();

    // Connect both tenants over WebSockets simultaneously
    [socketA, socketB] = await Promise.all([
      connectTenantSocket(userA.token),
      connectTenantSocket(userB.token),
    ]);
    console.log(`[Setup] Connected User A (${socketA.room}) & User B (${socketB.room})\n`);

    // =========================================================================
    // TEST 1: High-Concurrency Burst (30 Simultaneous Jobs across 2 Tenants)
    // =========================================================================
    console.log('[Test 1] Firing 30 concurrent POST /api/jobs/batch requests (15 User A + 15 User B)...');
    const burstStart = process.hrtime.bigint();

    const dispatchPromises = [];
    for (let i = 0; i < 15; i += 1) {
      dispatchPromises.push(
        fetch(`${BASE_URL}/api/jobs/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${userA.token}`,
          },
          body: JSON.stringify({ jobType: 'BULK_INVOICE_SYNC', itemCount: 3 }),
        }).then(async (res) => ({ tenant: 'A', status: res.status, body: await res.json() }))
      );

      dispatchPromises.push(
        fetch(`${BASE_URL}/api/jobs/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${userB.token}`,
          },
          body: JSON.stringify({ jobType: 'WEBHOOK_FANOUT', itemCount: 3 }),
        }).then(async (res) => ({ tenant: 'B', status: res.status, body: await res.json() }))
      );
    }

    const burstResults = await Promise.all(dispatchPromises);
    const burstDurationMs = Number(process.hrtime.bigint() - burstStart) / 1e6;

    const tenantAJobIds = new Set();
    const tenantBJobIds = new Set();

    for (const res of burstResults) {
      if (res.status !== 202 || !res.body.jobId) {
        throw new Error(`Expected HTTP 202 on all 30 burst jobs, got ${res.status}: ${JSON.stringify(res.body)}`);
      }
      createdJobIds.push(res.body.jobId);
      if (res.tenant === 'A') tenantAJobIds.add(res.body.jobId);
      if (res.tenant === 'B') tenantBJobIds.add(res.body.jobId);
    }

    console.log(
      `  PASS: All 30 concurrent POST /api/jobs/batch requests returned HTTP 202 (${burstDurationMs.toFixed(1)}ms total burst window)`
    );

    // Wait for all 30 jobs to complete over WebSockets (15 for User A, 15 for User B)
    await Promise.all([
      socketA.waitForCompletedCount(15, 90000),
      socketB.waitForCompletedCount(15, 90000),
    ]);

    // Verify strict tenant room isolation across all progress & completed events
    const leakedProgressToA = socketA.progressEvents.filter((e) => tenantBJobIds.has(e.jobId));
    const leakedCompletedToA = socketA.completedEvents.filter((e) => tenantBJobIds.has(e.jobId));
    const leakedProgressToB = socketB.progressEvents.filter((e) => tenantAJobIds.has(e.jobId));
    const leakedCompletedToB = socketB.completedEvents.filter((e) => tenantAJobIds.has(e.jobId));

    if (
      socketA.completedEvents.length !== 15 ||
      socketB.completedEvents.length !== 15 ||
      leakedProgressToA.length > 0 ||
      leakedCompletedToA.length > 0 ||
      leakedProgressToB.length > 0 ||
      leakedCompletedToB.length > 0
    ) {
      throw new Error(
        `Tenant isolation breach or count mismatch: A=${socketA.completedEvents.length}, B=${socketB.completedEvents.length}`
      );
    }

    if (batchWorker.maxActiveCount > 5 || batchWorker.maxActiveCount < 1) {
      throw new Error(
        `Worker concurrency cap violated: maxActiveCount=${batchWorker.maxActiveCount} (expected 1..5)`
      );
    }

    console.log(
      `  PASS: User A received exactly ${socketA.completedEvents.length} job:completed events and ${socketA.progressEvents.length} progress ticks (0 leaked from User B)`
    );
    console.log(
      `  PASS: User B received exactly ${socketB.completedEvents.length} job:completed events and ${socketB.progressEvents.length} progress ticks (0 leaked from User A)`
    );
    console.log(
      `  PASS: Worker concurrency cap strictly respected under 30-job burst (maxObservedActiveCount=${batchWorker.maxActiveCount} <= 5)\n`
    );

    // =========================================================================
    // TEST 2: Production SPA & API Routing Guard
    // =========================================================================
    console.log('[Test 2] Verifying NODE_ENV=production SPA wildcard fallback & /api/jobs auth guard...');
    const [rootRes, deepLinkRes, unauthApiRes] = await Promise.all([
      fetch(`${BASE_URL}/`),
      fetch(`${BASE_URL}/mission-control/live`),
      fetch(`${BASE_URL}/api/jobs`),
    ]);

    const rootHtml = await rootRes.text();
    const deepLinkHtml = await deepLinkRes.text();

    if (
      rootRes.status !== 200 ||
      !rootHtml.includes('<div id="root"></div>') ||
      deepLinkRes.status !== 200 ||
      !deepLinkHtml.includes('<div id="root"></div>')
    ) {
      throw new Error(
        `Production SPA fallback failed: GET / (${rootRes.status}), GET /mission-control/live (${deepLinkRes.status})`
      );
    }

    if (unauthApiRes.status !== 401) {
      throw new Error(`Expected HTTP 401 on unauthenticated GET /api/jobs, got ${unauthApiRes.status}`);
    }

    console.log('  PASS: GET / returned HTTP 200 HTML containing <div id="root"></div>');
    console.log('  PASS: GET /mission-control/live returned HTTP 200 HTML containing <div id="root"></div>');
    console.log('  PASS: Unauthenticated GET /api/jobs returned HTTP 401 (API guard not shadowed by SPA splat)\n');

    // =========================================================================
    // TEST 3: Graceful Worker Drain (closeWorker() during active in-flight job)
    // =========================================================================
    console.log('[Test 3] Verifying Graceful Worker Drain (closeWorker() on active job)...');
    let resolveActiveStart;
    const activeStartPromise = new Promise((r) => {
      resolveActiveStart = r;
    });

    let drainJobId = null;
    const onProgress = (job) => {
      if (drainJobId && String(job.id) === String(drainJobId)) {
        resolveActiveStart();
      }
    };
    batchWorker.on('progress', onProgress);

    const drainRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({ jobType: 'LEDGER_RECONCILE', itemCount: 6 }),
    });
    const drainBody = await drainRes.json();
    drainJobId = drainBody.jobId;
    createdJobIds.push(drainJobId);

    // Wait until the job is actively mid-execution
    await activeStartPromise;
    batchWorker.off('progress', onProgress);
    console.log(`  INFO: Job ${drainJobId} is actively executing; invoking closeWorker() to test graceful drain...`);

    await batchWorker.closeWorker();

    const drainedJob = await batchQueue.getJob(drainJobId);
    const drainedState = await drainedJob.getState();

    if (drainedState !== 'completed' || drainedJob.progress !== 100) {
      throw new Error(
        `Expected in-flight job ${drainJobId} to drain to 'completed' (100%) before closeWorker() resolved, got state=${drainedState}, progress=${drainedJob.progress}`
      );
    }

    if (unhandledRejections.length > 0) {
      throw new Error(`Unhandled rejections occurred during stress/drain test: ${unhandledRejections.length}`);
    }

    console.log(
      `  PASS: closeWorker() waited for in-flight job ${drainJobId} to finish cleanly (state="${drainedState}", progress=${drainedJob.progress}%)`
    );
    console.log('  PASS: Zero unhandled promise rejections occurred\n');

    console.log('=== ALL TASK 4.5 ADVERSARIAL STRESS & PRODUCTION CHECKS PASSED ===');
  } catch (error) {
    console.error('STRESS SUITE FAILED:', error);
    exitCode = 1;
  } finally {
    process.off('unhandledRejection', onUnhandledRejection);
    if (socketA && socketA.socket) socketA.socket.disconnect();
    if (socketB && socketB.socket) socketB.socket.disconnect();

    for (const jId of createdJobIds) {
      try {
        const j = await batchQueue.getJob(jId);
        if (j) await j.remove();
      } catch (_) {}
    }
    await FailedJob.deleteMany({ jobId: { $in: createdJobIds } }).catch(() => {});
    await User.deleteMany({ email: { $in: createdEmails } }).catch(() => {});

    await closeServer();
    await batchWorker.closeWorker({ quitRedis: true });
    await batchQueue.close();
    if (redisConnection.status !== 'end') {
      await redisConnection.quit().catch(() => {});
    }
    if (mongoose.connection.readyState !== 0) {
      await mongoose.connection.close().catch(() => {});
    }
    process.exit(exitCode);
  }
}

runStressSuite();
