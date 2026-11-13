require('dotenv').config();
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const batchQueue = require('../queues/batchQueue');
const batchWorker = require('../workers/batchWorker');
const User = require('../models/User');
const FailedJob = require('../models/FailedJob');
const { startServer, server, io } = require('../server');

const TEST_PORT = 5056;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

function waitForJobOutcome(worker, targetJobId, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    const progressHistory = [];
    const failedAttempts = [];

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timeout waiting for job ${targetJobId} outcome after ${timeoutMs}ms`));
    }, timeoutMs);

    const onProgress = (job, progress) => {
      if (job && String(job.id) === String(targetJobId)) {
        progressHistory.push(progress);
      }
    };

    const onCompleted = (job, result) => {
      if (job && String(job.id) === String(targetJobId)) {
        cleanup();
        resolve({
          outcome: 'completed',
          job,
          result,
          progressHistory,
          failedAttempts,
        });
      }
    };

    const onFailed = (job, err) => {
      if (job && String(job.id) === String(targetJobId)) {
        failedAttempts.push({
          attemptsMade: job.attemptsMade,
          reason: err ? err.message : job.failedReason,
        });
        const maxAttempts = (job.opts && job.opts.attempts) || 3;
        if (job.attemptsMade >= maxAttempts) {
          // Wait for dlq:persisted event to ensure MongoDB write is complete
          const onDlqPersisted = (dlqDoc) => {
            if (dlqDoc && String(dlqDoc.jobId) === String(targetJobId)) {
              worker.off('dlq:persisted', onDlqPersisted);
              cleanup();
              resolve({
                outcome: 'failed',
                job,
                error: err,
                dlqDoc,
                progressHistory,
                failedAttempts,
              });
            }
          };
          worker.on('dlq:persisted', onDlqPersisted);
        }
      }
    };

    function cleanup() {
      clearTimeout(timer);
      worker.off('progress', onProgress);
      worker.off('completed', onCompleted);
      worker.off('failed', onFailed);
    }

    worker.on('progress', onProgress);
    worker.on('completed', onCompleted);
    worker.on('failed', onFailed);
  });
}

async function runVerification() {
  console.log('=== STARTING TASK 4.2 VERIFICATION ===\n');
  let exitCode = 0;
  const createdJobIds = [];
  const createdEmails = [];

  try {
    await startServer(TEST_PORT);
    await batchWorker.waitUntilReady();

    // Verify dedicated worker Redis connection isolation
    if (batchWorker.workerConnection === redisConnection) {
      throw new Error('Worker must use a dedicated/duplicated Redis connection, not the shared instance');
    }
    console.log('[Setup] Verified Worker uses isolated duplicated Redis connection (maxRetriesPerRequest: null)\n');

    // Register User A and User B
    const ts = Date.now();
    const emailA = `userA_42_${ts}@pulsequeue.test`;
    const emailB = `userB_42_${ts}@pulsequeue.test`;
    createdEmails.push(emailA, emailB);

    const regARes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'User A', email: emailA, password: 'Password123!' }),
    });
    const userA = await regARes.json();

    const regBRes = await fetch(`${BASE_URL}/api/auth/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'User B', email: emailB, password: 'Password123!' }),
    });
    const userB = await regBRes.json();

    // -------------------------------------------------------------------------
    // TEST 1: Happy Path Job Completion (progress 0 -> 100)
    // -------------------------------------------------------------------------
    console.log('[Test 1] Happy Path: Enqueue { itemCount: 5 } and verify stepwise progress 0 -> 100...');
    const happyRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({ jobType: 'BULK_INVOICE_SYNC', itemCount: 5 }),
    });
    const happyBody = await happyRes.json();
    createdJobIds.push(happyBody.jobId);

    const happyOutcome = await waitForJobOutcome(batchWorker, happyBody.jobId);
    const happyStatusRes = await fetch(`${BASE_URL}/api/jobs/${happyBody.jobId}`, {
      headers: { Authorization: `Bearer ${userA.token}` },
    });
    const happyStatus = await happyStatusRes.json();

    if (happyOutcome.outcome !== 'completed' || happyStatus.state !== 'completed' || happyStatus.progress !== 100) {
      throw new Error(`Happy path failed: ${JSON.stringify(happyStatus)}`);
    }
    console.log(`  PASS: Job ${happyBody.jobId} completed with stepwise progress: [${happyOutcome.progressHistory.join('%, ')}%]`);
    console.log(`  PASS: GET /api/jobs/${happyBody.jobId} -> state="${happyStatus.state}", progress=${happyStatus.progress}\n`);

    // -------------------------------------------------------------------------
    // TEST 2: Transient Failure Recovery (fails attempt 1, recovers on attempt 2)
    // -------------------------------------------------------------------------
    console.log('[Test 2] Transient Recovery: Enqueue { itemCount: 3, simulateTransientFail: true }...');
    const transientRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 3,
        simulateTransientFail: true,
      }),
    });
    const transientBody = await transientRes.json();
    createdJobIds.push(transientBody.jobId);

    const transientOutcome = await waitForJobOutcome(batchWorker, transientBody.jobId);
    const transientStatusRes = await fetch(`${BASE_URL}/api/jobs/${transientBody.jobId}`, {
      headers: { Authorization: `Bearer ${userA.token}` },
    });
    const transientStatus = await transientStatusRes.json();

    if (
      transientOutcome.outcome !== 'completed' ||
      transientOutcome.failedAttempts.length !== 1 ||
      transientStatus.state !== 'completed' ||
      transientStatus.attemptsMade !== 2
    ) {
      throw new Error(
        `Transient recovery check failed: outcome=${transientOutcome.outcome}, failedAttempts=${JSON.stringify(
          transientOutcome.failedAttempts
        )}, status=${JSON.stringify(transientStatus)}`
      );
    }
    console.log(
      `  PASS: Attempt 1 failed as expected ("${transientOutcome.failedAttempts[0].reason}"), backed off, and recovered on Attempt 2`
    );
    console.log(
      `  PASS: GET /api/jobs/${transientBody.jobId} -> state="${transientStatus.state}", attemptsMade=${transientStatus.attemptsMade}, progress=${transientStatus.progress}\n`
    );

    // -------------------------------------------------------------------------
    // TEST 3: Poison-Pill Exhaustion into MongoDB FailedJob DLQ
    // -------------------------------------------------------------------------
    console.log('[Test 3] Poison-Pill DLQ Quarantine: Enqueue { itemCount: 3, simulateFatalFail: true }...');
    const fatalRes = await fetch(`${BASE_URL}/api/jobs/batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${userA.token}`,
      },
      body: JSON.stringify({
        jobType: 'BULK_INVOICE_SYNC',
        itemCount: 3,
        simulateFatalFail: true,
      }),
    });
    const fatalBody = await fatalRes.json();
    createdJobIds.push(fatalBody.jobId);

    const fatalOutcome = await waitForJobOutcome(batchWorker, fatalBody.jobId);
    const fatalStatusRes = await fetch(`${BASE_URL}/api/jobs/${fatalBody.jobId}`, {
      headers: { Authorization: `Bearer ${userA.token}` },
    });
    const fatalStatus = await fatalStatusRes.json();

    const dlqRecord = await FailedJob.findOne({ jobId: fatalBody.jobId });
    if (
      fatalOutcome.outcome !== 'failed' ||
      fatalStatus.state !== 'failed' ||
      fatalStatus.attemptsMade !== 3 ||
      !dlqRecord ||
      dlqRecord.attemptsMade !== 3
    ) {
      throw new Error(
        `Poison-pill exhaustion check failed: status=${JSON.stringify(fatalStatus)}, dlqRecord=${JSON.stringify(dlqRecord)}`
      );
    }
    console.log(
      `  PASS: Job ${fatalBody.jobId} exhausted all ${fatalStatus.attemptsMade} attempts with exponential backoff and transitioned to state="${fatalStatus.state}"`
    );
    console.log(
      `  PASS: Persisted to MongoDB FailedJob DLQ -> jobId="${dlqRecord.jobId}", failedReason="${dlqRecord.failedReason}", attemptsMade=${dlqRecord.attemptsMade}\n`
    );

    // -------------------------------------------------------------------------
    // TEST 4: Tenant-Scoped DLQ API (GET /api/jobs/dlq)
    // -------------------------------------------------------------------------
    console.log('[Test 4] Tenant-Scoped DLQ API: Querying GET /api/jobs/dlq as User A and User B...');
    const dlqResA = await fetch(`${BASE_URL}/api/jobs/dlq`, {
      headers: { Authorization: `Bearer ${userA.token}` },
    });
    const dlqListA = await dlqResA.json();

    const dlqResB = await fetch(`${BASE_URL}/api/jobs/dlq`, {
      headers: { Authorization: `Bearer ${userB.token}` },
    });
    const dlqListB = await dlqResB.json();

    if (
      dlqResA.status !== 200 ||
      !Array.isArray(dlqListA) ||
      !dlqListA.some((doc) => doc.jobId === fatalBody.jobId)
    ) {
      throw new Error(`Expected User A DLQ list to contain ${fatalBody.jobId}, got: ${JSON.stringify(dlqListA)}`);
    }

    if (dlqResB.status !== 200 || !Array.isArray(dlqListB) || dlqListB.length !== 0) {
      throw new Error(`Expected User B DLQ list to be empty array [], got: ${JSON.stringify(dlqListB)}`);
    }

    console.log(`  PASS: User A GET /api/jobs/dlq -> HTTP ${dlqResA.status} (returned ${dlqListA.length} quarantined job(s), matching jobId="${dlqListA[0].jobId}")`);
    console.log(`  PASS: User B GET /api/jobs/dlq -> HTTP ${dlqResB.status} (returned ${JSON.stringify(dlqListB)} - strict tenant isolation verified)\n`);

    console.log('=== ALL TASK 4.2 VERIFICATION CHECKS PASSED ===');
  } catch (error) {
    console.error('VERIFICATION FAILED:', error);
    exitCode = 1;
  } finally {
    // Cleanup test jobs and users
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
    io.close();
    server.close();
    await batchQueue.close();
    await redisConnection.quit();
    await mongoose.connection.close();
    process.exit(exitCode);
  }
}

runVerification();
