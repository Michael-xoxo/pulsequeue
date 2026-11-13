require('dotenv').config();
const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const redisConnection = require('../config/redis');
const connectDB = require('../config/db');
const FailedJob = require('../models/FailedJob');
const { CHANNEL_NAME } = require('../socket/queueBridge');

// Dedicated Redis connection for the blocking BullMQ Worker consumer
const workerConnection = redisConnection.duplicate({
  maxRetriesPerRequest: null,
});

workerConnection.on('error', (err) => {
  console.error('Worker Redis connection error:', err.message);
});

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const pendingLifecycleTasks = new Set();

const trackTask = (promise) => {
  pendingLifecycleTasks.add(promise);
  promise.finally(() => pendingLifecycleTasks.delete(promise));
  return promise;
};

const publishLifecycleEvent = async (userId, event, payload) => {
  if (!userId) return;
  await redisConnection.publish(
    CHANNEL_NAME,
    JSON.stringify({
      event,
      userId: String(userId),
      payload,
    })
  );
};

let activeJobsCount = 0;
let maxObservedConcurrency = 0;

const processorFn = async (job) => {
  activeJobsCount += 1;
  if (activeJobsCount > maxObservedConcurrency) {
    maxObservedConcurrency = activeJobsCount;
  }
  batchWorker.activeCount = activeJobsCount;
  batchWorker.maxActiveCount = maxObservedConcurrency;

  try {
    const {
      userId,
      jobType = 'BULK_INVOICE_SYNC',
      itemCount = 1,
      simulateTransientFail = false,
      simulateFatalFail = false,
    } = job.data || {};

    // Chaos Hook 1: Fatal poison-pill payload fails on every attempt until retries exhaust
    if (simulateFatalFail === true) {
      throw new Error('Simulated fatal poison-pill payload');
    }

    // Chaos Hook 2: Transient failure fails on attempt 1 and recovers on attempt 2
    // Note: In BullMQ v6, job.attemptsMade is 0 on Attempt 1 and 1 on Attempt 2 during processor execution
    const currentAttempt = (job.attemptsMade ?? 0) + 1;
    if (simulateTransientFail === true && currentAttempt < 2) {
      throw new Error('Simulated transient downstream timeout');
    }

    const totalItems = Math.max(1, Number(itemCount) || 1);
    const steps = Math.min(totalItems, 5);
    const msPerStep = Math.round((totalItems * 50) / steps);

    for (let step = 1; step <= steps; step += 1) {
      await delay(msPerStep);
      const percent = Math.round((step / steps) * 100);

      await Promise.all([
        job.updateProgress(percent),
        publishLifecycleEvent(userId, 'job:progress', {
          jobId: String(job.id),
          jobType,
          progress: percent,
          timestamp: new Date().toISOString(),
        }),
      ]);
    }

    return {
      jobId: String(job.id),
      processedItems: totalItems,
      status: 'completed',
      completedAt: new Date().toISOString(),
    };
  } finally {
    activeJobsCount = Math.max(0, activeJobsCount - 1);
    batchWorker.activeCount = activeJobsCount;
  }
};

const batchWorker = new Worker('batch-processor', processorFn, {
  connection: workerConnection,
  concurrency: 5,
});

batchWorker.activeCount = 0;
batchWorker.maxActiveCount = 0;

batchWorker.on('completed', (job, result) => {
  trackTask(
    (async () => {
      try {
        if (!job || !job.data) return;
        await publishLifecycleEvent(job.data.userId, 'job:completed', {
          jobId: String(job.id),
          jobType: job.data.jobType || job.name || 'BULK_INVOICE_SYNC',
          result,
          timestamp: new Date().toISOString(),
        });
      } catch (err) {
        console.error(`Failed to publish job:completed event for ${job?.id}:`, err.message);
      }
    })()
  );
});

batchWorker.on('failed', (job, err) => {
  trackTask(
    (async () => {
      try {
        if (!job) return;
        const maxAttempts = (job.opts && job.opts.attempts) || 3;
        const isDeadLetter = job.attemptsMade >= maxAttempts;
        const failedReason = (err && err.message) || job.failedReason || 'Unknown worker failure';
        const jobType = (job.data && job.data.jobType) || job.name || 'BULK_INVOICE_SYNC';

        let quarantinedJob = null;
        if (isDeadLetter) {
          await connectDB();
          quarantinedJob = await FailedJob.findOneAndUpdate(
            { jobId: String(job.id) },
            {
              jobId: String(job.id),
              userId: job.data.userId,
              jobType,
              payload: job.data,
              failedReason,
              stacktrace:
                Array.isArray(job.stacktrace) && job.stacktrace.length > 0
                  ? job.stacktrace
                  : err && err.stack
                  ? [err.stack]
                  : [],
              attemptsMade: job.attemptsMade,
              failedAt: new Date(),
            },
            { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
          );
        }

        if (job.data && job.data.userId) {
          await publishLifecycleEvent(job.data.userId, 'job:failed', {
            jobId: String(job.id),
            jobType,
            failedReason,
            attemptsMade: job.attemptsMade,
            isDeadLetter,
            timestamp: new Date().toISOString(),
          });
        }

        if (quarantinedJob) {
          batchWorker.emit('dlq:persisted', quarantinedJob);
        }
      } catch (dlqError) {
        console.error(`Failed to handle failed job ${job?.id}:`, dlqError.message);
      }
    })()
  );
});

let workerClosed = false;

const startWorker = async () => {
  await connectDB();
  await batchWorker.waitUntilReady();
  if (batchWorker.isPaused()) {
    batchWorker.resume();
  }
  console.log('BullMQ batchWorker ready and listening on queue "batch-processor" (concurrency: 5)');
  return batchWorker;
};

const closeWorker = async ({ quitRedis = false } = {}) => {
  if (!workerClosed) {
    workerClosed = true;
    await batchWorker.close();
  }
  if (pendingLifecycleTasks.size > 0) {
    await Promise.allSettled(Array.from(pendingLifecycleTasks));
  }
  if (quitRedis && workerConnection.status !== 'end') {
    await workerConnection.quit().catch(() => {});
  }
};

if (require.main === module) {
  startWorker().catch((err) => {
    console.error('Worker failed to start:', err.message);
    process.exit(1);
  });

  const handleWorkerSignal = async (signal) => {
    console.log(`Received ${signal}. Draining active BullMQ jobs gracefully...`);
    try {
      await closeWorker({ quitRedis: true });
      if (redisConnection.status !== 'end') {
        await redisConnection.quit().catch(() => {});
      }
      if (mongoose.connection.readyState !== 0) {
        await mongoose.connection.close().catch(() => {});
      }
      process.exit(0);
    } catch (err) {
      console.error('Error during worker graceful shutdown:', err.message);
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => handleWorkerSignal('SIGTERM'));
  process.on('SIGINT', () => handleWorkerSignal('SIGINT'));
}

batchWorker.batchWorker = batchWorker;
batchWorker.workerConnection = workerConnection;
batchWorker.processorFn = processorFn;
batchWorker.startWorker = startWorker;
batchWorker.closeWorker = closeWorker;

module.exports = batchWorker;
