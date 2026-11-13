require('dotenv').config();
const { Worker } = require('bullmq');
const redisConnection = require('../config/redis');
const connectDB = require('../config/db');
const FailedJob = require('../models/FailedJob');

// Dedicated Redis connection for the blocking BullMQ Worker consumer
const workerConnection = redisConnection.duplicate({
  maxRetriesPerRequest: null,
});

workerConnection.on('error', (err) => {
  console.error('Worker Redis connection error:', err.message);
});

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const processorFn = async (job) => {
  const {
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
    await job.updateProgress(percent);
  }

  return {
    jobId: job.id,
    processedItems: totalItems,
    status: 'completed',
    completedAt: new Date().toISOString(),
  };
};

const batchWorker = new Worker('batch-processor', processorFn, {
  connection: workerConnection,
  concurrency: 5,
});

batchWorker.on('failed', async (job, err) => {
  try {
    if (!job) return;
    const maxAttempts = (job.opts && job.opts.attempts) || 3;

    if (job.attemptsMade >= maxAttempts) {
      await connectDB();
      const quarantinedJob = await FailedJob.findOneAndUpdate(
        { jobId: String(job.id) },
        {
          jobId: String(job.id),
          userId: job.data.userId,
          jobType: job.data.jobType || job.name || 'BULK_INVOICE_SYNC',
          payload: job.data,
          failedReason: (err && err.message) || job.failedReason || 'Unknown worker failure',
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

      batchWorker.emit('dlq:persisted', quarantinedJob);
    }
  } catch (dlqError) {
    console.error(`Failed to persist exhausted job ${job?.id} to DLQ:`, dlqError.message);
  }
});

if (require.main === module) {
  connectDB()
    .then(() => {
      console.log('BullMQ batchWorker started and listening on queue "batch-processor" (concurrency: 5)');
    })
    .catch((err) => {
      console.error('Worker failed to connect to MongoDB:', err.message);
      process.exit(1);
    });
}

batchWorker.batchWorker = batchWorker;
batchWorker.workerConnection = workerConnection;
batchWorker.processorFn = processorFn;

module.exports = batchWorker;
