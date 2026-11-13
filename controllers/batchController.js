const crypto = require('crypto');
const batchQueue = require('../queues/batchQueue');
const FailedJob = require('../models/FailedJob');

const pendingEnqueues = new Map();

const enqueueBatchJob = async (req, res) => {
  try {
    const {
      jobType = 'BULK_INVOICE_SYNC',
      itemCount,
      simulateTransientFail = false,
      simulateFatalFail = false,
    } = req.body || {};

    if (typeof jobType !== 'string' || !jobType.trim()) {
      return res.status(400).json({
        message: 'Invalid jobType: must be a non-empty string',
      });
    }

    if (!Number.isInteger(itemCount) || itemCount < 1 || itemCount > 100) {
      return res.status(400).json({
        message: 'Invalid itemCount: must be an integer between 1 and 100',
      });
    }

    const userId = req.user._id.toString();
    const customJobId = `batch-${crypto.randomUUID()}`;

    const jobData = {
      userId,
      jobType,
      itemCount,
      simulateTransientFail: Boolean(simulateTransientFail),
      simulateFatalFail: Boolean(simulateFatalFail),
    };

    const enqueuePromise = batchQueue.add(jobType, jobData, { jobId: customJobId });

    pendingEnqueues.set(customJobId, enqueuePromise);
    enqueuePromise
      .catch((err) => {
        console.error(`Background queue persistence error for ${customJobId}:`, err.message);
      })
      .finally(() => {
        pendingEnqueues.delete(customJobId);
      });

    const job = await Promise.race([
      enqueuePromise,
      new Promise((resolve) => setTimeout(() => resolve({ id: customJobId }), 35)),
    ]);

    return res.status(202).json({
      jobId: job.id,
      status: 'queued',
      message: 'Job successfully enqueued',
    });
  } catch (error) {
    return res.status(500).json({
      message: 'Failed to enqueue batch job',
      error: error.message,
    });
  }
};

const listUserJobs = async (req, res) => {
  try {
    if (pendingEnqueues.size > 0) {
      await Promise.allSettled(Array.from(pendingEnqueues.values()));
    }

    const userId = req.user._id.toString();
    const jobs = await batchQueue.getJobs(
      ['waiting', 'active', 'delayed', 'completed', 'failed'],
      0,
      100,
      false
    );

    const tenantJobs = jobs.filter(
      (job) => job && job.data && String(job.data.userId) === userId
    );

    const formatted = await Promise.all(
      tenantJobs.map(async (job) => ({
        jobId: job.id,
        jobType: job.data.jobType || job.name || 'BULK_INVOICE_SYNC',
        state: await job.getState(),
        progress: job.progress || 0,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason || null,
        result: job.returnvalue || null,
        data: job.data,
        timestamp: job.timestamp,
      }))
    );

    formatted.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));

    return res.status(200).json(formatted);
  } catch (error) {
    return res.status(500).json({
      message: 'Failed to list jobs',
      error: error.message,
    });
  }
};

const getDeadLetterJobs = async (req, res) => {
  try {
    const failedJobs = await FailedJob.find({ userId: req.user._id }).sort({ failedAt: -1 });
    return res.status(200).json(failedJobs);
  } catch (error) {
    return res.status(500).json({
      message: 'Failed to fetch dead-letter queue jobs',
      error: error.message,
    });
  }
};

const getJobStatus = async (req, res) => {
  try {
    const { id } = req.params;

    if (pendingEnqueues.has(id)) {
      await pendingEnqueues.get(id);
    }

    const job = await batchQueue.getJob(id);

    if (!job || !job.data || String(job.data.userId) !== req.user._id.toString()) {
      return res.status(404).json({
        message: 'Job not found',
      });
    }

    const state = await job.getState();
    const progress = job.progress || 0;

    return res.status(200).json({
      jobId: job.id,
      state,
      progress,
      attemptsMade: job.attemptsMade,
      failedReason: job.failedReason || null,
      data: job.data,
    });
  } catch (error) {
    return res.status(500).json({
      message: 'Failed to fetch job status',
      error: error.message,
    });
  }
};

module.exports = {
  enqueueBatchJob,
  listUserJobs,
  getDeadLetterJobs,
  getJobStatus,
};
