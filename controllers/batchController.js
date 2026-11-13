const crypto = require('crypto');
const batchQueue = require('../queues/batchQueue');

const pendingEnqueues = new Map();

const enqueueBatchJob = async (req, res) => {
  try {
    const { jobType = 'BULK_INVOICE_SYNC', itemCount } = req.body || {};

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

    const enqueuePromise = batchQueue.add(
      jobType,
      {
        userId,
        jobType,
        itemCount,
      },
      { jobId: customJobId }
    );

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
  getJobStatus,
};
