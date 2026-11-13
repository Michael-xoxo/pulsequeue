const { Queue } = require('bullmq');
const connection = require('../config/redis');

const defaultJobOptions = {
  attempts: 3,
  backoff: {
    type: 'exponential',
    delay: 1000,
  },
  removeOnComplete: 100,
  removeOnFail: false,
};

const batchQueue = new Queue('batch-processor', {
  connection,
  defaultJobOptions,
});

batchQueue.batchQueue = batchQueue;
batchQueue.batchProcessorQueue = batchQueue;

module.exports = batchQueue;
