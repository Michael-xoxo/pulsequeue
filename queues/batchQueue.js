const { Queue } = require('bullmq');
const connection = require('../config/redis');

const batchQueue = new Queue('batch-processor', { connection });

batchQueue.batchQueue = batchQueue;
batchQueue.batchProcessorQueue = batchQueue;

module.exports = batchQueue;
