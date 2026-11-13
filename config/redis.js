require('dotenv').config();
const Redis = require('ioredis');

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const requiresTls = redisUrl.startsWith('rediss://') || redisUrl.includes('upstash.io');

const redisOptions = {
  maxRetriesPerRequest: null,
  ...(requiresTls ? { tls: { rejectUnauthorized: false } } : {}),
};

const connection = new Redis(redisUrl, redisOptions);

connection.on('connect', () => {
  console.log('Redis client connected');
});

connection.on('error', (err) => {
  console.error('Redis connection error:', err.message);
});

connection.connection = connection;
connection.redis = connection;

module.exports = connection;
