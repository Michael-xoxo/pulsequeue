require('dotenv').config();
const http = require('http');
const path = require('path');
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const connectDB = require('./config/db');
const redisConnection = require('./config/redis');
const initSocket = require('./socket/socketHandler');
const queueBridge = require('./socket/queueBridge');
const authRoutes = require('./routes/authRoutes');
const batchRoutes = require('./routes/batchRoutes');

const app = express();
const server = http.createServer(app);
const io = initSocket(server);
const bridge = queueBridge(io);

app.set('io', io);

app.use(cors());
app.use(express.json());

app.get('/api/health', async (req, res) => {
  try {
    const pong = await redisConnection.ping();
    return res.status(200).json({ status: 'ok', redis: pong });
  } catch (error) {
    return res.status(500).json({ status: 'error', error: error.message });
  }
});

app.use('/api/auth', authRoutes);
app.use('/api/jobs', batchRoutes);

// Production Static Asset Pipeline & Express 5 Wildcard SPA Fallback
// Registered strictly after /api/auth and /api/jobs
const clientDistPath = path.join(__dirname, 'client/dist');
const staticMiddleware = express.static(clientDistPath);

if (process.env.NODE_ENV === 'production') {
  app.use(staticMiddleware);
} else {
  app.use((req, res, next) => {
    if (process.env.NODE_ENV === 'production') {
      return staticMiddleware(req, res, next);
    }
    return next();
  });
}

app.get('/{*splat}', (req, res, next) => {
  if (process.env.NODE_ENV === 'production') {
    return res.sendFile(path.join(clientDistPath, 'index.html'));
  }
  return next();
});

const PORT = process.env.PORT || 5000;
let embeddedWorker = null;

const startServer = async (port = PORT) => {
  await connectDB();
  const pong = await redisConnection.ping();
  await bridge.readyPromise;
  console.log(`Redis PING response: ${pong}`);

  if (process.env.EMBED_WORKER === 'true') {
    const workerModule = require('./workers/batchWorker');
    embeddedWorker = await workerModule.startWorker();
    console.log('Embedded BullMQ worker initialized via EMBED_WORKER=true');
  }

  return new Promise((resolve) => {
    server.listen(port, () => {
      console.log(`PulseQueue Hybrid HTTP + WebSocket Server running on port ${port}`);
      resolve(server);
    });
  });
};

const closeServer = async () => {
  if (embeddedWorker) {
    const workerToClose = embeddedWorker;
    embeddedWorker = null;
    await workerToClose.closeWorker({ quitRedis: true });
  }
  await bridge.close();
  io.close();
  return new Promise((resolve) => {
    if (!server.listening) return resolve();
    server.close(() => resolve());
  });
};

if (require.main === module) {
  startServer().catch((err) => {
    console.error('Failed to start server:', err);
    process.exit(1);
  });

  const gracefulShutdown = async (signal) => {
    console.log(`Received ${signal}. Draining server and embedded worker gracefully...`);
    try {
      await closeServer();
      if (redisConnection.status !== 'end') {
        await redisConnection.quit().catch(() => {});
      }
      if (mongoose.connection.readyState !== 0) {
        await mongoose.connection.close().catch(() => {});
      }
      process.exit(0);
    } catch (err) {
      console.error('Error during graceful shutdown:', err.message);
      process.exit(1);
    }
  };

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
}

module.exports = { app, server, io, bridge, startServer, closeServer };
