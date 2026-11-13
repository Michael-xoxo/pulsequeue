require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const connectDB = require('./config/db');
const redisConnection = require('./config/redis');
const initSocket = require('./socket/socketHandler');
const authRoutes = require('./routes/authRoutes');
const batchRoutes = require('./routes/batchRoutes');

const app = express();
const server = http.createServer(app);
const io = initSocket(server);

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

const PORT = process.env.PORT || 5000;

const startServer = async (port = PORT) => {
  await connectDB();
  const pong = await redisConnection.ping();
  console.log(`Redis PING response: ${pong}`);

  return new Promise((resolve) => {
    server.listen(port, () => {
      console.log(`PulseQueue Hybrid HTTP + WebSocket Server running on port ${port}`);
      resolve(server);
    });
  });
};

if (require.main === module) {
  startServer().catch((err) => {
    console.error('Failed to start server:', err);
    process.exit(1);
  });
}

module.exports = { app, server, io, startServer };
