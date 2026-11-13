const redisConnection = require('../config/redis');

const CHANNEL_NAME = 'pulsequeue:events';
const ALLOWED_EVENTS = new Set(['job:progress', 'job:completed', 'job:failed']);

let activeBridge = null;

const queueBridge = (io) => {
  if (activeBridge) {
    return activeBridge;
  }

  // Dedicated duplicated ioredis connection for blocking Redis Pub/Sub subscription
  const subConnection = redisConnection.duplicate({
    maxRetriesPerRequest: null,
  });

  subConnection.on('error', (err) => {
    console.error('QueueBridge Redis subscriber error:', err.message);
  });

  subConnection.on('message', (channel, rawMessage) => {
    if (channel !== CHANNEL_NAME) return;

    try {
      const parsed = JSON.parse(rawMessage);
      const { event, userId, payload } = parsed || {};

      if (!userId || !ALLOWED_EVENTS.has(event) || !payload) {
        return;
      }

      // RULE 5: Strictly emit only to tenant-isolated room
      const tenantRoom = `tenant:${String(userId)}`;
      io.to(tenantRoom).emit(event, payload);
    } catch (err) {
      console.error('QueueBridge failed to parse event message:', err.message);
    }
  });

  const readyPromise = subConnection.subscribe(CHANNEL_NAME);

  const close = async () => {
    if (!activeBridge) return;
    activeBridge = null;
    try {
      await subConnection.unsubscribe(CHANNEL_NAME);
    } catch (_) {}
    try {
      await subConnection.quit();
    } catch (_) {}
  };

  activeBridge = {
    subConnection,
    readyPromise,
    close,
    CHANNEL_NAME,
  };

  return activeBridge;
};

queueBridge.queueBridge = queueBridge;
queueBridge.CHANNEL_NAME = CHANNEL_NAME;

module.exports = queueBridge;
