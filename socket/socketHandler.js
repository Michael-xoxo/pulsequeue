const { Server } = require('socket.io');
const jwt = require('jsonwebtoken');

let ioInstance = null;

const initSocket = (httpServer) => {
  const io = new Server(httpServer, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST'],
    },
  });

  io.use((socket, next) => {
    try {
      let token =
        (socket.handshake.auth && socket.handshake.auth.token) ||
        (socket.handshake.headers && socket.handshake.headers.authorization);

      if (!token) {
        return next(new Error('Authentication error'));
      }

      if (typeof token === 'string' && token.startsWith('Bearer ')) {
        token = token.slice(7).trim();
      }

      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      decoded._id = (decoded._id || decoded.id).toString();
      socket.user = decoded;
      socket.join(`tenant:${decoded._id}`);

      return next();
    } catch (error) {
      return next(new Error('Authentication error'));
    }
  });

  io.on('connection', (socket) => {
    const tenantRoom = `tenant:${socket.user._id}`;
    socket.join(tenantRoom);

    io.to(tenantRoom).emit('tenant:joined', {
      room: tenantRoom,
      userId: socket.user._id,
    });
  });

  ioInstance = io;
  return io;
};

const getIO = () => {
  if (!ioInstance) {
    throw new Error('Socket.io has not been initialized');
  }
  return ioInstance;
};

initSocket.initSocket = initSocket;
initSocket.getIO = getIO;

module.exports = initSocket;
