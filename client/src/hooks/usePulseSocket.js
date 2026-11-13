import { useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';

/**
 * Custom hook managing authenticated Socket.io connection and real-time job lifecycle events.
 * Zero HTTP polling is used; all progress/completion/failure transitions stream via WebSockets.
 */
export function usePulseSocket({
  token,
  onJobProgress,
  onJobCompleted,
  onJobFailed,
  onReconnected,
}) {
  const [connectionStatus, setConnectionStatus] = useState('Disconnected');
  const [tenantRoom, setTenantRoom] = useState('');
  const socketRef = useRef(null);

  const callbacksRef = useRef({
    onJobProgress,
    onJobCompleted,
    onJobFailed,
    onReconnected,
  });

  useEffect(() => {
    callbacksRef.current = {
      onJobProgress,
      onJobCompleted,
      onJobFailed,
      onReconnected,
    };
  }, [onJobProgress, onJobCompleted, onJobFailed, onReconnected]);

  useEffect(() => {
    if (!token) {
      setConnectionStatus('Disconnected');
      setTenantRoom('');
      return undefined;
    }

    const socket = io('/', {
      auth: { token },
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionAttempts: Infinity,
      reconnectionDelay: 1000,
    });

    socketRef.current = socket;

    socket.on('connect', () => {
      setConnectionStatus('Connected');
    });

    socket.on('tenant:joined', (data) => {
      if (data && data.room) {
        setTenantRoom(data.room);
      }
      setConnectionStatus('Connected');
    });

    socket.on('disconnect', () => {
      setConnectionStatus('Disconnected');
    });

    socket.io.on('reconnect_attempt', () => {
      setConnectionStatus('Reconnecting');
    });

    socket.io.on('reconnect', () => {
      setConnectionStatus('Connected');
      if (callbacksRef.current.onReconnected) {
        callbacksRef.current.onReconnected();
      }
    });

    socket.on('connect_error', () => {
      setConnectionStatus('Disconnected');
    });

    socket.on('job:progress', (payload) => {
      if (callbacksRef.current.onJobProgress) {
        callbacksRef.current.onJobProgress(payload);
      }
    });

    socket.on('job:completed', (payload) => {
      if (callbacksRef.current.onJobCompleted) {
        callbacksRef.current.onJobCompleted(payload);
      }
    });

    socket.on('job:failed', (payload) => {
      if (callbacksRef.current.onJobFailed) {
        callbacksRef.current.onJobFailed(payload);
      }
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, [token]);

  return {
    socket: socketRef.current,
    connectionStatus,
    tenantRoom,
  };
}
