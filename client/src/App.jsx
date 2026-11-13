import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from './context/AuthContext';
import { usePulseSocket } from './hooks/usePulseSocket';
import JobDispatcher from './components/JobDispatcher';
import LiveJobFeed from './components/LiveJobFeed';
import DlqInspector from './components/DlqInspector';

function AuthGate() {
  const { login, register, loading, authError } = useAuth();
  const [isRegisterMode, setIsRegisterMode] = useState(false);
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (isRegisterMode) {
        await register(name, email, password);
      } else {
        await login(email, password);
      }
    } catch {
      // Handled via authError in context
    }
  };

  return (
    <main className="auth-shell">
      <div className="auth-card panel">
        <div className="brand-header">
          <span className="pulse-dot" />
          <h1>PulseQueue Mission Control</h1>
        </div>
        <p className="auth-subtitle">
          Tenant-isolated event-driven BullMQ &amp; Socket.io command center
        </p>

        <form onSubmit={handleSubmit} className="auth-form">
          {isRegisterMode && (
            <div className="form-group">
              <label htmlFor="name">Operator Name</label>
              <input
                id="name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className="input-control"
                placeholder="Jane Doe"
                required
              />
            </div>
          )}

          <div className="form-group">
            <label htmlFor="email">Tenant Email</label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="input-control"
              placeholder="operator@pulsequeue.io"
              required
            />
          </div>

          <div className="form-group">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="input-control"
              placeholder="••••••••"
              required
            />
          </div>

          {authError && <div className="alert alert-error">{authError}</div>}

          <button type="submit" className="btn btn-primary" disabled={loading}>
            {loading
              ? 'Authenticating...'
              : isRegisterMode
              ? 'Provision Tenant Account'
              : 'Sign In to Mission Control'}
          </button>
        </form>

        <button
          type="button"
          className="btn-link"
          onClick={() => setIsRegisterMode((prev) => !prev)}
        >
          {isRegisterMode
            ? 'Already have a tenant account? Sign in'
            : 'Need a tenant workspace? Register here'}
        </button>
      </div>
    </main>
  );
}

function MissionControlDashboard() {
  const { token, user, logout, authFetch } = useAuth();
  const [jobs, setJobs] = useState([]);
  const [hydrating, setHydrating] = useState(false);
  const [dlqRefreshSignal, setDlqRefreshSignal] = useState(0);

  const hydrateJobs = useCallback(async () => {
    setHydrating(true);
    try {
      const res = await authFetch('/api/jobs');
      if (res.ok) {
        const data = await res.json();
        setJobs(Array.isArray(data) ? data : []);
      }
    } catch (err) {
      console.error('Failed to hydrate jobs:', err);
    } finally {
      setHydrating(false);
    }
  }, [authFetch]);

  useEffect(() => {
    hydrateJobs();
  }, [hydrateJobs]);

  const handleJobEnqueued = useCallback((optimisticJob) => {
    setJobs((prev) => {
      const exists = prev.some((j) => j.jobId === optimisticJob.jobId);
      if (exists) return prev;
      return [optimisticJob, ...prev];
    });
  }, []);

  const handleJobProgress = useCallback((payload) => {
    setJobs((prev) => {
      const idx = prev.findIndex((j) => j.jobId === payload.jobId);
      if (idx === -1) {
        return [
          {
            jobId: payload.jobId,
            jobType: payload.jobType || 'BULK_INVOICE_SYNC',
            state: 'active',
            progress: payload.progress,
            attemptsMade: 1,
            failedReason: null,
            timestamp: Date.now(),
          },
          ...prev,
        ];
      }

      const next = [...prev];
      next[idx] = {
        ...next[idx],
        state: 'active',
        progress: payload.progress,
        failedReason: null,
      };
      return next;
    });
  }, []);

  const handleJobCompleted = useCallback((payload) => {
    setJobs((prev) => {
      const idx = prev.findIndex((j) => j.jobId === payload.jobId);
      if (idx === -1) {
        return [
          {
            jobId: payload.jobId,
            jobType: payload.jobType || 'BULK_INVOICE_SYNC',
            state: 'completed',
            progress: 100,
            result: payload.result,
            completedAt: payload.timestamp,
            failedReason: null,
            timestamp: Date.now(),
          },
          ...prev,
        ];
      }

      const next = [...prev];
      next[idx] = {
        ...next[idx],
        state: 'completed',
        progress: 100,
        result: payload.result,
        completedAt: payload.timestamp,
        failedReason: null,
      };
      return next;
    });
  }, []);

  const handleJobFailed = useCallback((payload) => {
    setJobs((prev) => {
      const idx = prev.findIndex((j) => j.jobId === payload.jobId);
      const nextState = payload.isDeadLetter ? 'failed' : 'active';

      if (idx === -1) {
        return [
          {
            jobId: payload.jobId,
            jobType: payload.jobType || 'BULK_INVOICE_SYNC',
            state: nextState,
            progress: 0,
            attemptsMade: payload.attemptsMade,
            failedReason: payload.failedReason,
            timestamp: Date.now(),
          },
          ...prev,
        ];
      }

      const next = [...prev];
      next[idx] = {
        ...next[idx],
        state: nextState,
        attemptsMade: payload.attemptsMade,
        failedReason: payload.failedReason,
      };
      return next;
    });

    if (payload.isDeadLetter) {
      setDlqRefreshSignal((n) => n + 1);
    }
  }, []);

  const { connectionStatus, tenantRoom } = usePulseSocket({
    token,
    onJobProgress: handleJobProgress,
    onJobCompleted: handleJobCompleted,
    onJobFailed: handleJobFailed,
    onReconnected: hydrateJobs,
  });

  return (
    <div className="dashboard-layout">
      <header className="topbar">
        <div className="brand-header">
          <span className="pulse-dot" />
          <h1>PulseQueue Mission Control</h1>
        </div>

        <div className="topbar-controls">
          <span
            className={`socket-badge socket-${connectionStatus.toLowerCase()}`}
            aria-live="polite"
          >
            Socket: {connectionStatus}
          </span>
          {tenantRoom && <code className="room-pill">{tenantRoom}</code>}
          <span className="operator-email">{user?.email}</span>
          <button type="button" className="btn btn-secondary btn-sm" onClick={logout}>
            Sign Out
          </button>
        </div>
      </header>

      <main className="dashboard-grid">
        <div className="grid-left">
          <JobDispatcher onJobEnqueued={handleJobEnqueued} />
        </div>
        <div className="grid-right">
          <LiveJobFeed jobs={jobs} loading={hydrating} onRefresh={hydrateJobs} />
          <DlqInspector refreshSignal={dlqRefreshSignal} />
        </div>
      </main>
    </div>
  );
}

export default function App() {
  const { isAuthenticated } = useAuth();
  return isAuthenticated ? <MissionControlDashboard /> : <AuthGate />;
}
