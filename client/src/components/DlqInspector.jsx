import React, { useEffect, useState, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';

export default function DlqInspector({ refreshSignal }) {
  const { authFetch } = useAuth();
  const [dlqJobs, setDlqJobs] = useState([]);
  const [loading, setLoading] = useState(false);

  const fetchDlqJobs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await authFetch('/api/jobs/dlq');
      if (res.ok) {
        const data = await res.json();
        setDlqJobs(Array.isArray(data) ? data : []);
      }
    } catch (err) {
      console.error('Failed to load DLQ jobs:', err);
    } finally {
      setLoading(false);
    }
  }, [authFetch]);

  useEffect(() => {
    fetchDlqJobs();
  }, [fetchDlqJobs, refreshSignal]);

  return (
    <section className="panel dlq-panel" aria-labelledby="dlq-heading">
      <div className="panel-header">
        <div>
          <h2 id="dlq-heading">Dead-Letter Queue (DLQ) Quarantine</h2>
          <p className="panel-subtitle">Exhausted poison-pill payloads persisted in MongoDB</p>
        </div>
        <span className="badge badge-danger">{dlqJobs.length} Quarantined</span>
      </div>

      {loading && dlqJobs.length === 0 ? (
        <div className="empty-state">Inspecting Dead-Letter Queue...</div>
      ) : dlqJobs.length === 0 ? (
        <div className="empty-state">Zero quarantined jobs in Dead-Letter Queue.</div>
      ) : (
        <div className="dlq-table-wrapper">
          <table className="dlq-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Job Type</th>
                <th>Attempts</th>
                <th>Failure Reason</th>
                <th>Failed At</th>
              </tr>
            </thead>
            <tbody>
              {dlqJobs.map((item) => (
                <tr key={item.jobId}>
                  <td>
                    <code>{item.jobId}</code>
                  </td>
                  <td>{item.jobType}</td>
                  <td>{item.attemptsMade}</td>
                  <td className="dlq-reason">{item.failedReason}</td>
                  <td>{item.failedAt ? new Date(item.failedAt).toLocaleTimeString() : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
