import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';

const JOB_TYPES = [
  'BULK_INVOICE_SYNC',
  'WEBHOOK_FANOUT',
  'LEDGER_RECONCILE',
];

export default function JobDispatcher({ onJobEnqueued }) {
  const { authFetch } = useAuth();
  const [jobType, setJobType] = useState(JOB_TYPES[0]);
  const [itemCount, setItemCount] = useState(10);
  const [simulateTransientFail, setSimulateTransientFail] = useState(false);
  const [simulateFatalFail, setSimulateFatalFail] = useState(false);
  const [dispatching, setDispatching] = useState(false);
  const [feedback, setFeedback] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setDispatching(true);
    setFeedback(null);

    const parsedCount = Number(itemCount);

    try {
      const res = await authFetch('/api/jobs/batch', {
        method: 'POST',
        body: JSON.stringify({
          jobType,
          itemCount: parsedCount,
          simulateTransientFail,
          simulateFatalFail,
        }),
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.message || 'Failed to enqueue job');
      }

      if (onJobEnqueued) {
        onJobEnqueued({
          jobId: data.jobId,
          jobType,
          state: 'waiting',
          progress: 0,
          attemptsMade: 0,
          failedReason: null,
          result: null,
          data: {
            jobType,
            itemCount: parsedCount,
            simulateTransientFail,
            simulateFatalFail,
          },
          timestamp: Date.now(),
        });
      }

      setFeedback({
        type: 'success',
        text: `Enqueued ${data.jobId} (HTTP 202 Accepted)`,
      });
    } catch (err) {
      setFeedback({
        type: 'error',
        text: err.message,
      });
    } finally {
      setDispatching(false);
    }
  };

  return (
    <section className="panel dispatcher-panel" aria-labelledby="dispatcher-heading">
      <div className="panel-header">
        <h2 id="dispatcher-heading">Batch Producer Dispatch</h2>
        <span className="badge badge-neutral">HTTP 202 Producer</span>
      </div>

      <form onSubmit={handleSubmit} className="dispatcher-form">
        <div className="form-group">
          <label htmlFor="jobType">Pipeline Job Type</label>
          <select
            id="jobType"
            value={jobType}
            onChange={(e) => setJobType(e.target.value)}
            className="input-control"
          >
            {JOB_TYPES.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
        </div>

        <div className="form-group">
          <label htmlFor="itemCount">
            Batch Item Count (1–100): <strong>{itemCount}</strong>
          </label>
          <input
            id="itemCount"
            type="number"
            min={1}
            max={100}
            value={itemCount}
            onChange={(e) => setItemCount(Number(e.target.value))}
            className="input-control"
            required
          />
        </div>

        <fieldset className="chaos-fieldset">
          <legend>Chaos Engineering Hooks</legend>
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={simulateTransientFail}
              onChange={(e) => {
                setSimulateTransientFail(e.target.checked);
                if (e.target.checked) setSimulateFatalFail(false);
              }}
            />
            <span>Simulate Transient Failure (Recovers on Retry #2)</span>
          </label>

          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={simulateFatalFail}
              onChange={(e) => {
                setSimulateFatalFail(e.target.checked);
                if (e.target.checked) setSimulateTransientFail(false);
              }}
            />
            <span>Simulate Fatal Poison-Pill (Exhausts to DLQ)</span>
          </label>
        </fieldset>

        <button type="submit" className="btn btn-primary" disabled={dispatching}>
          {dispatching ? 'Enqueuing...' : 'Dispatch Batch Job'}
        </button>

        {feedback && (
          <div className={`alert ${feedback.type === 'error' ? 'alert-error' : 'alert-success'}`}>
            {feedback.text}
          </div>
        )}
      </form>
    </section>
  );
}
