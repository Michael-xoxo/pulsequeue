import React from 'react';

export default function LiveJobFeed({ jobs, loading, onRefresh }) {
  return (
    <section className="panel feed-panel" aria-labelledby="feed-heading">
      <div className="panel-header">
        <div>
          <h2 id="feed-heading">Live Job Telemetry Stream</h2>
          <p className="panel-subtitle">Zero-polling WebSocket progress fan-out</p>
        </div>
        <button type="button" className="btn btn-secondary btn-sm" onClick={onRefresh}>
          Hydrate State
        </button>
      </div>

      {loading && jobs.length === 0 ? (
        <div className="empty-state">Hydrating tenant job state...</div>
      ) : jobs.length === 0 ? (
        <div className="empty-state">No batch jobs dispatched in this tenant workspace yet.</div>
      ) : (
        <div className="job-list">
          {jobs.map((job) => {
            const progress = Math.min(100, Math.max(0, Number(job.progress) || 0));
            const state = job.state || 'waiting';

            return (
              <article key={job.jobId} className={`job-card state-${state}`}>
                <div className="job-card-top">
                  <div>
                    <span className="job-type">{job.jobType || 'BULK_INVOICE_SYNC'}</span>
                    <code className="job-id">{job.jobId}</code>
                  </div>
                  <span className={`status-pill status-${state}`}>{state}</span>
                </div>

                <div
                  className="progress-track"
                  role="progressbar"
                  aria-valuenow={progress}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-label={`Job ${job.jobId} progress`}
                >
                  <div
                    className={`progress-fill fill-${state}`}
                    style={{ width: `${progress}%` }}
                  />
                </div>

                <div className="job-card-meta">
                  <span>Progress: {progress}%</span>
                  {job.attemptsMade > 0 && <span>Attempts: {job.attemptsMade}</span>}
                  {job.completedAt && (
                    <span>Completed: {new Date(job.completedAt).toLocaleTimeString()}</span>
                  )}
                </div>

                {job.failedReason && (
                  <div className="job-error-banner">
                    <strong>Failure Reason:</strong> {job.failedReason}
                  </div>
                )}
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}
