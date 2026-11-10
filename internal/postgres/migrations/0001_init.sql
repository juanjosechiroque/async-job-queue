CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(10) PRIMARY KEY,
    lines INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
    file_path TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS jobs_status_created_at_idx ON jobs (status, created_at);
