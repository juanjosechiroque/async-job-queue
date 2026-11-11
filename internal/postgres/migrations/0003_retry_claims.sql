ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0);
ALTER TABLE jobs ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- A row already processing during the upgrade represents its first attempt.
-- Rows without a lease predate migration 0002 and must not remain stuck forever.
UPDATE jobs SET attempts = 1, claim_version = 1,
    lease_expires_at = COALESCE(lease_expires_at, now())
WHERE status = 'processing';

CREATE INDEX jobs_queued_available_idx ON jobs (available_at, created_at, id)
WHERE status = 'queued';
