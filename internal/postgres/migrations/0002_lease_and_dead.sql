ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ NULL;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

UPDATE jobs SET status = 'dead' WHERE status = 'failed';

ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('queued', 'processing', 'completed', 'dead'));
