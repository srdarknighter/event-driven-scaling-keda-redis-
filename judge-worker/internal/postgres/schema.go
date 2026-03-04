package postgres

var Schema = `
CREATE TABLE IF NOT EXISTS submissions (
    submission_id VARCHAR(255) PRIMARY KEY,
    user_id       VARCHAR(255) NOT NULL,
    tier          VARCHAR(50)  NOT NULL,
    language      VARCHAR(50)  NOT NULL,
    execution_ms  INT          NOT NULL,
    status        VARCHAR(50)  NOT NULL,
    completed_at  TIMESTAMPTZ  NOT NULL
)

CREATE TABLE IF NOT EXISTS job_outbox (
    id            BIGSERIAL    PRIMARY KEY,
    submission_id VARCHAR(255) NOT NULL UNIQUE,
    user_id       VARCHAR(255) NOT NULL,
    language      VARCHAR(50)  NOT NULL,
    tier          VARCHAR(50)  NOT NULL,
    payload       JSONB        NOT NULL,   -- full Job struct, serialised
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ
)

CREATE INDEX IF NOT EXISTS idx_job_outbox_status ON job_outbox(status) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS processed_jobs (
	submission_id VARCHAR(255) NOT NULL,
	worker_id     VARCHAR(255) NOT NULL,
	claimed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    result_payload JSONB, 
    result_saved_at TIMESTAMPTZ
)
`
