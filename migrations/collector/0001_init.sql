CREATE SCHEMA IF NOT EXISTS collector;

CREATE TABLE IF NOT EXISTS collector.tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    keyword TEXT NOT NULL,
    created_by TEXT NOT NULL,
    priority INT NOT NULL,
    frequency_seconds INT NULL,
    since TEXT NULL,
    until TEXT NULL,
    required_count BIGINT NULL,
    completed_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    next_run_at TIMESTAMPTZ NULL,
    last_run_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS collector.task_runs (
    run_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES collector.tasks(task_id),
    run_no BIGINT NOT NULL,
    status TEXT NOT NULL,
    stop_reason TEXT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NULL,
    ended_at TIMESTAMPTZ NULL,
    output_path TEXT NULL,
    page_count BIGINT NOT NULL DEFAULT 0,
    fetched_count BIGINT NOT NULL DEFAULT 0,
    new_count BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    next_cursor TEXT NULL,
    error_message TEXT NULL,
    CONSTRAINT collector_task_runs_task_run_no_key UNIQUE (task_id, run_no)
);

CREATE INDEX IF NOT EXISTS idx_collector_task_runs_task_id_scheduled_at
    ON collector.task_runs (task_id, scheduled_at DESC);

CREATE TABLE IF NOT EXISTS collector.task_seen_posts (
    task_id TEXT NOT NULL REFERENCES collector.tasks(task_id),
    post_id TEXT NOT NULL,
    run_id TEXT NULL REFERENCES collector.task_runs(run_id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, post_id)
);

CREATE INDEX IF NOT EXISTS idx_collector_task_seen_posts_run_id
    ON collector.task_seen_posts (run_id);

CREATE TABLE IF NOT EXISTS collector.keyword_monthly_usage (
    keyword TEXT NOT NULL,
    usage_month DATE NOT NULL,
    post_id TEXT NOT NULL,
    task_id TEXT NULL REFERENCES collector.tasks(task_id),
    run_id TEXT NULL REFERENCES collector.task_runs(run_id),
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (keyword, usage_month, post_id)
);

CREATE INDEX IF NOT EXISTS idx_collector_keyword_monthly_usage_keyword_month
    ON collector.keyword_monthly_usage (keyword, usage_month DESC);
