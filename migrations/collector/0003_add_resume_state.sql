ALTER TABLE collector.tasks
    ADD COLUMN IF NOT EXISTS resume_cursor TEXT NULL;

ALTER TABLE collector.tasks
    ADD COLUMN IF NOT EXISTS resume_offset BIGINT NOT NULL DEFAULT 0;

ALTER TABLE collector.task_runs
    ADD COLUMN IF NOT EXISTS resume_cursor TEXT NULL;

ALTER TABLE collector.task_runs
    ADD COLUMN IF NOT EXISTS resume_offset BIGINT NOT NULL DEFAULT 0;
