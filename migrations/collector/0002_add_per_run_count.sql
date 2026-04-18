ALTER TABLE collector.tasks
    ADD COLUMN IF NOT EXISTS per_run_count BIGINT NULL;
