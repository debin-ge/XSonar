package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xsonar/pkg/collector"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type schedulerStoreConfig struct {
	Backend       string
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	LeaderLockTTL time.Duration
}

type schedulerRow interface {
	Scan(dest ...any) error
}

type schedulerRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

type schedulerDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) schedulerRow
	Query(context.Context, string, ...any) (schedulerRows, error)
}

type pgxSchedulerDB struct {
	pool *pgxpool.Pool
}

func (d *pgxSchedulerDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return d.pool.Exec(ctx, sql, args...)
}

func (d *pgxSchedulerDB) QueryRow(ctx context.Context, sql string, args ...any) schedulerRow {
	return d.pool.QueryRow(ctx, sql, args...)
}

func (d *pgxSchedulerDB) Query(ctx context.Context, sql string, args ...any) (schedulerRows, error) {
	return d.pool.Query(ctx, sql, args...)
}

type pgRedisStore struct {
	logger        *xlog.Logger
	db            schedulerDB
	pg            *pgxpool.Pool
	redis         *redis.Client
	leaderLockTTL time.Duration
}

var releaseSchedulerLeaderLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func loadSchedulerStoreConfig() schedulerStoreConfig {
	cfg := schedulerStoreConfig{
		Backend: strings.ToLower(strings.TrimSpace(firstNonEmpty(
			os.Getenv("SCHEDULER_RPC_STORE_BACKEND"),
			os.Getenv("COMMON_STORE_BACKEND"),
		))),
		PostgresDSN: strings.TrimSpace(firstNonEmpty(
			os.Getenv("SCHEDULER_RPC_POSTGRES_DSN"),
			os.Getenv("COMMON_POSTGRES_DSN"),
		)),
		RedisAddr: strings.TrimSpace(firstNonEmpty(
			os.Getenv("SCHEDULER_RPC_REDIS_ADDR"),
			os.Getenv("COMMON_REDIS_ADDR"),
		)),
		RedisPassword: firstNonEmpty(
			os.Getenv("SCHEDULER_RPC_REDIS_PASSWORD"),
			os.Getenv("COMMON_REDIS_PASSWORD"),
		),
		LeaderLockTTL: time.Duration(defaultLeaderLockTTLMS) * time.Millisecond,
	}

	if redisDBValue := strings.TrimSpace(firstNonEmpty(
		os.Getenv("SCHEDULER_RPC_REDIS_DB"),
		os.Getenv("COMMON_REDIS_DB"),
	)); redisDBValue != "" {
		parsed, err := strconv.Atoi(redisDBValue)
		if err != nil {
			panic(fmt.Sprintf("invalid redis db %q", redisDBValue))
		}
		cfg.RedisDB = parsed
	}

	if ttlValue := strings.TrimSpace(firstNonEmpty(
		os.Getenv("SCHEDULER_RPC_LEADER_LOCK_TTL_MS"),
		os.Getenv("COMMON_LEADER_LOCK_TTL_MS"),
	)); ttlValue != "" {
		parsed, err := strconv.Atoi(ttlValue)
		if err != nil || parsed <= 0 {
			panic(fmt.Sprintf("invalid leader lock ttl %q", ttlValue))
		}
		cfg.LeaderLockTTL = time.Duration(parsed) * time.Millisecond
	}

	if cfg.Backend == "" {
		if cfg.PostgresDSN != "" && cfg.RedisAddr != "" {
			cfg.Backend = "pgredis"
		} else {
			cfg.Backend = "memory"
		}
	}

	return cfg
}

func validateSchedulerStoreConfig(cfg schedulerStoreConfig) error {
	switch cfg.Backend {
	case "memory":
		return nil
	case "pgredis":
		if strings.TrimSpace(cfg.PostgresDSN) == "" {
			return fmt.Errorf("COMMON_POSTGRES_DSN is required when scheduler backend is pgredis")
		}
		if strings.TrimSpace(cfg.RedisAddr) == "" {
			return fmt.Errorf("COMMON_REDIS_ADDR is required when scheduler backend is pgredis")
		}
		return nil
	default:
		return fmt.Errorf("unsupported scheduler store backend %q", cfg.Backend)
	}
}

func newPGRedisStore(cfg schedulerStoreConfig, logger *xlog.Logger) (*pgRedisStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	store := &pgRedisStore{
		logger:        logger,
		db:            &pgxSchedulerDB{pool: pool},
		pg:            pool,
		redis:         redisClient,
		leaderLockTTL: cfg.LeaderLockTTL,
	}

	if err := store.ensureSchema(ctx); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}

	if logger != nil {
		logger.Info("scheduler-rpc connected to postgres/redis backend", map[string]any{
			"redis_addr": cfg.RedisAddr,
		})
	}

	return store, nil
}

func (s *pgRedisStore) Close(context.Context) error {
	if s == nil {
		return nil
	}
	if s.pg != nil {
		s.pg.Close()
	}
	if s.redis != nil {
		return s.redis.Close()
	}
	return nil
}

func (s *pgRedisStore) ensureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("scheduler database is not configured")
	}
	if _, err := s.db.Exec(ctx, collector.SchemaSQL); err != nil {
		return fmt.Errorf("ensure collector schema: %w", err)
	}
	return nil
}

func (s *pgRedisStore) TryBecomeLeader(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if s == nil || s.redis == nil {
		return false, errors.New("scheduler redis is not configured")
	}
	ttl = normalizeLeaderTTL(ttl)

	result, err := s.redis.SetArgs(ctx, collector.SchedulerLeaderLockKey(), owner, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return result == "OK", nil
}

func (s *pgRedisStore) ReleaseLeader(ctx context.Context, owner string) error {
	if s == nil || s.redis == nil {
		return errors.New("scheduler redis is not configured")
	}
	_, err := releaseSchedulerLeaderLockScript.Run(ctx, s.redis, []string{schedulerLeaderLockKey()}, owner).Result()
	return err
}

func schedulerLeaderLockKey() string {
	return collector.SchedulerLeaderLockKey()
}

func (s *pgRedisStore) CreateTask(ctx context.Context, item *task) (*task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}
	if item == nil {
		return nil, schedulerInvalidRequest("task is required")
	}

	row := s.db.QueryRow(ctx, schedulerCreateTaskSQL,
		strings.TrimSpace(item.TaskID),
		strings.ToLower(strings.TrimSpace(item.TaskType)),
		strings.TrimSpace(item.Keyword),
		strings.TrimSpace(item.CreatedBy),
		item.Priority,
		int32Arg(item.FrequencySeconds),
		stringArg(item.Since),
		stringArg(item.Until),
		int64Arg(item.RequiredCount),
		int64Arg(item.PerRunCount),
	)

	created, err := scanTaskRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return created, nil
}

func (s *pgRedisStore) GetTask(ctx context.Context, taskID, createdBy string) (*task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}

	row := s.db.QueryRow(ctx, schedulerGetTaskSQL, strings.TrimSpace(taskID), strings.TrimSpace(createdBy))
	item, err := scanTaskRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return item, nil
}

func (s *pgRedisStore) ListTaskRuns(ctx context.Context, taskID, createdBy string, limit int) ([]taskRun, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}
	if limit <= 0 {
		limit = defaultListTaskRunsDefaultLimit
	}

	rows, err := s.db.Query(ctx, schedulerListTaskRunsSQL, strings.TrimSpace(taskID), strings.TrimSpace(createdBy), limit)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	defer rows.Close()

	items := make([]taskRun, 0, limit)
	for rows.Next() {
		item, scanErr := scanTaskRunRow(rows)
		if scanErr != nil {
			return nil, schedulerServiceErrorFromErr(scanErr)
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return items, nil
}

func (s *pgRedisStore) StopTask(ctx context.Context, taskID, createdBy string) (*task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}

	row := s.db.QueryRow(ctx, schedulerStopTaskSQL, strings.TrimSpace(taskID), strings.TrimSpace(createdBy))
	item, err := scanTaskRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return item, nil
}

func (s *pgRedisStore) ListDueTasks(ctx context.Context, now time.Time, limit int) ([]task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}
	if limit <= 0 {
		limit = defaultDispatchBatchSize
	}

	rows, err := s.db.Query(ctx, schedulerListDueTasksSQL, now.UTC(), limit)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	defer rows.Close()

	items := make([]task, 0, limit)
	for rows.Next() {
		item, scanErr := scanTaskRow(rows)
		if scanErr != nil {
			return nil, schedulerServiceErrorFromErr(scanErr)
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return items, nil
}

func (s *pgRedisStore) CreateRun(ctx context.Context, item *taskRun) (*taskRun, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}
	if item == nil {
		return nil, schedulerInvalidRequest("run is required")
	}

	taskID := strings.TrimSpace(item.TaskID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}

	runID := strings.TrimSpace(item.RunID)
	if runID == "" {
		runID = shared.NewID("run")
	}
	runNo := item.RunNo
	if runNo <= 0 {
		return nil, schedulerInvalidRequest("run_no is required")
	}

	status := strings.TrimSpace(item.Status)
	if status == "" {
		status = RunStatusQueued
	}

	scheduledAt := item.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().UTC()
	} else {
		scheduledAt = scheduledAt.UTC()
	}

	row := s.db.QueryRow(ctx, schedulerCreateRunSQL, runID, taskID, runNo, status, scheduledAt)
	created, err := scanTaskRunRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return created, nil
}

func (s *pgRedisStore) NextRunNo(ctx context.Context, taskID string) (int64, *serviceError) {
	if s == nil || s.db == nil {
		return 0, internalSchedulerError("scheduler database is not configured")
	}

	var runNo int64
	row := s.db.QueryRow(ctx, schedulerNextRunNoSQL, strings.TrimSpace(taskID))
	if err := row.Scan(&runNo); err != nil {
		return 0, schedulerServiceErrorFromErr(err)
	}
	return runNo, nil
}

func (s *pgRedisStore) HasOpenRun(ctx context.Context, taskID string) (bool, *serviceError) {
	if s == nil || s.db == nil {
		return false, internalSchedulerError("scheduler database is not configured")
	}

	var exists bool
	row := s.db.QueryRow(ctx, schedulerHasOpenRunSQL, strings.TrimSpace(taskID))
	if err := row.Scan(&exists); err != nil {
		return false, schedulerServiceErrorFromErr(err)
	}
	return exists, nil
}

func (s *pgRedisStore) UpdateTaskDispatch(ctx context.Context, taskID, status string, nextRunAt *time.Time) (*task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}

	var next any
	if nextRunAt != nil {
		next = nextRunAt.UTC()
	}

	row := s.db.QueryRow(ctx, schedulerUpdateTaskDispatchSQL, strings.TrimSpace(taskID), strings.TrimSpace(status), next)
	item, err := scanTaskRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return item, nil
}

func (s *pgRedisStore) MarkTaskRunning(ctx context.Context, taskID, runID string, startedAt time.Time) (*task, *serviceError) {
	if s == nil || s.db == nil {
		return nil, internalSchedulerError("scheduler database is not configured")
	}

	taskID = strings.TrimSpace(taskID)
	runID = strings.TrimSpace(runID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}

	startedAt = startedAt.UTC()
	if runID != "" {
		row := s.db.QueryRow(ctx, schedulerMarkTaskRunningWithRunSQL, taskID, runID, startedAt)
		item, err := scanTaskRow(row)
		if err == nil {
			return item, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			if _, taskErr := s.GetTask(ctx, taskID, ""); taskErr != nil {
				return nil, taskErr
			}
			return nil, schedulerNotFound("run not found")
		}
		return nil, schedulerServiceErrorFromErr(err)
	}

	row := s.db.QueryRow(ctx, schedulerMarkTaskRunningSQL, taskID, startedAt)
	item, err := scanTaskRow(row)
	if err != nil {
		return nil, schedulerServiceErrorFromErr(err)
	}
	return item, nil
}

func (s *pgRedisStore) QueueBacklog(ctx context.Context, now time.Time) (queueBacklog, error) {
	if s == nil || s.db == nil {
		return queueBacklog{}, errors.New("scheduler database is not configured")
	}

	var backlog queueBacklog
	var oldestScheduledAt sql.NullTime
	row := s.db.QueryRow(ctx, schedulerQueueBacklogSQL)
	if err := row.Scan(&backlog.PendingCount, &oldestScheduledAt); err != nil {
		return queueBacklog{}, err
	}
	if oldestScheduledAt.Valid {
		backlog.OldestAge = now.UTC().Sub(oldestScheduledAt.Time.UTC())
		if backlog.OldestAge < 0 {
			backlog.OldestAge = 0
		}
	}
	return backlog, nil
}

func (s *pgRedisStore) EnqueueRun(ctx context.Context, runID string) error {
	if s == nil || s.redis == nil {
		return errors.New("scheduler redis is not configured")
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("run_id is required")
	}

	return s.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: collector.RunsStreamKey(),
		Values: map[string]any{
			"run_id": strings.TrimSpace(runID),
		},
	}).Err()
}

func scanTaskRow(row schedulerRow) (*task, error) {
	var item task
	var frequencySeconds sql.NullInt32
	var since sql.NullString
	var until sql.NullString
	var requiredCount sql.NullInt64
	var perRunCount sql.NullInt64
	var resumeCursor sql.NullString
	var nextRunAt sql.NullTime
	var lastRunAt sql.NullTime

	if err := row.Scan(
		&item.TaskID,
		&item.TaskType,
		&item.Keyword,
		&item.CreatedBy,
		&item.Priority,
		&frequencySeconds,
		&since,
		&until,
		&requiredCount,
		&perRunCount,
		&resumeCursor,
		&item.ResumeOffset,
		&item.CompletedCount,
		&item.Status,
		&nextRunAt,
		&lastRunAt,
		&item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return nil, err
	}

	if frequencySeconds.Valid {
		item.FrequencySeconds = cloneInt32Ptr(&frequencySeconds.Int32)
	}
	if since.Valid {
		item.Since = since.String
	}
	if until.Valid {
		item.Until = until.String
	}
	if requiredCount.Valid {
		item.RequiredCount = cloneInt64Ptr(&requiredCount.Int64)
	}
	if perRunCount.Valid {
		item.PerRunCount = cloneInt64Ptr(&perRunCount.Int64)
	}
	if resumeCursor.Valid {
		item.ResumeCursor = resumeCursor.String
	}
	if nextRunAt.Valid {
		item.NextRunAt = cloneTimePtr(&nextRunAt.Time)
	}
	if lastRunAt.Valid {
		item.LastRunAt = cloneTimePtr(&lastRunAt.Time)
	}

	return &item, nil
}

func scanTaskRunRow(row schedulerRow) (*taskRun, error) {
	var item taskRun
	var stopReason sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var outputPath sql.NullString
	var nextCursor sql.NullString
	var resumeCursor sql.NullString
	var errorMessage sql.NullString

	if err := row.Scan(
		&item.RunID,
		&item.TaskID,
		&item.RunNo,
		&item.Status,
		&stopReason,
		&item.ScheduledAt,
		&startedAt,
		&endedAt,
		&outputPath,
		&item.PageCount,
		&item.FetchedCount,
		&item.NewCount,
		&item.DuplicateCount,
		&nextCursor,
		&resumeCursor,
		&item.ResumeOffset,
		&errorMessage,
	); err != nil {
		return nil, err
	}

	if stopReason.Valid {
		item.StopReason = stopReason.String
	}
	if startedAt.Valid {
		item.StartedAt = cloneTimePtr(&startedAt.Time)
	}
	if endedAt.Valid {
		item.EndedAt = cloneTimePtr(&endedAt.Time)
	}
	if outputPath.Valid {
		item.OutputPath = outputPath.String
	}
	if nextCursor.Valid {
		item.NextCursor = nextCursor.String
	}
	if resumeCursor.Valid {
		item.ResumeCursor = resumeCursor.String
	}
	if errorMessage.Valid {
		item.ErrorMessage = errorMessage.String
	}

	return &item, nil
}

func int32Arg(value *int32) any {
	if value == nil {
		return nil
	}
	return *value
}

func int64Arg(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func stringArg(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

const schedulerCreateTaskSQL = `
INSERT INTO collector.tasks (
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    NULL,
    0,
    0,
    'pending',
    NOW(),
    NULL,
    NOW(),
    NOW()
)
RETURNING
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
`

const schedulerGetTaskSQL = `
SELECT
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
FROM collector.tasks
WHERE task_id = $1
  AND ($2 = '' OR created_by = $2)
`

const schedulerListTaskRunsSQL = `
SELECT
    run_id,
    task_id,
    run_no,
    status,
    stop_reason,
    scheduled_at,
    started_at,
    ended_at,
    output_path,
    page_count,
    fetched_count,
    new_count,
    duplicate_count,
    next_cursor,
    resume_cursor,
    resume_offset,
    error_message
FROM collector.task_runs
WHERE task_id = $1
  AND EXISTS (
      SELECT 1
      FROM collector.tasks
      WHERE task_id = $1
        AND ($2 = '' OR created_by = $2)
  )
ORDER BY run_no DESC, run_id DESC
LIMIT $3
`

const schedulerListDueTasksSQL = `
SELECT
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
FROM collector.tasks
WHERE status = 'pending'
  AND next_run_at IS NOT NULL
  AND next_run_at <= $1
ORDER BY priority ASC, next_run_at ASC, task_id ASC
LIMIT $2
`

const schedulerNextRunNoSQL = `
SELECT COALESCE(MAX(run_no), 0) + 1
FROM collector.task_runs
WHERE task_id = $1
`

const schedulerHasOpenRunSQL = `
SELECT EXISTS (
    SELECT 1
    FROM collector.task_runs
    WHERE task_id = $1
      AND status IN ('queued', 'leased', 'running')
)
`

const schedulerCreateRunSQL = `
INSERT INTO collector.task_runs (
    run_id,
    task_id,
    run_no,
    status,
    stop_reason,
    scheduled_at,
    started_at,
    ended_at,
    output_path,
    page_count,
    fetched_count,
    new_count,
    duplicate_count,
    next_cursor,
    resume_cursor,
    resume_offset,
    error_message
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    NULL,
    $5,
    NULL,
    NULL,
    NULL,
    0,
    0,
    0,
    0,
    NULL,
    NULL,
    0,
    NULL
)
RETURNING
    run_id,
    task_id,
    run_no,
    status,
    stop_reason,
    scheduled_at,
    started_at,
    ended_at,
    output_path,
    page_count,
    fetched_count,
    new_count,
    duplicate_count,
    next_cursor,
    resume_cursor,
    resume_offset,
    error_message
`

const schedulerUpdateTaskDispatchSQL = `
UPDATE collector.tasks
SET status = $2,
    next_run_at = COALESCE($3::timestamptz, next_run_at),
    updated_at = NOW()
WHERE task_id = $1
  AND ($2 = '' OR created_by = $2)
RETURNING
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
`

const schedulerMarkTaskRunningSQL = `
UPDATE collector.tasks
SET status = 'running',
    last_run_at = $2,
    updated_at = NOW()
WHERE task_id = $1
RETURNING
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
`

const schedulerMarkTaskRunningWithRunSQL = `
WITH updated_run AS (
    UPDATE collector.task_runs
    SET status = 'running',
        started_at = $3
    WHERE task_id = $1
      AND run_id = $2
    RETURNING 1
),
updated_task AS (
    UPDATE collector.tasks
    SET status = 'running',
        last_run_at = $3,
        updated_at = NOW()
    WHERE task_id = $1
      AND EXISTS (SELECT 1 FROM updated_run)
    RETURNING
        task_id,
        task_type,
        keyword,
        created_by,
        priority,
        frequency_seconds,
        since,
        until,
        required_count,
        per_run_count,
        resume_cursor,
        resume_offset,
        completed_count,
        status,
        next_run_at,
        last_run_at,
        created_at,
        updated_at
)
SELECT
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
FROM updated_task
`

const schedulerStopTaskSQL = `
UPDATE collector.tasks
SET status = 'paused',
    next_run_at = NULL,
    updated_at = NOW()
WHERE task_id = $1
RETURNING
    task_id,
    task_type,
    keyword,
    created_by,
    priority,
    frequency_seconds,
    since,
    until,
    required_count,
    per_run_count,
    resume_cursor,
    resume_offset,
    completed_count,
    status,
    next_run_at,
    last_run_at,
    created_at,
    updated_at
`

const schedulerQueueBacklogSQL = `
SELECT
    COUNT(*),
    MIN(scheduled_at)
FROM collector.task_runs
WHERE status IN ('queued', 'leased', 'running')
`

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
