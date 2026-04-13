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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xsonar/pkg/collector"
	"xsonar/pkg/xlog"
)

type workerStoreConfig struct {
	Backend       string
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
}

type workerRow interface {
	Scan(dest ...any) error
}

type workerDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) workerRow
}

type pgxWorkerDB struct {
	pool *pgxpool.Pool
}

func (d *pgxWorkerDB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return d.pool.Exec(ctx, query, args...)
}

func (d *pgxWorkerDB) QueryRow(ctx context.Context, query string, args ...any) workerRow {
	return d.pool.QueryRow(ctx, query, args...)
}

type pgRedisWorkerStore struct {
	logger *xlog.Logger
	db     workerDB
	pg     *pgxpool.Pool
	redis  *redis.Client
}

var renewRunLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

func loadWorkerStoreConfig() workerStoreConfig {
	cfg := workerStoreConfig{
		Backend: strings.ToLower(strings.TrimSpace(firstNonEmpty(
			os.Getenv("COLLECTOR_WORKER_RPC_STORE_BACKEND"),
			os.Getenv("COMMON_STORE_BACKEND"),
		))),
		PostgresDSN: strings.TrimSpace(firstNonEmpty(
			os.Getenv("COLLECTOR_WORKER_RPC_POSTGRES_DSN"),
			os.Getenv("COMMON_POSTGRES_DSN"),
		)),
		RedisAddr: strings.TrimSpace(firstNonEmpty(
			os.Getenv("COLLECTOR_WORKER_RPC_REDIS_ADDR"),
			os.Getenv("COMMON_REDIS_ADDR"),
		)),
		RedisPassword: firstNonEmpty(
			os.Getenv("COLLECTOR_WORKER_RPC_REDIS_PASSWORD"),
			os.Getenv("COMMON_REDIS_PASSWORD"),
		),
	}

	if redisDBValue := strings.TrimSpace(firstNonEmpty(
		os.Getenv("COLLECTOR_WORKER_RPC_REDIS_DB"),
		os.Getenv("COMMON_REDIS_DB"),
	)); redisDBValue != "" {
		parsed, err := strconv.Atoi(redisDBValue)
		if err != nil {
			panic(fmt.Sprintf("invalid redis db %q", redisDBValue))
		}
		cfg.RedisDB = parsed
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

func validateWorkerStoreConfig(cfg workerStoreConfig) error {
	switch cfg.Backend {
	case "memory":
		return nil
	case "pgredis":
		if strings.TrimSpace(cfg.PostgresDSN) == "" {
			return errors.New("COMMON_POSTGRES_DSN is required when worker backend is pgredis")
		}
		if strings.TrimSpace(cfg.RedisAddr) == "" {
			return errors.New("COMMON_REDIS_ADDR is required when worker backend is pgredis")
		}
		return nil
	default:
		return fmt.Errorf("unsupported worker store backend %q", cfg.Backend)
	}
}

func newPGRedisWorkerStore(cfg workerStoreConfig, logger *xlog.Logger) (*pgRedisWorkerStore, error) {
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

	store := &pgRedisWorkerStore{
		logger: logger,
		db:     &pgxWorkerDB{pool: pool},
		pg:     pool,
		redis:  redisClient,
	}
	if err := store.ensureSchema(ctx); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}

	return store, nil
}

func (s *pgRedisWorkerStore) Close() error {
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

func (s *pgRedisWorkerStore) ensureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("worker database is not configured")
	}
	if _, err := s.db.Exec(ctx, collector.SchemaSQL); err != nil {
		return fmt.Errorf("ensure collector schema: %w", err)
	}
	return nil
}

func (s *pgRedisWorkerStore) LeaseRun(ctx context.Context, runID, workerID string, ttl time.Duration) (bool, error) {
	if s == nil || s.redis == nil || s.db == nil {
		return false, errors.New("worker store is not configured")
	}

	runID = strings.TrimSpace(runID)
	workerID = strings.TrimSpace(workerID)
	acquired, err := s.redis.SetNX(ctx, collector.RunLeaseKey(runID), workerID, normalizeWorkerLeaseTTL(ttl)).Result()
	if err != nil || !acquired {
		return acquired, err
	}

	if _, err := s.db.Exec(ctx, workerLeaseRunSQL, runID); err != nil {
		_ = s.redis.Del(ctx, collector.RunLeaseKey(runID)).Err()
		return false, err
	}
	return true, nil
}

func (s *pgRedisWorkerStore) RenewRunLease(ctx context.Context, runID, workerID string, ttl time.Duration) (bool, error) {
	if s == nil || s.redis == nil {
		return false, errors.New("worker redis is not configured")
	}

	result, err := renewRunLeaseScript.Run(ctx, s.redis, []string{collector.RunLeaseKey(strings.TrimSpace(runID))}, strings.TrimSpace(workerID), normalizeWorkerLeaseTTL(ttl).Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *pgRedisWorkerStore) LoadRunTask(ctx context.Context, runID string) (runTaskView, error) {
	if s == nil || s.db == nil {
		return runTaskView{}, errors.New("worker database is not configured")
	}

	row := s.db.QueryRow(ctx, workerLoadRunTaskSQL, strings.TrimSpace(runID))
	return scanRunTaskView(row)
}

func (s *pgRedisWorkerStore) RecordTaskSeenPost(ctx context.Context, taskID, postID, runID string, seenAt time.Time) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("worker database is not configured")
	}

	tag, err := s.db.Exec(ctx, workerInsertTaskSeenPostSQL, strings.TrimSpace(taskID), strings.TrimSpace(postID), nullableString(runID), seenAt.UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *pgRedisWorkerStore) RecordKeywordMonthlyUsage(ctx context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("worker database is not configured")
	}

	tag, err := s.db.Exec(ctx, workerInsertKeywordMonthlyUsageSQL, strings.TrimSpace(keyword), normalizeUsageMonth(usageMonth, seenAt), strings.TrimSpace(postID), nullableString(taskID), seenAt.UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *pgRedisWorkerStore) UpdateRunProgress(ctx context.Context, params updateRunProgressParams) error {
	if s == nil || s.db == nil {
		return errors.New("worker database is not configured")
	}

	var startedAt any
	if params.StartedAt != nil {
		startedAt = params.StartedAt.UTC()
	}

	_, err := s.db.Exec(ctx, workerUpdateRunProgressSQL,
		strings.TrimSpace(params.RunID),
		nullableString(params.Status),
		startedAt,
		nullableString(params.OutputPath),
		params.PageCount,
		params.FetchedCount,
		params.NewCount,
		params.DuplicateCount,
		nullableString(params.NextCursor),
	)
	return err
}

func (s *pgRedisWorkerStore) MarkRunFinished(ctx context.Context, params finishRunParams) error {
	if s == nil || s.pg == nil {
		return errors.New("worker database is not configured")
	}

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var completedCount any
	if params.CompletedCount != nil {
		completedCount = *params.CompletedCount
	}

	if _, err := tx.Exec(ctx, workerMarkRunFinishedSQL,
		strings.TrimSpace(params.RunID),
		strings.TrimSpace(params.RunStatus),
		nullableString(params.StopReason),
		params.EndedAt.UTC(),
		nullableString(params.OutputPath),
		params.PageCount,
		params.FetchedCount,
		params.NewCount,
		params.DuplicateCount,
		nullableString(params.NextCursor),
		nullableString(params.ErrorMessage),
	); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, workerUpdateTaskOnFinishSQL,
		strings.TrimSpace(params.TaskID),
		nullableString(params.TaskStatus),
		completedCount,
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func scanRunTaskView(row workerRow) (runTaskView, error) {
	var view runTaskView
	var frequencySeconds sql.NullInt32
	var since sql.NullString
	var until sql.NullString
	var requiredCount sql.NullInt64
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var stopReason sql.NullString
	var outputPath sql.NullString
	var nextCursor sql.NullString
	var errorMessage sql.NullString

	if err := row.Scan(
		&view.Task.TaskID,
		&view.Task.TaskType,
		&view.Task.Keyword,
		&view.Task.CreatedBy,
		&view.Task.Priority,
		&frequencySeconds,
		&since,
		&until,
		&requiredCount,
		&view.Task.CompletedCount,
		&view.Task.Status,
		&view.Run.RunID,
		&view.Run.RunNo,
		&view.Run.Status,
		&stopReason,
		&view.Run.ScheduledAt,
		&startedAt,
		&endedAt,
		&outputPath,
		&view.Run.PageCount,
		&view.Run.FetchedCount,
		&view.Run.NewCount,
		&view.Run.DuplicateCount,
		&nextCursor,
		&errorMessage,
	); err != nil {
		return runTaskView{}, err
	}

	view.Run.TaskID = view.Task.TaskID
	if frequencySeconds.Valid {
		view.Task.FrequencySeconds = cloneInt32Ptr(&frequencySeconds.Int32)
	}
	if since.Valid {
		view.Task.Since = since.String
	}
	if until.Valid {
		view.Task.Until = until.String
	}
	if requiredCount.Valid {
		view.Task.RequiredCount = cloneInt64Ptr(&requiredCount.Int64)
	}
	if stopReason.Valid {
		view.Run.StopReason = stopReason.String
	}
	if startedAt.Valid {
		view.Run.StartedAt = cloneTimePtr(&startedAt.Time)
	}
	if endedAt.Valid {
		view.Run.EndedAt = cloneTimePtr(&endedAt.Time)
	}
	if outputPath.Valid {
		view.Run.OutputPath = outputPath.String
	}
	if nextCursor.Valid {
		view.Run.NextCursor = nextCursor.String
	}
	if errorMessage.Valid {
		view.Run.ErrorMessage = errorMessage.String
	}

	return view, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func normalizeUsageMonth(usageMonth string, seenAt time.Time) string {
	if strings.TrimSpace(usageMonth) != "" {
		return strings.TrimSpace(usageMonth)
	}
	monthStart := time.Date(seenAt.UTC().Year(), seenAt.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	return monthStart.Format("2006-01-02")
}

const workerLeaseRunSQL = `
UPDATE collector.task_runs
SET status = 'leased'
WHERE run_id = $1
`

const workerLoadRunTaskSQL = `
SELECT
    t.task_id,
    t.task_type,
    t.keyword,
    t.created_by,
    t.priority,
    t.frequency_seconds,
    t.since,
    t.until,
    t.required_count,
    t.completed_count,
    t.status,
    r.run_id,
    r.run_no,
    r.status,
    r.stop_reason,
    r.scheduled_at,
    r.started_at,
    r.ended_at,
    r.output_path,
    r.page_count,
    r.fetched_count,
    r.new_count,
    r.duplicate_count,
    r.next_cursor,
    r.error_message
FROM collector.task_runs r
JOIN collector.tasks t ON t.task_id = r.task_id
WHERE r.run_id = $1
`

const workerInsertTaskSeenPostSQL = `
INSERT INTO collector.task_seen_posts (
    task_id,
    post_id,
    run_id,
    first_seen_at
) VALUES ($1, $2, $3, $4)
ON CONFLICT (task_id, post_id) DO NOTHING
`

const workerInsertKeywordMonthlyUsageSQL = `
INSERT INTO collector.keyword_monthly_usage (
    keyword,
    usage_month,
    post_id,
    task_id,
    collected_at
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (keyword, usage_month, post_id) DO NOTHING
`

const workerUpdateRunProgressSQL = `
UPDATE collector.task_runs
SET status = COALESCE($2, status),
    started_at = COALESCE($3::timestamptz, started_at),
    output_path = COALESCE($4, output_path),
    page_count = $5,
    fetched_count = $6,
    new_count = $7,
    duplicate_count = $8,
    next_cursor = COALESCE($9, next_cursor)
WHERE run_id = $1
`

const workerMarkRunFinishedSQL = `
UPDATE collector.task_runs
SET status = $2,
    stop_reason = $3,
    ended_at = $4,
    output_path = COALESCE($5, output_path),
    page_count = $6,
    fetched_count = $7,
    new_count = $8,
    duplicate_count = $9,
    next_cursor = COALESCE($10, next_cursor),
    error_message = COALESCE($11, error_message)
WHERE run_id = $1
`

const workerUpdateTaskOnFinishSQL = `
UPDATE collector.tasks
SET status = COALESCE($2, status),
    completed_count = COALESCE($3, completed_count),
    updated_at = NOW()
WHERE task_id = $1
`

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
