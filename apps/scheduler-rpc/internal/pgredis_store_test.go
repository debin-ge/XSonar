package internal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"

	"xsonar/pkg/model"
	"xsonar/pkg/xlog"
)

func TestLoadSchedulerStoreConfigFallsBackToCommonEnv(t *testing.T) {
	t.Setenv("COMMON_STORE_BACKEND", "pgredis")
	t.Setenv("COMMON_POSTGRES_DSN", "postgres://scheduler:secret@127.0.0.1:5432/xsonar")
	t.Setenv("COMMON_REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("COMMON_REDIS_PASSWORD", "redis-secret")
	t.Setenv("COMMON_REDIS_DB", "2")
	t.Setenv("COMMON_LEADER_LOCK_TTL_MS", "2500")

	cfg := loadSchedulerStoreConfig()

	if cfg.Backend != "pgredis" {
		t.Fatalf("expected backend pgredis, got %q", cfg.Backend)
	}
	if cfg.PostgresDSN != "postgres://scheduler:secret@127.0.0.1:5432/xsonar" {
		t.Fatalf("expected postgres dsn to come from COMMON_POSTGRES_DSN, got %q", cfg.PostgresDSN)
	}
	if cfg.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("expected redis addr to come from COMMON_REDIS_ADDR, got %q", cfg.RedisAddr)
	}
	if cfg.RedisPassword != "redis-secret" {
		t.Fatalf("expected redis password to come from COMMON_REDIS_PASSWORD, got %q", cfg.RedisPassword)
	}
	if cfg.RedisDB != 2 {
		t.Fatalf("expected redis db to be 2, got %d", cfg.RedisDB)
	}
	if cfg.LeaderLockTTL != 2500*time.Millisecond {
		t.Fatalf("expected leader lock ttl to be 2500ms, got %s", cfg.LeaderLockTTL)
	}
}

func TestLoadSchedulerStoreConfigDefaultsToMemoryWhenNoBackendConfigured(t *testing.T) {
	cfg := loadSchedulerStoreConfig()

	if cfg.Backend != "memory" {
		t.Fatalf("expected memory backend by default, got %q", cfg.Backend)
	}
}

func TestNewServicePanicsOnUnsupportedSchedulerBackend(t *testing.T) {
	t.Setenv("COMMON_STORE_BACKEND", "bogus")

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected unsupported backend to panic")
		}
	}()

	_ = newService(xlog.NewStdout("scheduler-rpc-test"))
}

func TestNewServicePanicsWhenPGRedisConfigIsIncomplete(t *testing.T) {
	t.Setenv("COMMON_STORE_BACKEND", "pgredis")
	t.Setenv("COMMON_POSTGRES_DSN", "")
	t.Setenv("COMMON_REDIS_ADDR", "127.0.0.1:6379")

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected incomplete pgredis config to panic")
		}
	}()

	_ = newService(xlog.NewStdout("scheduler-rpc-test"))
}

func TestSchedulerDuplicateKeyErrorMapsToConflict(t *testing.T) {
	svcErr := schedulerServiceErrorFromErr(&pgconn.PgError{Code: "23505"})
	if svcErr == nil {
		t.Fatal("expected duplicate key error to map to a service error")
	}
	if svcErr.statusCode != 409 {
		t.Fatalf("expected conflict status, got %d", svcErr.statusCode)
	}
	if svcErr.code != model.CodeConflict {
		t.Fatalf("expected conflict code, got %d", svcErr.code)
	}

	if got := schedulerServiceErrorFromErr(errors.New("other failure")); got == nil || got.code == model.CodeConflict {
		t.Fatalf("expected unrelated errors to map to a non-conflict scheduler error, got %#v", got)
	}
}

func TestSchedulerLeaderLockUsesRedisTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	acquired, err := store.TryBecomeLeader(ctx, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("TryBecomeLeader returned error: %v", err)
	}
	if !acquired {
		t.Fatal("expected first leader acquisition to succeed")
	}
	if !server.Exists(schedulerLeaderLockKey()) {
		t.Fatal("expected leader lock key to exist")
	}
	if ttl := server.TTL(schedulerLeaderLockKey()); ttl != 5*time.Second {
		t.Fatalf("expected leader lock ttl to be 5s, got %s", ttl)
	}

	acquired, err = store.TryBecomeLeader(ctx, "worker-b", 5*time.Second)
	if err != nil {
		t.Fatalf("TryBecomeLeader returned error: %v", err)
	}
	if acquired {
		t.Fatal("expected second leader acquisition to fail while lock is held")
	}

	if err := store.ReleaseLeader(ctx, "worker-b"); err != nil {
		t.Fatalf("ReleaseLeader returned error for stale owner: %v", err)
	}
	if !server.Exists(schedulerLeaderLockKey()) {
		t.Fatal("expected stale owner release to keep the lock")
	}

	if err := store.ReleaseLeader(ctx, "worker-a"); err != nil {
		t.Fatalf("ReleaseLeader returned error for active owner: %v", err)
	}
	if server.Exists(schedulerLeaderLockKey()) {
		t.Fatal("expected matching owner to release the lock")
	}
}

func TestSchedulerCreateTaskUsesPendingStatusAndNextRunAtNow(t *testing.T) {
	fakeNow := time.Date(2026, 4, 11, 9, 30, 0, 0, time.UTC)
	cases := []struct {
		name      string
		item      task
		wantType  string
		wantFreq  *int32
		wantCount *int64
	}{
		{
			name: "periodic",
			item: task{
				TaskID:           "task_periodic",
				TaskType:         TaskTypePeriodic,
				Keyword:          "openai",
				CreatedBy:        "admin",
				Priority:         10,
				FrequencySeconds: pgInt32Ptr(30),
			},
			wantType: TaskTypePeriodic,
			wantFreq: pgInt32Ptr(30),
		},
		{
			name: "range",
			item: task{
				TaskID:        "task_range",
				TaskType:      TaskTypeRange,
				Keyword:       "openai",
				CreatedBy:     "admin",
				Priority:      20,
				Since:         "2026-04-01",
				Until:         "2026-04-02",
				RequiredCount: pgInt64Ptr(100),
			},
			wantType:  TaskTypeRange,
			wantCount: pgInt64Ptr(100),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &pgRedisStore{
				db: fakeSchedulerDB{
					queryRowFn: func(ctx context.Context, sql string, args ...any) schedulerRow {
						t.Helper()
						if !strings.Contains(sql, "INSERT INTO collector.tasks") {
							t.Fatalf("expected task insert sql, got %q", sql)
						}
						if !strings.Contains(sql, "NOW()") {
							t.Fatalf("expected task insert sql to set timestamps with NOW(), got %q", sql)
						}
						if !strings.Contains(sql, "pending") {
							t.Fatalf("expected task insert sql to default status to pending, got %q", sql)
						}
						if args[0] != tc.item.TaskID {
							t.Fatalf("unexpected task_id arg: %#v", args[0])
						}
						if args[1] != tc.wantType {
							t.Fatalf("unexpected task_type arg: %#v", args[1])
						}
						if args[2] != tc.item.Keyword {
							t.Fatalf("unexpected keyword arg: %#v", args[2])
						}
						if args[3] != tc.item.CreatedBy {
							t.Fatalf("unexpected created_by arg: %#v", args[3])
						}
						if args[4] != tc.item.Priority {
							t.Fatalf("unexpected priority arg: %#v", args[4])
						}
						if tc.wantFreq == nil && args[5] != nil {
							t.Fatalf("expected nil frequency arg, got %#v", args[5])
						}
						if tc.wantFreq != nil && args[5] != *tc.wantFreq {
							t.Fatalf("unexpected frequency arg: %#v", args[5])
						}
						if tc.item.Since == "" && args[6] != nil {
							t.Fatalf("expected nil since arg, got %#v", args[6])
						}
						if tc.item.Since != "" && args[6] != tc.item.Since {
							t.Fatalf("unexpected since arg: %#v", args[6])
						}
						if tc.item.Until == "" && args[7] != nil {
							t.Fatalf("expected nil until arg, got %#v", args[7])
						}
						if tc.item.Until != "" && args[7] != tc.item.Until {
							t.Fatalf("unexpected until arg: %#v", args[7])
						}
						if tc.wantCount == nil && args[8] != nil {
							t.Fatalf("expected nil required_count arg, got %#v", args[8])
						}
						if tc.wantCount != nil && args[8] != *tc.wantCount {
							t.Fatalf("unexpected required_count arg: %#v", args[8])
						}

						return fakeSchedulerRow{
							scanFn: func(dest ...any) error {
								assignTaskRowValues(dest, tc.item, fakeNow)
								return nil
							},
						}
					},
				},
			}

			got, err := store.CreateTask(context.Background(), &tc.item)
			if err != nil {
				t.Fatalf("CreateTask returned error: %v", err)
			}
			if got.Status != TaskStatusPending {
				t.Fatalf("expected pending task status, got %q", got.Status)
			}
			if got.NextRunAt == nil || !got.NextRunAt.Equal(fakeNow) {
				t.Fatalf("expected next_run_at to be populated, got %#v", got.NextRunAt)
			}
			if got.TaskType != tc.wantType {
				t.Fatalf("unexpected task type: %q", got.TaskType)
			}
			if tc.wantFreq == nil && got.FrequencySeconds != nil {
				t.Fatalf("expected nil frequency_seconds, got %#v", got.FrequencySeconds)
			}
			if tc.wantFreq != nil && got.FrequencySeconds == nil {
				t.Fatal("expected frequency_seconds to be populated")
			}
			if tc.wantFreq != nil && *got.FrequencySeconds != *tc.wantFreq {
				t.Fatalf("unexpected frequency_seconds: %d", *got.FrequencySeconds)
			}
			if tc.wantCount == nil && got.RequiredCount != nil {
				t.Fatalf("expected nil required_count, got %#v", got.RequiredCount)
			}
			if tc.wantCount != nil && got.RequiredCount == nil {
				t.Fatal("expected required_count to be populated")
			}
			if tc.wantCount != nil && *got.RequiredCount != *tc.wantCount {
				t.Fatalf("unexpected required_count: %d", *got.RequiredCount)
			}
		})
	}
}

func TestSchedulerGetTaskScansNullableColumns(t *testing.T) {
	fakeNow := time.Date(2026, 4, 11, 9, 45, 0, 0, time.UTC)
	store := &pgRedisStore{
		db: fakeSchedulerDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) schedulerRow {
				if !strings.Contains(sql, "FROM collector.tasks") {
					t.Fatalf("expected get task sql, got %q", sql)
				}
				if args[0] != "task_1" {
					t.Fatalf("unexpected task_id arg: %#v", args[0])
				}
				return fakeSchedulerRow{
					scanFn: func(dest ...any) error {
						assignTaskRowValues(dest, task{
							TaskID:           "task_1",
							TaskType:         TaskTypePeriodic,
							Keyword:          "openai",
							CreatedBy:        "admin",
							Priority:         10,
							FrequencySeconds: pgInt32Ptr(30),
						}, fakeNow)
						return nil
					},
				}
			},
		},
	}

	got, err := store.GetTask(context.Background(), "task_1")
	if err != nil {
		t.Fatalf("GetTask returned error: %v", err)
	}
	if got.TaskID != "task_1" || got.Keyword != "openai" {
		t.Fatalf("unexpected task view: %#v", got)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(fakeNow) {
		t.Fatalf("expected next run time to be scanned, got %#v", got.NextRunAt)
	}
}

func TestSchedulerListTaskRunsUsesRequestedLimit(t *testing.T) {
	fakeNow := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
	store := &pgRedisStore{
		db: fakeSchedulerDB{
			queryFn: func(ctx context.Context, sql string, args ...any) (schedulerRows, error) {
				if !strings.Contains(sql, "FROM collector.task_runs") {
					t.Fatalf("expected task runs sql, got %q", sql)
				}
				if args[0] != "task_1" {
					t.Fatalf("unexpected task_id arg: %#v", args[0])
				}
				if args[1] != 25 {
					t.Fatalf("unexpected limit arg: %#v", args[1])
				}
				return &fakeSchedulerRows{
					rows: []fakeSchedulerRow{
						{
							scanFn: func(dest ...any) error {
								assignTaskRunRowValues(dest, taskRunSeed{
									RunID:       "run_1",
									TaskID:      "task_1",
									RunNo:       1,
									Status:      RunStatusQueued,
									ScheduledAt: fakeNow,
								})
								return nil
							},
						},
					},
				}, nil
			},
		},
	}

	got, err := store.ListTaskRuns(context.Background(), "task_1", 25)
	if err != nil {
		t.Fatalf("ListTaskRuns returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(got))
	}
	if got[0].RunID != "run_1" || got[0].TaskID != "task_1" {
		t.Fatalf("unexpected task run view: %#v", got[0])
	}
}

type fakeSchedulerDB struct {
	execFn     func(context.Context, string, ...any) (pgconn.CommandTag, error)
	queryRowFn func(context.Context, string, ...any) schedulerRow
	queryFn    func(context.Context, string, ...any) (schedulerRows, error)
}

func (d fakeSchedulerDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if d.execFn != nil {
		return d.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (d fakeSchedulerDB) QueryRow(ctx context.Context, sql string, args ...any) schedulerRow {
	if d.queryRowFn != nil {
		return d.queryRowFn(ctx, sql, args...)
	}
	return fakeSchedulerRow{}
}

func (d fakeSchedulerDB) Query(ctx context.Context, sql string, args ...any) (schedulerRows, error) {
	if d.queryFn != nil {
		return d.queryFn(ctx, sql, args...)
	}
	return &fakeSchedulerRows{}, nil
}

type fakeSchedulerRow struct {
	scanFn func(dest ...any) error
}

func (r fakeSchedulerRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type fakeSchedulerRows struct {
	rows []fakeSchedulerRow
	idx  int
	err  error
}

func (r *fakeSchedulerRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeSchedulerRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("scan called before Next")
	}
	return r.rows[r.idx-1].Scan(dest...)
}

func (r *fakeSchedulerRows) Err() error {
	return r.err
}

func (r *fakeSchedulerRows) Close() {}

type taskRunSeed struct {
	RunID       string
	TaskID      string
	RunNo       int64
	Status      string
	ScheduledAt time.Time
}

func assignTaskRowValues(dest []any, item task, now time.Time) {
	if len(dest) != 15 {
		panic("unexpected task row destination count")
	}

	*(dest[0].(*string)) = item.TaskID
	*(dest[1].(*string)) = item.TaskType
	*(dest[2].(*string)) = item.Keyword
	*(dest[3].(*string)) = item.CreatedBy
	*(dest[4].(*int32)) = item.Priority

	if item.FrequencySeconds == nil {
		*(dest[5].(*sql.NullInt32)) = sql.NullInt32{}
	} else {
		*(dest[5].(*sql.NullInt32)) = sql.NullInt32{Int32: *item.FrequencySeconds, Valid: true}
	}
	if item.Since == "" {
		*(dest[6].(*sql.NullString)) = sql.NullString{}
	} else {
		*(dest[6].(*sql.NullString)) = sql.NullString{String: item.Since, Valid: true}
	}
	if item.Until == "" {
		*(dest[7].(*sql.NullString)) = sql.NullString{}
	} else {
		*(dest[7].(*sql.NullString)) = sql.NullString{String: item.Until, Valid: true}
	}
	if item.RequiredCount == nil {
		*(dest[8].(*sql.NullInt64)) = sql.NullInt64{}
	} else {
		*(dest[8].(*sql.NullInt64)) = sql.NullInt64{Int64: *item.RequiredCount, Valid: true}
	}
	*(dest[9].(*int64)) = 0
	*(dest[10].(*string)) = TaskStatusPending
	*(dest[11].(*sql.NullTime)) = sql.NullTime{Time: now, Valid: true}
	*(dest[12].(*sql.NullTime)) = sql.NullTime{}
	*(dest[13].(*time.Time)) = now.UTC()
	*(dest[14].(*time.Time)) = now.UTC()
}

func assignTaskRunRowValues(dest []any, seed taskRunSeed) {
	if len(dest) != 15 {
		panic("unexpected task run row destination count")
	}

	*(dest[0].(*string)) = seed.RunID
	*(dest[1].(*string)) = seed.TaskID
	*(dest[2].(*int64)) = seed.RunNo
	*(dest[3].(*string)) = seed.Status
	*(dest[4].(*sql.NullString)) = sql.NullString{}
	*(dest[5].(*time.Time)) = seed.ScheduledAt
	*(dest[6].(*sql.NullTime)) = sql.NullTime{}
	*(dest[7].(*sql.NullTime)) = sql.NullTime{}
	*(dest[8].(*sql.NullString)) = sql.NullString{}
	*(dest[9].(*int64)) = 0
	*(dest[10].(*int64)) = 0
	*(dest[11].(*int64)) = 0
	*(dest[12].(*int64)) = 0
	*(dest[13].(*sql.NullString)) = sql.NullString{}
	*(dest[14].(*sql.NullString)) = sql.NullString{}
}

func pgInt32Ptr(v int32) *int32 {
	return &v
}

func pgInt64Ptr(v int64) *int64 {
	return &v
}
