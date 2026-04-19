package internal

import (
	"context"
	"testing"
	"time"

	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/pkg/xlog"
)

func TestDispatcherOnlyLeaderDispatchesDueTasks(t *testing.T) {
	now := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newFakeSchedulerStore()
	mustSeedTask(t, store, testRangeTask("task_range_1", now.Add(-time.Minute)))

	first := newDispatcher(config.Config{
		QueueBacklogHardLimit: 10,
		QueueBacklogMaxLagMS:  300000,
		LeaderLockTTLMS:       15000,
	}, xlog.NewStdout("scheduler-rpc-test"), store)
	first.workerID = "scheduler-1"

	second := newDispatcher(config.Config{
		QueueBacklogHardLimit: 10,
		QueueBacklogMaxLagMS:  300000,
		LeaderLockTTLMS:       15000,
	}, xlog.NewStdout("scheduler-rpc-test"), store)
	second.workerID = "scheduler-2"

	if err := first.tick(context.Background(), now); err != nil {
		t.Fatalf("first tick returned error: %v", err)
	}
	if err := second.tick(context.Background(), now); err != nil {
		t.Fatalf("second tick returned error: %v", err)
	}

	runs, svcErr := store.ListTaskRuns(context.Background(), "task_range_1", 10)
	if svcErr != nil {
		t.Fatalf("ListTaskRuns returned error: %v", svcErr)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly one dispatched run, got %d", len(runs))
	}
	if len(store.enqueuedRunIDs) != 1 {
		t.Fatalf("expected exactly one queued stream publish, got %d", len(store.enqueuedRunIDs))
	}
}

func TestDispatcherSkipsPeriodicTaskWithOpenRun(t *testing.T) {
	now := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newFakeSchedulerStore()
	mustSeedTask(t, store, testPeriodicTask("task_periodic_1", now.Add(-time.Minute), 30))
	store.addTaskRun(taskRun{
		RunID:       "run_open_1",
		TaskID:      "task_periodic_1",
		RunNo:       1,
		Status:      RunStatusQueued,
		ScheduledAt: now.Add(-30 * time.Second),
	})

	dispatcher := newDispatcher(config.Config{
		QueueBacklogHardLimit: 10,
		QueueBacklogMaxLagMS:  300000,
		LeaderLockTTLMS:       15000,
	}, xlog.NewStdout("scheduler-rpc-test"), store)

	if err := dispatcher.tick(context.Background(), now); err != nil {
		t.Fatalf("tick returned error: %v", err)
	}

	runs, svcErr := store.ListTaskRuns(context.Background(), "task_periodic_1", 10)
	if svcErr != nil {
		t.Fatalf("ListTaskRuns returned error: %v", svcErr)
	}
	if len(runs) != 1 {
		t.Fatalf("expected open run to suppress new dispatch, got %d runs", len(runs))
	}
	if len(store.enqueuedRunIDs) != 0 {
		t.Fatalf("expected no stream publish while open run exists, got %d", len(store.enqueuedRunIDs))
	}
}

func TestDispatcherPausesOnHardBacklog(t *testing.T) {
	now := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newFakeSchedulerStore()
	mustSeedTask(t, store, testRangeTask("task_range_1", now.Add(-time.Minute)))

	for i := 0; i < 3; i++ {
		taskID := "backlog_task_" + string(rune('a'+i))
		mustSeedTask(t, store, testPeriodicTask(taskID, now.Add(-time.Minute), 60))
		store.addTaskRun(taskRun{
			RunID:       "run_backlog_" + string(rune('a'+i)),
			TaskID:      taskID,
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: now.Add(-2 * time.Minute),
		})
	}

	dispatcher := newDispatcher(config.Config{
		QueueBacklogHardLimit: 3,
		QueueBacklogMaxLagMS:  300000,
		LeaderLockTTLMS:       15000,
	}, xlog.NewStdout("scheduler-rpc-test"), store)

	if err := dispatcher.tick(context.Background(), now); err != nil {
		t.Fatalf("tick returned error: %v", err)
	}

	runs, svcErr := store.ListTaskRuns(context.Background(), "task_range_1", 10)
	if svcErr != nil {
		t.Fatalf("ListTaskRuns returned error: %v", svcErr)
	}
	if len(runs) != 0 {
		t.Fatalf("expected hard backlog protection to prevent dispatch, got %d runs", len(runs))
	}

	task, svcErr := store.GetTask(context.Background(), "task_range_1")
	if svcErr != nil {
		t.Fatalf("GetTask returned error: %v", svcErr)
	}
	if task.Status != TaskStatusPending {
		t.Fatalf("expected task to remain pending under hard backlog, got %q", task.Status)
	}
}

func TestDispatcherCoalescesMissedPeriodicTicks(t *testing.T) {
	now := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newFakeSchedulerStore()
	mustSeedTask(t, store, testPeriodicTask("task_periodic_1", now.Add(-5*time.Minute), 30))

	dispatcher := newDispatcher(config.Config{
		QueueBacklogHardLimit: 10,
		QueueBacklogMaxLagMS:  300000,
		LeaderLockTTLMS:       15000,
	}, xlog.NewStdout("scheduler-rpc-test"), store)

	if err := dispatcher.tick(context.Background(), now); err != nil {
		t.Fatalf("first tick returned error: %v", err)
	}
	if err := dispatcher.tick(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatalf("second tick returned error: %v", err)
	}

	runs, svcErr := store.ListTaskRuns(context.Background(), "task_periodic_1", 10)
	if svcErr != nil {
		t.Fatalf("ListTaskRuns returned error: %v", svcErr)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one coalesced periodic run, got %d", len(runs))
	}
	if runs[0].RunNo != 1 || runs[0].Status != RunStatusQueued {
		t.Fatalf("unexpected run payload: %#v", runs[0])
	}
	if len(store.enqueuedRunIDs) != 1 {
		t.Fatalf("expected one stream publish, got %d", len(store.enqueuedRunIDs))
	}

	task, svcErr := store.GetTask(context.Background(), "task_periodic_1")
	if svcErr != nil {
		t.Fatalf("GetTask returned error: %v", svcErr)
	}
	wantNextRunAt := now.Add(30 * time.Second)
	if task.NextRunAt == nil || !task.NextRunAt.Equal(wantNextRunAt) {
		t.Fatalf("expected next_run_at %s, got %#v", wantNextRunAt, task.NextRunAt)
	}
}

func mustSeedTask(t *testing.T, store *fakeSchedulerStore, item *task) {
	t.Helper()

	if _, svcErr := store.CreateTask(context.Background(), item); svcErr != nil {
		t.Fatalf("CreateTask returned error: %v", svcErr)
	}
}

func testPeriodicTask(taskID string, nextRunAt time.Time, frequencySeconds int32) *task {
	return &task{
		TaskID:           taskID,
		TaskType:         TaskTypePeriodic,
		Keyword:          "openai",
		Priority:         10,
		FrequencySeconds: int32Ptr(frequencySeconds),
		CreatedBy:        "admin",
		Status:           TaskStatusPending,
		CreatedAt:        nextRunAt.Add(-time.Hour),
		UpdatedAt:        nextRunAt.Add(-time.Hour),
		NextRunAt:        cloneTimePtr(&nextRunAt),
	}
}

func testRangeTask(taskID string, nextRunAt time.Time) *task {
	requiredCount := int64(100)
	return &task{
		TaskID:         taskID,
		TaskType:       TaskTypeRange,
		Keyword:        "openai",
		Priority:       10,
		Since:          "since:2026-04-12_00:00:00_UTC",
		Until:          "until:2026-04-13_00:00:00_UTC",
		RequiredCount:  &requiredCount,
		CreatedBy:      "admin",
		Status:         TaskStatusPending,
		CreatedAt:      nextRunAt.Add(-time.Hour),
		UpdatedAt:      nextRunAt.Add(-time.Hour),
		NextRunAt:      cloneTimePtr(&nextRunAt),
		CompletedCount: 0,
	}
}
