package internal

import (
	"context"
	"testing"
	"time"

	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/pkg/model"
	"xsonar/pkg/xlog"
)

func TestCreateTaskRejectsInvalidTaskType(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:    "task-1",
		TaskType:  "invalid",
		Keyword:   "openai",
		CreatedBy: "admin-user-1",
	})

	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestCreateTaskRejectsPriorityOutsideRange(t *testing.T) {
	svc, _ := newTestSchedulerService()

	for _, priority := range []int32{-1, 101} {
		_, svcErr := svc.createTask(context.Background(), createTaskRequest{
			TaskID:           "task-1",
			TaskType:         "periodic",
			Keyword:          "openai",
			Priority:         priority,
			FrequencySeconds: int32Ptr(60),
			CreatedBy:        "admin-user-1",
		})
		assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
	}
}

func TestCreateTaskRejectsMissingFrequencyForPeriodic(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:    "task-1",
		TaskType:  "periodic",
		Keyword:   "openai",
		CreatedBy: "admin-user-1",
	})

	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestCreateTaskRejectsMissingSinceUntilForRange(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:    "task-1",
		TaskType:  "range",
		Keyword:   "openai",
		CreatedBy: "admin-user-1",
	})

	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestCreateTaskRejectsMissingCreatedBy(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		FrequencySeconds: int32Ptr(60),
	})

	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestCreateTaskRejectsDuplicateTaskID(t *testing.T) {
	svc, _ := newTestSchedulerService()

	if _, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		FrequencySeconds: int32Ptr(60),
		CreatedBy:        "admin-user-1",
	}); svcErr != nil {
		t.Fatalf("seed createTask returned error: %v", svcErr)
	}

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		FrequencySeconds: int32Ptr(60),
		CreatedBy:        "admin-user-1",
	})

	assertSchedulerError(t, svcErr, model.CodeConflict)
}

func TestCreateTaskPreservesCreatedByInStore(t *testing.T) {
	svc, store := newTestSchedulerService()

	_, svcErr := svc.createTask(context.Background(), createTaskRequest{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		FrequencySeconds: int32Ptr(60),
		CreatedBy:        "admin-user-1",
	})
	if svcErr != nil {
		t.Fatalf("createTask returned error: %v", svcErr)
	}

	if store.lastCreatedTask == nil {
		t.Fatal("expected create task to reach store")
	}
	if store.lastCreatedTask.CreatedBy != "admin-user-1" {
		t.Fatalf("expected created_by admin-user-1, got %q", store.lastCreatedTask.CreatedBy)
	}
}

func TestGetTaskReturnsTaskFromStore(t *testing.T) {
	svc, store := newTestSchedulerService()

	createdTask := &task{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		Priority:         5,
		FrequencySeconds: int32Ptr(60),
		CreatedBy:        "admin-user-1",
		Status:           schedulerTaskStatusPending,
		CreatedAt:        time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC),
	}
	if _, svcErr := store.CreateTask(context.Background(), createdTask); svcErr != nil {
		t.Fatalf("seed task creation failed: %v", svcErr)
	}

	got, getErr := svc.getTask(context.Background(), getTaskRequest{TaskID: "task-1"})
	if getErr != nil {
		t.Fatalf("getTask returned error: %v", getErr)
	}

	gotTask := got.(*task)
	if gotTask.TaskID != createdTask.TaskID || gotTask.TaskType != createdTask.TaskType {
		t.Fatalf("unexpected task: got %+v want %+v", gotTask, createdTask)
	}
}

func TestGetTaskRejectsMissingTaskID(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.getTask(context.Background(), getTaskRequest{})
	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestListTaskRunsReturnsRunsFromStore(t *testing.T) {
	svc, store := newTestSchedulerService()

	createdTask := &task{
		TaskID:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		Priority:         5,
		FrequencySeconds: int32Ptr(60),
		CreatedBy:        "admin-user-1",
		Status:           schedulerTaskStatusPending,
		CreatedAt:        time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC),
	}
	if _, svcErr := store.CreateTask(context.Background(), createdTask); svcErr != nil {
		t.Fatalf("seed task creation failed: %v", svcErr)
	}
	store.addTaskRun(taskRun{
		RunID:       "run-1",
		TaskID:      "task-1",
		RunNo:       1,
		Status:      "succeeded",
		ScheduledAt: time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC),
	})
	store.addTaskRun(taskRun{
		RunID:       "run-2",
		TaskID:      "task-1",
		RunNo:       2,
		Status:      "failed",
		ScheduledAt: time.Date(2026, 4, 11, 11, 0, 0, 0, time.UTC),
	})

	got, listErr := svc.listTaskRuns(context.Background(), listTaskRunsRequest{TaskID: "task-1"})
	if listErr != nil {
		t.Fatalf("listTaskRuns returned error: %v", listErr)
	}

	result := got.(map[string]any)
	items := result["items"].([]taskRun)
	if len(items) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(items))
	}
	if items[0].RunID != "run-2" || items[1].RunID != "run-1" {
		t.Fatalf("unexpected run order: %#v", items)
	}
}

func TestListTaskRunsRejectsMissingTaskID(t *testing.T) {
	svc, _ := newTestSchedulerService()

	_, svcErr := svc.listTaskRuns(context.Background(), listTaskRunsRequest{})
	assertSchedulerError(t, svcErr, model.CodeInvalidRequest)
}

func TestSchedulerConfigDefaults(t *testing.T) {
	cfg := defaultConfig()
	if cfg.DispatchScanIntervalMS != 1000 {
		t.Fatalf("unexpected dispatch scan interval: %d", cfg.DispatchScanIntervalMS)
	}
	if cfg.QueueBacklogSoftLimit != 5000 || cfg.QueueBacklogHardLimit != 20000 {
		t.Fatalf("unexpected queue backlog defaults: %+v", cfg)
	}
	if cfg.QueueBacklogMaxLagMS != 300000 || cfg.LeaderLockTTLMS != 15000 {
		t.Fatalf("unexpected scheduler timing defaults: %+v", cfg)
	}
	if cfg.ListTaskRunsDefaultLimit != 50 {
		t.Fatalf("unexpected list task runs default limit: %d", cfg.ListTaskRunsDefaultLimit)
	}
}

func newTestSchedulerService() (*schedulerService, *fakeSchedulerStore) {
	store := newFakeSchedulerStore()
	svc := newSchedulerServiceWithStore(config.Config{}, xlog.NewStdout("scheduler-rpc-test"), store)
	return svc, store
}

func assertSchedulerError(t *testing.T, svcErr *serviceError, wantCode int) {
	t.Helper()

	if svcErr == nil {
		t.Fatal("expected error, got nil")
	}
	if svcErr.code != wantCode {
		t.Fatalf("expected error code %d, got %d", wantCode, svcErr.code)
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}
