package server

import (
	"context"
	"testing"

	schedulerinternal "xsonar/apps/scheduler-rpc/internal"
	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/apps/scheduler-rpc/internal/svc"
	"xsonar/pkg/proto/schedulerpb"
	"xsonar/pkg/xlog"
)

func TestCreateTaskMapsCreatedByToInternalService(t *testing.T) {
	store := schedulerinternal.NewFakeSchedulerStore()
	logger := xlog.NewStdout("scheduler-rpc-test")
	svcCtx := &svc.ServiceContext{
		Logger:  logger,
		Service: schedulerinternal.NewSchedulerServiceWithStore(config.Config{}, logger, store),
	}

	server := NewSchedulerServiceServer(svcCtx)
	resp, err := server.CreateTask(context.Background(), &schedulerpb.CreateTaskRequest{
		TaskId:           "task-1",
		TaskType:         "periodic",
		Keyword:          "openai",
		Priority:         5,
		FrequencySeconds: int32Ptr(60),
		PerRunCount:      int64Ptr(25),
		CreatedBy:        "admin-user-1",
	})
	if err != nil {
		t.Fatalf("CreateTask returned error: %v", err)
	}
	if resp.GetCode() != 0 {
		t.Fatalf("unexpected response code: %d", resp.GetCode())
	}
	if got := store.LastCreatedTaskCreatedBy(); got != "admin-user-1" {
		t.Fatalf("expected created_by to be forwarded, got %q", got)
	}
	if got := store.LastCreatedTaskPerRunCount(); got == nil || *got != 25 {
		t.Fatalf("expected per_run_count to be forwarded, got %#v", got)
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}
