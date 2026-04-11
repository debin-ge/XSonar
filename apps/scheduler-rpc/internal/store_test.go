package internal

import (
	"context"
	"testing"
)

func TestFakeSchedulerStoreLastCreatedTaskTracksCommittedCreate(t *testing.T) {
	store := NewFakeSchedulerStore()

	if _, svcErr := store.CreateTask(context.Background(), &task{
		TaskID:    "task-1",
		TaskType:  schedulerTaskTypePeriodic,
		Keyword:   "openai",
		CreatedBy: "admin-user-1",
	}); svcErr != nil {
		t.Fatalf("expected first create to succeed: %v", svcErr)
	}

	if got := store.LastCreatedTaskCreatedBy(); got != "admin-user-1" {
		t.Fatalf("expected last created task created_by admin-user-1, got %q", got)
	}

	if _, svcErr := store.CreateTask(context.Background(), &task{
		TaskID:    "task-1",
		TaskType:  schedulerTaskTypePeriodic,
		Keyword:   "openai",
		CreatedBy: "admin-user-2",
	}); svcErr == nil {
		t.Fatal("expected duplicate create to fail")
	}

	if got := store.LastCreatedTaskCreatedBy(); got != "admin-user-1" {
		t.Fatalf("expected last successful create to stay admin-user-1, got %q", got)
	}
}
