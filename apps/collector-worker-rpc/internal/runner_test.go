package internal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/xlog"
)

func TestRunnerStopsPeriodicRunAtPageLimit(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_periodic_1",
			TaskType: TaskTypePeriodic,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_1",
			TaskID:      "task_periodic_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	runner := newRunner(testRunnerConfig(root, 2), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets":      []map[string]any{{"id": "post_1", "text": "one"}},
					"next_cursor": "cursor_1",
				},
			},
			{
				Body: map[string]any{
					"tweets":      []map[string]any{{"id": "post_2", "text": "two"}},
					"next_cursor": "cursor_2",
				},
			},
		},
	}, "worker-1")

	if err := runner.run(context.Background(), "run_periodic_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "page_limit" {
		t.Fatalf("expected stop_reason page_limit, got %q", view.Run.StopReason)
	}
	if view.Run.PageCount != 2 {
		t.Fatalf("expected page_count 2, got %d", view.Run.PageCount)
	}
	data, err := os.ReadFile(view.Run.OutputPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %q", string(data))
	}
}

func TestRunnerPeriodicRunWritesOnlyFirstSeenPosts(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_periodic_1",
			TaskType: TaskTypePeriodic,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_1",
			TaskID:      "task_periodic_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})
	if _, err := store.RecordTaskSeenPost(context.Background(), "task_periodic_1", "post_2", "old_run", time.Now().UTC()); err != nil {
		t.Fatalf("RecordTaskSeenPost returned error: %v", err)
	}

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_1", "text": "one"},
						{"id": "post_2", "text": "two"},
					},
				},
			},
		},
	}, "worker-1")

	if err := runner.run(context.Background(), "run_periodic_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.NewCount != 1 || view.Run.DuplicateCount != 1 {
		t.Fatalf("unexpected counts: %+v", view.Run)
	}
	data, err := os.ReadFile(view.Run.OutputPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 NDJSON line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], `"post_id":"post_1"`) {
		t.Fatalf("expected first-seen post to be written, got %q", lines[0])
	}
}

func TestRunnerRangeRunPublishesSingleFinalFile(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_range_1",
			TaskType: TaskTypeRange,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_range_1",
			TaskID:      "task_range_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_1", "text": "one"},
						{"id": "post_2", "text": "two"},
					},
				},
			},
		},
	}, "worker-1")

	if err := runner.run(context.Background(), "run_range_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_range_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	wantPath := filepath.Join(root, "task_range_1", "task_range_1.ndjson")
	if view.Run.OutputPath != wantPath {
		t.Fatalf("expected output path %q, got %q", wantPath, view.Run.OutputPath)
	}
	if _, err := os.Stat(wantPath + ".part"); !os.IsNotExist(err) {
		t.Fatalf("expected .part file to be gone after success, got %v", err)
	}
	if view.Task.Status != TaskStatusSucceeded || view.Run.Status != RunStatusSucceeded {
		t.Fatalf("unexpected terminal state: task=%q run=%q", view.Task.Status, view.Run.Status)
	}
}

func TestRunnerStopsRangeRunWhenRequiredCountReached(t *testing.T) {
	root := t.TempDir()
	requiredCount := int64(2)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:         "task_range_required_1",
			TaskType:       TaskTypeRange,
			Keyword:        "openai",
			RequiredCount:  &requiredCount,
			CompletedCount: 0,
			Status:         TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_range_required_1",
			TaskID:      "task_range_required_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_1", "text": "one"},
						{"id": "post_2", "text": "two"},
						{"id": "post_3", "text": "three"},
					},
					"next_cursor": "cursor_1",
				},
			},
		},
	}
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, provider, "worker-1")

	if err := runner.run(context.Background(), "run_range_required_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_range_required_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "required_count_reached" {
		t.Fatalf("expected stop_reason required_count_reached, got %q", view.Run.StopReason)
	}
	if view.Run.NewCount != 2 {
		t.Fatalf("expected new_count 2, got %d", view.Run.NewCount)
	}
	if view.Task.CompletedCount != 2 {
		t.Fatalf("expected completed_count 2, got %d", view.Task.CompletedCount)
	}
	if provider.calls != 1 {
		t.Fatalf("expected provider to stop after 1 page, got %d calls", provider.calls)
	}
	data, err := os.ReadFile(view.Run.OutputPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %q", string(data))
	}
}

func TestRunnerStopsPeriodicTaskWhenRequiredCountReached(t *testing.T) {
	root := t.TempDir()
	requiredCount := int64(2)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:         "task_periodic_required_1",
			TaskType:       TaskTypePeriodic,
			Keyword:        "openai",
			RequiredCount:  &requiredCount,
			CompletedCount: 1,
			Status:         TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_required_1",
			TaskID:      "task_periodic_required_1",
			RunNo:       2,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_2", "text": "two"},
						{"id": "post_3", "text": "three"},
					},
					"next_cursor": "cursor_1",
				},
			},
		},
	}, "worker-1")

	if err := runner.run(context.Background(), "run_periodic_required_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_required_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "required_count_reached" {
		t.Fatalf("expected stop_reason required_count_reached, got %q", view.Run.StopReason)
	}
	if view.Task.Status != TaskStatusSucceeded {
		t.Fatalf("expected task status succeeded, got %q", view.Task.Status)
	}
	if view.Task.CompletedCount != 2 {
		t.Fatalf("expected completed_count 2, got %d", view.Task.CompletedCount)
	}
}

func TestRunnerRecordsKeywordUsageWithMonthStartDate(t *testing.T) {
	root := t.TempDir()
	store := &capturingWorkerStore{memoryWorkerStore: newMemoryWorkerStore()}
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_usage_month_1",
			TaskType: TaskTypeRange,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_usage_month_1",
			TaskID:      "task_usage_month_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_1", "text": "one"}},
				},
			},
		},
	}, "worker-1")

	if err := runner.run(context.Background(), "run_usage_month_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	want := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	if len(store.recordedUsageMonths) != 1 {
		t.Fatalf("expected 1 usage month record, got %d", len(store.recordedUsageMonths))
	}
	if store.recordedUsageMonths[0] != want {
		t.Fatalf("expected usage month %q, got %q", want, store.recordedUsageMonths[0])
	}
}

func TestRunnerResumesExistingPartFileAfterLeaseTakeover(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_range_1", "task_range_1.ndjson")
	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	if err := writer.Append(map[string]any{"task_id": "task_range_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	startedAt := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_range_1",
			TaskType: TaskTypeRange,
			Keyword:  "openai",
			Status:   TaskStatusRunning,
		},
		Run: workerRun{
			RunID:        "run_range_1",
			TaskID:       "task_range_1",
			RunNo:        1,
			Status:       RunStatusRunning,
			ScheduledAt:  startedAt,
			StartedAt:    &startedAt,
			OutputPath:   finalPath,
			PageCount:    1,
			FetchedCount: 1,
			NewCount:     1,
			NextCursor:   "cursor_2",
		},
	})

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_2", "text": "two"}},
				},
			},
		},
	}, "worker-2")

	if err := runner.run(context.Background(), "run_range_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines after takeover, got %d", len(lines))
	}
}

type providerPage struct {
	StatusCode int
	Body       map[string]any
}

type fakePolicyResolver struct{}

func (fakePolicyResolver) ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error) {
	_ = ctx
	_ = req
	return envelopeResponse(testedPolicyPayload()), nil
}

type fakeProviderExecutor struct {
	responses []providerPage
	calls     int
}

type capturingWorkerStore struct {
	*memoryWorkerStore
	recordedUsageMonths []string
}

func (s *capturingWorkerStore) RecordKeywordMonthlyUsage(ctx context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error) {
	s.recordedUsageMonths = append(s.recordedUsageMonths, usageMonth)
	return s.memoryWorkerStore.RecordKeywordMonthlyUsage(ctx, keyword, usageMonth, postID, taskID, seenAt)
}

func (f *fakeProviderExecutor) ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error) {
	_ = ctx
	_ = req
	if f.calls >= len(f.responses) {
		return nil, nil
	}

	page := f.responses[f.calls]
	f.calls++
	statusCode := page.StatusCode
	if statusCode == 0 {
		statusCode = 200
	}
	return envelopeResponse(map[string]any{
		"status_code":          statusCode,
		"result_code":          "UPSTREAM_OK",
		"body":                 page.Body,
		"upstream_duration_ms": 5,
	}), nil
}

func testedPolicyPayload() map[string]any {
	return map[string]any{
		"policy_key":       "search_tweets_v1",
		"upstream_method":  "GET",
		"upstream_path":    "/base/apitools/search",
		"default_params":   map[string]any{"resFormat": "json"},
		"provider_name":    "fapi.uk",
		"provider_api_key": "provider-key-1",
	}
}

func envelopeResponse(data any) *clients.EnvelopeResponse {
	body, _ := json.Marshal(data)
	return &clients.EnvelopeResponse{
		Code:    model.CodeOK,
		Message: "ok",
		Data:    body,
	}
}

func testRunnerConfig(root string, periodicRunMaxPages int) config.Config {
	return config.Config{
		WorkerID:                "collector-worker-test",
		QueueStream:             "collector:runs",
		QueueGroup:              "collector-workers",
		QueueBlockMS:            10,
		RunLeaseTTLMS:           1000,
		LeaseRenewIntervalMS:    50,
		PeriodicRunMaxPages:     periodicRunMaxPages,
		NDJSONFlushEveryRecords: 1,
		NDJSONFsyncOnClose:      true,
		OutputRootDir:           root,
	}
}
