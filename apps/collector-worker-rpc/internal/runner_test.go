package internal

import (
	"context"
	"encoding/json"
	"errors"
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

func TestRunnerResolvePolicyAlwaysUsesLatestPolicyRPCResponse(t *testing.T) {
	root := t.TempDir()
	calls := 0
	firstPayload := testedPolicyPayload()
	secondPayload := testedPolicyPayload()
	secondPayload["upstream_path"] = "/base/apitools/search-v2"
	secondPayload["provider_api_key"] = "provider-key-rotated"
	resolver := fakePolicyResolver{
		calls: &calls,
		responses: []*clients.EnvelopeResponse{
			envelopeResponse(firstPayload),
			envelopeResponse(secondPayload),
		},
	}
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), nil, resolver, nil, "worker-1")

	first, err := runner.resolvePolicy(context.Background())
	if err != nil {
		t.Fatalf("first resolvePolicy returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected first policy resolve to hit RPC once, got %d", calls)
	}
	if first.UpstreamPath != "/base/apitools/search" || first.ProviderAPIKey != "provider-key-1" {
		t.Fatalf("expected first policy payload to match initial publish, got path=%q key=%q", first.UpstreamPath, first.ProviderAPIKey)
	}

	second, err := runner.resolvePolicy(context.Background())
	if err != nil {
		t.Fatalf("second resolvePolicy returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected second policy resolve to hit RPC again, got %d calls", calls)
	}
	if second.UpstreamPath != "/base/apitools/search-v2" || second.ProviderAPIKey != "provider-key-rotated" {
		t.Fatalf("expected second policy payload to reflect latest publish, got path=%q key=%q", second.UpstreamPath, second.ProviderAPIKey)
	}
}

func TestRunnerStopsPeriodicRunWhenPerRunCountReached(t *testing.T) {
	root := t.TempDir()
	perRunCount := int64(2)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:      "task_periodic_per_run_1",
			TaskType:    TaskTypePeriodic,
			Keyword:     "openai",
			PerRunCount: &perRunCount,
			Status:      TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_per_run_1",
			TaskID:      "task_periodic_per_run_1",
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

	if err := runner.run(context.Background(), "run_periodic_per_run_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_per_run_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "per_run_count_reached" {
		t.Fatalf("expected stop_reason per_run_count_reached, got %q", view.Run.StopReason)
	}
	if view.Run.NewCount != 2 || view.Task.CompletedCount != 2 {
		t.Fatalf("unexpected counts: run=%+v task=%+v", view.Run, view.Task)
	}
	if view.Task.Status != TaskStatusPending {
		t.Fatalf("expected periodic task to remain pending, got %q", view.Task.Status)
	}
	if len(provider.queries) != 1 || strings.Contains(provider.queries[0], `"count"`) {
		t.Fatalf("expected provider query to omit count, got %#v", provider.queries)
	}
	if view.Task.ResumeCursor != "" || view.Task.ResumeOffset != 2 {
		t.Fatalf("expected task resume state to capture homepage offset, got task=%+v", view.Task)
	}
	if view.Run.ResumeCursor != "" || view.Run.ResumeOffset != 2 {
		t.Fatalf("expected run resume state to capture homepage offset, got run=%+v", view.Run)
	}
}

func TestRunnerPeriodicRunResumesRemainingPostsFromSavedOffset(t *testing.T) {
	root := t.TempDir()
	perRunCount := int64(2)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:      "task_periodic_resume_1",
			TaskType:    TaskTypePeriodic,
			Keyword:     "openai",
			PerRunCount: &perRunCount,
			Status:      TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_resume_1",
			TaskID:      "task_periodic_resume_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	firstProvider := &fakeProviderExecutor{
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

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, firstProvider, "worker-1")
	if err := runner.run(context.Background(), "run_periodic_resume_1"); err != nil {
		t.Fatalf("first run returned error: %v", err)
	}

	firstView, err := store.LoadRunTask(context.Background(), "run_periodic_resume_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}

	store.seedRunTask(runTaskView{
		Task: firstView.Task,
		Run: workerRun{
			RunID:       "run_periodic_resume_2",
			TaskID:      "task_periodic_resume_1",
			RunNo:       2,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 1, 0, 0, time.UTC),
		},
	})

	secondProvider := &fakeProviderExecutor{
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
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_4", "text": "four"},
					},
				},
			},
		},
	}

	runner = newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, secondProvider, "worker-1")
	if err := runner.run(context.Background(), "run_periodic_resume_2"); err != nil {
		t.Fatalf("second run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_resume_2")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.NewCount != 2 || view.Run.DuplicateCount != 0 {
		t.Fatalf("expected resumed run to collect two fresh posts, got %+v", view.Run)
	}
	if len(secondProvider.queries) != 2 {
		t.Fatalf("expected two provider calls, got %#v", secondProvider.queries)
	}
	if !strings.Contains(secondProvider.queries[0], `"words":"openai"`) || strings.Contains(secondProvider.queries[0], `"cursor"`) {
		t.Fatalf("expected first resumed query to restart from homepage without count, got %#v", secondProvider.queries)
	}
	if !strings.Contains(secondProvider.queries[1], `"cursor":"cursor_1"`) {
		t.Fatalf("expected second resumed query to advance to next cursor, got %#v", secondProvider.queries)
	}
	if view.Task.ResumeCursor != "" || view.Task.ResumeOffset != 0 {
		t.Fatalf("expected task resume state to clear after exhausting pages, got %+v", view.Task)
	}
}

func TestRunnerStopsPeriodicRunWhenPageContainsNoPosts(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_periodic_empty_1",
			TaskType: TaskTypePeriodic,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_empty_1",
			TaskID:      "task_periodic_empty_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"search_by_raw_query": map[string]any{
						"search_timeline": map[string]any{
							"timeline": map[string]any{
								"instructions": []map[string]any{
									{
										"type": "TimelineAddEntries",
										"entries": []map[string]any{
											{
												"entryId": "cursor-bottom-0",
												"content": map[string]any{
													"__typename": "TimelineTimelineCursor",
													"cursorType": "Bottom",
													"value":      "cursor-bottom-123",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_1", "text": "should not be requested"}},
				},
			},
		},
	}

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, provider, "worker-1")

	if err := runner.run(context.Background(), "run_periodic_empty_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_empty_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "empty_page_with_cursor" {
		t.Fatalf("expected stop_reason empty_page_with_cursor, got %q", view.Run.StopReason)
	}
	if view.Run.PageCount != 1 {
		t.Fatalf("expected page_count 1, got %d", view.Run.PageCount)
	}
	if view.Run.FetchedCount != 0 || view.Run.NewCount != 0 {
		t.Fatalf("expected empty run counts, got %+v", view.Run)
	}
	if provider.calls != 1 {
		t.Fatalf("expected provider to stop after first empty page, got %d calls", provider.calls)
	}
	if view.Task.Status != TaskStatusPaused {
		t.Fatalf("expected periodic task to pause, got %q", view.Task.Status)
	}
	if view.Task.ResumeCursor != "" || view.Task.ResumeOffset != 0 {
		t.Fatalf("expected resume state to clear after empty page with cursor, got task=%+v", view.Task)
	}
}

func TestRunnerKeepsPeriodicTaskPendingWhenEmptyPageHasNoCursor(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_periodic_empty_final_1",
			TaskType: TaskTypePeriodic,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_periodic_empty_final_1",
			TaskID:      "task_periodic_empty_final_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{},
				},
			},
		},
	}

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, provider, "worker-1")
	if err := runner.run(context.Background(), "run_periodic_empty_final_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_empty_final_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "empty_page" {
		t.Fatalf("expected stop_reason empty_page, got %q", view.Run.StopReason)
	}
	if view.Task.Status != TaskStatusPending {
		t.Fatalf("expected periodic task to remain pending, got %q", view.Task.Status)
	}
	if view.Task.ResumeCursor != "" || view.Task.ResumeOffset != 0 {
		t.Fatalf("expected empty terminal page to clear resume state, got task=%+v", view.Task)
	}
}

func TestRunnerStopsPausedTaskWithoutFetching(t *testing.T) {
	root := t.TempDir()
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_periodic_stopped_1",
			TaskType: TaskTypePeriodic,
			Keyword:  "openai",
			Status:   TaskStatusPaused,
		},
		Run: workerRun{
			RunID:       "run_periodic_stopped_1",
			TaskID:      "task_periodic_stopped_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{Body: map[string]any{"tweets": []map[string]any{{"id": "post_1", "text": "one"}}}},
		},
	}

	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, provider, "worker-1")

	if err := runner.run(context.Background(), "run_periodic_stopped_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_periodic_stopped_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.StopReason != "task_stopped" {
		t.Fatalf("expected stop_reason task_stopped, got %q", view.Run.StopReason)
	}
	if provider.calls != 0 {
		t.Fatalf("expected stopped task to skip provider calls, got %d", provider.calls)
	}
	if view.Task.Status != TaskStatusPaused {
		t.Fatalf("expected paused task status, got %q", view.Task.Status)
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

func TestRunnerDoesNotRecordSeenOrUsageWhenPageFlushFails(t *testing.T) {
	root := t.TempDir()
	store := &orderingWorkerStore{memoryWorkerStore: newMemoryWorkerStore()}
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_range_flush_fail_1",
			TaskType: TaskTypeRange,
			Keyword:  "openai",
			Status:   TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_range_flush_fail_1",
			TaskID:      "task_range_flush_fail_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})

	flushErr := errors.New("flush failed")
	var writer *stubRunnerWriter
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_1", "text": "one"}},
				},
			},
		},
	}, "worker-1")
	runner.newWriter = func(finalPath string, flushEvery int, fsyncOnClose bool) (runnerRecordWriter, error) {
		writer = &stubRunnerWriter{
			flushErr:              flushErr,
			autoFlushEvery:        1,
			failFlushAfterAppends: 1,
		}
		return writer, nil
	}

	err := runner.run(context.Background(), "run_range_flush_fail_1")
	if !errors.Is(err, flushErr) {
		t.Fatalf("expected flush error %v, got %v", flushErr, err)
	}
	if writer == nil {
		t.Fatalf("expected stub writer to be created")
	}
	if writer.flushDuringAppend != 0 {
		t.Fatalf("expected append-time auto-flush to be suppressed, got %d", writer.flushDuringAppend)
	}
	if store.usageCalls != 0 {
		t.Fatalf("expected usageCalls 0, got %d", store.usageCalls)
	}
	if store.seenCalls != 0 {
		t.Fatalf("expected seenCalls 0, got %d", store.seenCalls)
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

func TestRunnerStopsRangeRunAtRequiredCountAfterFilteringDuplicates(t *testing.T) {
	root := t.TempDir()
	requiredCount := int64(2)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:         "task_range_required_dedup_1",
			TaskType:       TaskTypeRange,
			Keyword:        "openai",
			RequiredCount:  &requiredCount,
			CompletedCount: 0,
			Status:         TaskStatusPending,
		},
		Run: workerRun{
			RunID:       "run_range_required_dedup_1",
			TaskID:      "task_range_required_dedup_1",
			RunNo:       1,
			Status:      RunStatusQueued,
			ScheduledAt: time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC),
		},
	})
	if _, err := store.RecordTaskSeenPost(context.Background(), "task_range_required_dedup_1", "post_1", "old_run", time.Now().UTC()); err != nil {
		t.Fatalf("RecordTaskSeenPost returned error: %v", err)
	}

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{
						{"id": "post_1", "text": "historical duplicate"},
						{"id": "post_2", "text": "first unique"},
						{"id": "post_2", "text": "same page duplicate"},
						{"id": "post_3", "text": "second unique"},
						{"id": "post_4", "text": "should remain unread"},
					},
				},
			},
		},
	}
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{}, provider, "worker-1")

	if err := runner.run(context.Background(), "run_range_required_dedup_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	view, err := store.LoadRunTask(context.Background(), "run_range_required_dedup_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.NewCount != 2 {
		t.Fatalf("expected new_count 2, got %d", view.Run.NewCount)
	}
	if view.Run.DuplicateCount != 2 {
		t.Fatalf("expected duplicate_count 2, got %d", view.Run.DuplicateCount)
	}

	data, err := os.ReadFile(view.Run.OutputPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %q", string(data))
	}

	var first collectorOutputRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("Unmarshal first line returned error: %v", err)
	}
	var second collectorOutputRecord
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("Unmarshal second line returned error: %v", err)
	}
	if first.PostID != "post_2" || second.PostID != "post_3" {
		t.Fatalf("expected output posts [post_2 post_3], got [%s %s]", first.PostID, second.PostID)
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

func TestRunnerPreservesPublishedFinalFileWhenRetryStartsAfterCommit(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_range_retry_1", "task_range_retry_1.ndjson")
	policyErr := errors.New("policy should not be resolved")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	if err := writer.Append(map[string]any{"task_id": "task_range_retry_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	startedAt := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:   "task_range_retry_1",
			TaskType: TaskTypeRange,
			Keyword:  "openai",
			Status:   TaskStatusRunning,
		},
		Run: workerRun{
			RunID:        "run_range_retry_1",
			TaskID:       "task_range_retry_1",
			RunNo:        1,
			Status:       RunStatusRunning,
			ScheduledAt:  startedAt,
			StartedAt:    &startedAt,
			OutputPath:   finalPath,
			PageCount:    1,
			FetchedCount: 1,
			NewCount:     1,
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_2", "text": "should not be fetched again"}},
				},
			},
		},
	}
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{err: policyErr}, provider, "worker-2")

	if err := runner.run(context.Background(), "run_range_retry_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if provider.calls != 0 {
		t.Fatalf("expected committed-output recovery to skip provider fetch, got %d calls", provider.calls)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"post_id":"post_1"`) {
		t.Fatalf("expected retry to preserve published final output, got %q", string(data))
	}

	view, err := store.LoadRunTask(context.Background(), "run_range_retry_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.Status != RunStatusSucceeded {
		t.Fatalf("expected recovered run status succeeded, got %q", view.Run.Status)
	}
	if view.Task.CompletedCount != 1 {
		t.Fatalf("expected completed_count to remain 1, got %d", view.Task.CompletedCount)
	}
}

func TestRunnerTreatsRedeliveredFinishedRunAsNoOp(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_range_finished_1", "task_range_finished_1.ndjson")
	policyErr := errors.New("policy should not be resolved")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	if err := writer.Append(map[string]any{"task_id": "task_range_finished_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	startedAt := time.Date(2026, 4, 13, 10, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(time.Minute)
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{
			TaskID:         "task_range_finished_1",
			TaskType:       TaskTypeRange,
			Keyword:        "openai",
			Status:         TaskStatusSucceeded,
			CompletedCount: 1,
		},
		Run: workerRun{
			RunID:        "run_range_finished_1",
			TaskID:       "task_range_finished_1",
			RunNo:        1,
			Status:       RunStatusSucceeded,
			ScheduledAt:  startedAt,
			StartedAt:    &startedAt,
			EndedAt:      &endedAt,
			OutputPath:   finalPath,
			PageCount:    1,
			FetchedCount: 1,
			NewCount:     1,
		},
	})

	provider := &fakeProviderExecutor{
		responses: []providerPage{
			{
				Body: map[string]any{
					"tweets": []map[string]any{{"id": "post_2", "text": "should not be fetched again"}},
				},
			},
		},
	}
	runner := newRunner(testRunnerConfig(root, 20), xlog.NewStdout("collector-worker-rpc-test"), store, fakePolicyResolver{err: policyErr}, provider, "worker-2")

	if err := runner.run(context.Background(), "run_range_finished_1"); err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if provider.calls != 0 {
		t.Fatalf("expected finished redelivery to skip provider fetch, got %d calls", provider.calls)
	}
	if _, err := os.Stat(finalPath + ".part"); !os.IsNotExist(err) {
		t.Fatalf("expected no new .part file for finished redelivery, got %v", err)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"post_id":"post_1"`) {
		t.Fatalf("expected finished redelivery to preserve published final output, got %q", string(data))
	}
}

type providerPage struct {
	StatusCode int
	Body       map[string]any
}

type fakePolicyResolver struct {
	calls     *int
	responses []*clients.EnvelopeResponse
	response  *clients.EnvelopeResponse
	err       error
}

func (f fakePolicyResolver) ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error) {
	_ = ctx
	_ = req
	callIndex := 0
	if f.calls != nil {
		callIndex = *f.calls
		*f.calls = *f.calls + 1
	}
	if f.err != nil {
		return nil, f.err
	}
	if len(f.responses) > 0 {
		if callIndex >= len(f.responses) {
			callIndex = len(f.responses) - 1
		}
		return f.responses[callIndex], nil
	}
	if f.response != nil {
		return f.response, nil
	}
	return envelopeResponse(testedPolicyPayload()), nil
}

type fakeProviderExecutor struct {
	responses []providerPage
	calls     int
	queries   []string
}

type capturingWorkerStore struct {
	*memoryWorkerStore
	recordedUsageMonths []string
}

func (s *capturingWorkerStore) RecordKeywordMonthlyUsage(ctx context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error) {
	s.recordedUsageMonths = append(s.recordedUsageMonths, usageMonth)
	return s.memoryWorkerStore.RecordKeywordMonthlyUsage(ctx, keyword, usageMonth, postID, taskID, seenAt)
}

type orderingWorkerStore struct {
	*memoryWorkerStore
	usageCalls int
	seenCalls  int
}

func (s *orderingWorkerStore) RecordKeywordMonthlyUsage(ctx context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error) {
	s.usageCalls++
	return s.memoryWorkerStore.RecordKeywordMonthlyUsage(ctx, keyword, usageMonth, postID, taskID, seenAt)
}

func (s *orderingWorkerStore) RecordTaskSeenPost(ctx context.Context, taskID, postID, runID string, seenAt time.Time) (bool, error) {
	s.seenCalls++
	return s.memoryWorkerStore.RecordTaskSeenPost(ctx, taskID, postID, runID, seenAt)
}

type stubRunnerWriter struct {
	appended              []any
	flushErr              error
	commitErr             error
	closeErr              error
	autoFlushEvery        int
	failFlushAfterAppends int
	flushDuringAppend     int
	batchSuppressed       bool
}

func (w *stubRunnerWriter) Append(record any) error {
	w.appended = append(w.appended, record)
	if w.autoFlushEvery > 0 && !w.batchSuppressed && len(w.appended)%w.autoFlushEvery == 0 {
		w.flushDuringAppend++
		return w.Flush()
	}
	return nil
}

func (w *stubRunnerWriter) Flush() error {
	if w.flushErr != nil && len(w.appended) >= w.failFlushAfterAppends {
		return w.flushErr
	}
	return nil
}

func (w *stubRunnerWriter) Commit() error {
	return w.commitErr
}

func (w *stubRunnerWriter) Close() error {
	return w.closeErr
}

func (w *stubRunnerWriter) beginBatchAppend(batchSize int) func() {
	_ = batchSize
	w.batchSuppressed = true
	return func() {
		w.batchSuppressed = false
	}
}

func (f *fakeProviderExecutor) ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error) {
	_ = ctx
	f.queries = append(f.queries, req.GetQueryJson())
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
