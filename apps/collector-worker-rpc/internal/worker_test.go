package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/pkg/xlog"
)

func TestWorkerConsumesQueuedRunAndAcquiresLease(t *testing.T) {
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{TaskID: "task_1", TaskType: TaskTypeRange, Keyword: "openai"},
		Run:  workerRun{RunID: "run_1", TaskID: "task_1", RunNo: 1, Status: RunStatusQueued, ScheduledAt: time.Now().UTC()},
	})
	queue := &fakeRunQueue{
		readMessages: []runQueueMessage{{ID: "1-0", RunID: "run_1"}},
	}
	runner := &fakeRunExecutor{}
	worker := newWorker(config.Config{
		QueueStream:          "collector:runs",
		QueueGroup:           "collector-workers",
		RunLeaseTTLMS:        1000,
		LeaseRenewIntervalMS: 50,
	}, xlog.NewStdout("collector-worker-rpc-test"), store, queue, runner, "worker-1")

	if err := worker.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce returned error: %v", err)
	}

	if len(runner.runIDs) != 1 || runner.runIDs[0] != "run_1" {
		t.Fatalf("expected runner to execute run_1, got %#v", runner.runIDs)
	}
	if len(queue.ackedIDs) != 1 || queue.ackedIDs[0] != "1-0" {
		t.Fatalf("expected message 1-0 to be acked, got %#v", queue.ackedIDs)
	}
	lease := store.leases["run_1"]
	if lease.WorkerID != "worker-1" {
		t.Fatalf("expected lease owner worker-1, got %#v", lease)
	}
}

func TestWorkerClaimsStaleMessagesWhenQueueIsIdle(t *testing.T) {
	queue := &fakeRunQueue{
		claimedMessages: []runQueueMessage{{ID: "2-0", RunID: "run_2"}},
	}
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{TaskID: "task_2", TaskType: TaskTypeRange, Keyword: "openai"},
		Run:  workerRun{RunID: "run_2", TaskID: "task_2", RunNo: 1, Status: RunStatusRunning, ScheduledAt: time.Now().UTC().Add(-time.Minute)},
	})
	runner := &fakeRunExecutor{}
	worker := newWorker(config.Config{
		QueueStream:          "collector:runs",
		QueueGroup:           "collector-workers",
		RunLeaseTTLMS:        1000,
		LeaseRenewIntervalMS: 50,
	}, xlog.NewStdout("collector-worker-rpc-test"), store, queue, runner, "worker-1")

	if err := worker.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce returned error: %v", err)
	}

	if len(runner.runIDs) != 1 || runner.runIDs[0] != "run_2" {
		t.Fatalf("expected claimed run_2 to execute, got %#v", runner.runIDs)
	}
	if len(queue.ackedIDs) != 1 || queue.ackedIDs[0] != "2-0" {
		t.Fatalf("expected claimed message 2-0 to be acked, got %#v", queue.ackedIDs)
	}
}

func TestWorkerMarksRunFailedAndAcksWhenRunnerFails(t *testing.T) {
	queue := &fakeRunQueue{
		readMessages: []runQueueMessage{{ID: "1-0", RunID: "run_1"}},
	}
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{TaskID: "task_1", TaskType: TaskTypeRange, Keyword: "openai"},
		Run:  workerRun{RunID: "run_1", TaskID: "task_1", RunNo: 1, Status: RunStatusQueued, ScheduledAt: time.Now().UTC()},
	})
	runner := &fakeRunExecutor{err: errors.New("boom")}
	worker := newWorker(config.Config{
		QueueStream:          "collector:runs",
		QueueGroup:           "collector-workers",
		RunLeaseTTLMS:        1000,
		LeaseRenewIntervalMS: 50,
	}, xlog.NewStdout("collector-worker-rpc-test"), store, queue, runner, "worker-1")

	if err := worker.processOnce(context.Background()); err != nil {
		t.Fatalf("expected processOnce to recover runner failure, got %v", err)
	}
	if len(queue.ackedIDs) != 1 || queue.ackedIDs[0] != "1-0" {
		t.Fatalf("expected failed run message to be acked, got %#v", queue.ackedIDs)
	}

	view, err := store.LoadRunTask(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("LoadRunTask returned error: %v", err)
	}
	if view.Run.Status != RunStatusFailed {
		t.Fatalf("expected run status failed, got %q", view.Run.Status)
	}
	if view.Task.Status != TaskStatusFailed {
		t.Fatalf("expected task status failed, got %q", view.Task.Status)
	}
	if view.Run.ErrorMessage != "boom" {
		t.Fatalf("expected run error message boom, got %q", view.Run.ErrorMessage)
	}
}

func TestWorkerRetriesEnsureGroupAfterFailure(t *testing.T) {
	queue := &fakeRunQueue{
		ensureErrs: []error{errors.New("temporary redis error")},
	}
	worker := newWorker(config.Config{
		QueueStream:          "collector:runs",
		QueueGroup:           "collector-workers",
		RunLeaseTTLMS:        1000,
		LeaseRenewIntervalMS: 50,
	}, xlog.NewStdout("collector-worker-rpc-test"), newMemoryWorkerStore(), queue, &fakeRunExecutor{}, "worker-1")

	if err := worker.processOnce(context.Background()); err == nil {
		t.Fatal("expected first processOnce to return ensure group error")
	}
	if queue.ensureCalls != 1 {
		t.Fatalf("expected one ensure call after first failure, got %d", queue.ensureCalls)
	}

	if err := worker.processOnce(context.Background()); err != nil {
		t.Fatalf("expected second processOnce to retry ensure group successfully, got %v", err)
	}
	if queue.ensureCalls != 2 {
		t.Fatalf("expected ensure group to be retried, got %d calls", queue.ensureCalls)
	}
}

func TestWorkerRecoversNoGroupAndRetriesRead(t *testing.T) {
	store := newMemoryWorkerStore()
	store.seedRunTask(runTaskView{
		Task: workerTask{TaskID: "task_1", TaskType: TaskTypeRange, Keyword: "openai"},
		Run:  workerRun{RunID: "run_1", TaskID: "task_1", RunNo: 1, Status: RunStatusQueued, ScheduledAt: time.Now().UTC()},
	})
	queue := &fakeRunQueue{
		readErrs:     []error{errors.New("NOGROUP No such key 'collector:runs' or consumer group 'collector-workers' in XREADGROUP with GROUP option")},
		readMessages: []runQueueMessage{{ID: "1-0", RunID: "run_1"}},
	}
	runner := &fakeRunExecutor{}
	worker := newWorker(config.Config{
		QueueStream:          "collector:runs",
		QueueGroup:           "collector-workers",
		RunLeaseTTLMS:        1000,
		LeaseRenewIntervalMS: 50,
	}, xlog.NewStdout("collector-worker-rpc-test"), store, queue, runner, "worker-1")

	if err := worker.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce returned error: %v", err)
	}
	if queue.ensureCalls != 2 {
		t.Fatalf("expected initial ensure plus NOGROUP recovery ensure, got %d", queue.ensureCalls)
	}
	if len(runner.runIDs) != 1 || runner.runIDs[0] != "run_1" {
		t.Fatalf("expected runner to execute run_1 after recovery, got %#v", runner.runIDs)
	}
	if len(queue.ackedIDs) != 1 || queue.ackedIDs[0] != "1-0" {
		t.Fatalf("expected recovered message to be acked, got %#v", queue.ackedIDs)
	}
}

type fakeRunQueue struct {
	readMessages    []runQueueMessage
	claimedMessages []runQueueMessage
	ackedIDs        []string
	ensureErrs      []error
	readErrs        []error
	claimErrs       []error
	ensureCalls     int
	readCalls       int
	claimCalls      int
}

func (q *fakeRunQueue) EnsureGroup(context.Context) error {
	q.ensureCalls++
	if len(q.ensureErrs) == 0 {
		return nil
	}
	err := q.ensureErrs[0]
	q.ensureErrs = q.ensureErrs[1:]
	return err
}

func (q *fakeRunQueue) Read(context.Context, string, string, string, time.Duration, int64) ([]runQueueMessage, error) {
	q.readCalls++
	if len(q.readErrs) > 0 {
		err := q.readErrs[0]
		q.readErrs = q.readErrs[1:]
		return nil, err
	}
	if len(q.readMessages) == 0 {
		return nil, nil
	}
	items := append([]runQueueMessage(nil), q.readMessages...)
	q.readMessages = nil
	return items, nil
}

func (q *fakeRunQueue) ClaimStale(context.Context, string, string, string, time.Duration, int64) ([]runQueueMessage, error) {
	q.claimCalls++
	if len(q.claimErrs) > 0 {
		err := q.claimErrs[0]
		q.claimErrs = q.claimErrs[1:]
		return nil, err
	}
	if len(q.claimedMessages) == 0 {
		return nil, nil
	}
	items := append([]runQueueMessage(nil), q.claimedMessages...)
	q.claimedMessages = nil
	return items, nil
}

func (q *fakeRunQueue) Ack(_ context.Context, _ string, _ string, ids ...string) error {
	q.ackedIDs = append(q.ackedIDs, ids...)
	return nil
}

type fakeRunExecutor struct {
	runIDs []string
	err    error
}

func (r *fakeRunExecutor) Run(_ context.Context, runID string) error {
	r.runIDs = append(r.runIDs, runID)
	return r.err
}
