package internal

import (
	"context"
	"testing"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/pkg/xlog"
)

func TestGetWorkerStateReturnsConfiguredRuntimeFields(t *testing.T) {
	cfg := config.Config{
		WorkerID:      "collector-worker-test",
		QueueStream:   "collector:runs",
		QueueGroup:    "collector-workers",
		OutputRootDir: "runtime/collector",
	}
	runner := newWorkerRunner(cfg, xlog.NewStdout("collector-worker-rpc-test"), nil, nil)
	svc := newCollectorWorkerServiceWithRunner(cfg, xlog.NewStdout("collector-worker-rpc-test"), runner)
	svc.Start(context.Background())
	defer func() { _ = svc.Close(context.Background()) }()

	got, svcErr := svc.getWorkerState(context.Background(), getWorkerStateRequest{})
	if svcErr != nil {
		t.Fatalf("getWorkerState returned error: %v", svcErr)
	}

	state := got.(map[string]any)
	if state["worker_id"] != "collector-worker-test" {
		t.Fatalf("expected worker_id collector-worker-test, got %#v", state["worker_id"])
	}
	if state["queue_group"] != "collector-workers" {
		t.Fatalf("expected queue_group collector-workers, got %#v", state["queue_group"])
	}
	if state["consumer_group"] != "collector-workers" {
		t.Fatalf("expected consumer_group collector-workers, got %#v", state["consumer_group"])
	}
	if state["queue_stream"] != "collector:runs" {
		t.Fatalf("expected queue_stream collector:runs, got %#v", state["queue_stream"])
	}
	if state["output_root_dir"] != "runtime/collector" {
		t.Fatalf("expected output_root_dir runtime/collector, got %#v", state["output_root_dir"])
	}
	if state["healthy"] != true {
		t.Fatalf("expected healthy true, got %#v", state["healthy"])
	}
}

func TestGetWorkerStateReflectsQueueLoopStatus(t *testing.T) {
	cfg := config.Config{
		WorkerID:      "collector-worker-test",
		QueueStream:   "collector:runs",
		QueueGroup:    "collector-workers",
		OutputRootDir: "runtime/collector",
	}
	runner := newWorkerRunner(cfg, xlog.NewStdout("collector-worker-rpc-test"), nil, nil)
	svc := newCollectorWorkerServiceWithRunner(cfg, xlog.NewStdout("collector-worker-rpc-test"), runner)

	beforeStart, svcErr := svc.getWorkerState(context.Background(), getWorkerStateRequest{})
	if svcErr != nil {
		t.Fatalf("getWorkerState before start returned error: %v", svcErr)
	}
	beforeState := beforeStart.(map[string]any)
	if beforeState["healthy"] != false {
		t.Fatalf("expected stopped worker to be unhealthy, got %#v", beforeState["healthy"])
	}
	if beforeState["queue_loop_running"] != false {
		t.Fatalf("expected stopped queue loop, got %#v", beforeState["queue_loop_running"])
	}

	svc.Start(context.Background())
	defer func() { _ = svc.Close(context.Background()) }()

	afterStart, svcErr := svc.getWorkerState(context.Background(), getWorkerStateRequest{})
	if svcErr != nil {
		t.Fatalf("getWorkerState after start returned error: %v", svcErr)
	}
	afterState := afterStart.(map[string]any)
	if afterState["healthy"] != true {
		t.Fatalf("expected running worker to be healthy, got %#v", afterState["healthy"])
	}
	if afterState["queue_loop_running"] != true {
		t.Fatalf("expected running queue loop, got %#v", afterState["queue_loop_running"])
	}
}
