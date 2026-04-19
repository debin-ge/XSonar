package collector

import "testing"

func TestRunLeaseKey(t *testing.T) {
	t.Parallel()

	const runID = "run-123"

	if got, want := RunLeaseKey(runID), "collector:run:lease:run-123"; got != want {
		t.Fatalf("RunLeaseKey(%q) = %q, want %q", runID, got, want)
	}
}

func TestWorkerHeartbeatKey(t *testing.T) {
	t.Parallel()

	const workerID = "worker-456"

	if got, want := WorkerHeartbeatKey(workerID), "collector:worker:heartbeat:worker-456"; got != want {
		t.Fatalf("WorkerHeartbeatKey(%q) = %q, want %q", workerID, got, want)
	}
}
