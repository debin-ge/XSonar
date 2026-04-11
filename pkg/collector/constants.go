package collector

import "fmt"

const keyPrefix = "collector"

// TaskTypePeriodic identifies a recurring collector task.
const TaskTypePeriodic = "periodic"

// TaskTypeRange identifies a bounded collector task.
const TaskTypeRange = "range"

// TaskStatusPending marks a task that has not started yet.
const TaskStatusPending = "pending"

// TaskStatusRunning marks a task that is actively executing.
const TaskStatusRunning = "running"

// TaskStatusSucceeded marks a task that completed successfully.
const TaskStatusSucceeded = "succeeded"

// TaskStatusPartial marks a task that completed with partial results.
const TaskStatusPartial = "partial"

// TaskStatusFailed marks a task that completed with a failure.
const TaskStatusFailed = "failed"

// TaskStatusPaused marks a task that is paused and should not be scheduled.
const TaskStatusPaused = "paused"

// RunStatusQueued marks a run that has been enqueued but not leased.
const RunStatusQueued = "queued"

// RunStatusLeased marks a run that is leased to a worker.
const RunStatusLeased = "leased"

// RunStatusRunning marks a run that is actively executing.
const RunStatusRunning = "running"

// RunStatusSucceeded marks a run that completed successfully.
const RunStatusSucceeded = "succeeded"

// RunStatusPartial marks a run that completed with partial results.
const RunStatusPartial = "partial"

// RunStatusFailed marks a run that completed with a failure.
const RunStatusFailed = "failed"

// RunStatusAbandoned marks a run whose lease expired or worker was lost.
const RunStatusAbandoned = "abandoned"

// SchedulerLeaderLockKey returns the Redis key used for scheduler leader election.
func SchedulerLeaderLockKey() string {
	return keyPrefix + ":scheduler:leader"
}

// RunsStreamKey returns the Redis stream key used to fan out pending runs.
func RunsStreamKey() string {
	return keyPrefix + ":runs"
}

// RunLeaseKey returns the Redis key used to store a run lease.
func RunLeaseKey(runID string) string {
	return fmt.Sprintf("%s:run:lease:%s", keyPrefix, runID)
}

// WorkerHeartbeatKey returns the Redis key used to store a worker heartbeat.
func WorkerHeartbeatKey(workerID string) string {
	return fmt.Sprintf("%s:worker:heartbeat:%s", keyPrefix, workerID)
}
