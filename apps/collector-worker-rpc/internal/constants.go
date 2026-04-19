package internal

import "xsonar/pkg/collector"

const (
	TaskTypePeriodic = collector.TaskTypePeriodic
	TaskTypeRange    = collector.TaskTypeRange

	TaskStatusPending   = collector.TaskStatusPending
	TaskStatusRunning   = collector.TaskStatusRunning
	TaskStatusSucceeded = collector.TaskStatusSucceeded
	TaskStatusPartial   = collector.TaskStatusPartial
	TaskStatusFailed    = collector.TaskStatusFailed
	TaskStatusPaused    = collector.TaskStatusPaused

	RunStatusQueued    = collector.RunStatusQueued
	RunStatusLeased    = collector.RunStatusLeased
	RunStatusRunning   = collector.RunStatusRunning
	RunStatusSucceeded = collector.RunStatusSucceeded
	RunStatusPartial   = collector.RunStatusPartial
	RunStatusFailed    = collector.RunStatusFailed
	RunStatusAbandoned = collector.RunStatusAbandoned
)
