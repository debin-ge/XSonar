package internal

import (
	"context"
	"sort"
	"sync"
	"time"
)

type SchedulerStore interface {
	CreateTask(ctx context.Context, item *task) (*task, bool)
	GetTask(ctx context.Context, taskID string) (*task, bool)
	ListTaskRuns(ctx context.Context, taskID string, limit int) ([]taskRun, bool)
}

type schedulerStore = SchedulerStore

type task struct {
	TaskID           string     `json:"task_id"`
	TaskType         string     `json:"task_type"`
	Keyword          string     `json:"keyword"`
	Priority         int32      `json:"priority"`
	FrequencySeconds *int32     `json:"frequency_seconds,omitempty"`
	Since            string     `json:"since,omitempty"`
	Until            string     `json:"until,omitempty"`
	RequiredCount    *int64     `json:"required_count,omitempty"`
	CreatedBy        string     `json:"created_by"`
	Status           string     `json:"status"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	NextRunAt        *time.Time `json:"next_run_at,omitempty"`
	LastRunAt        *time.Time `json:"last_run_at,omitempty"`
}

type taskRun struct {
	RunID          string     `json:"run_id"`
	TaskID         string     `json:"task_id"`
	RunNo          int64      `json:"run_no"`
	Status         string     `json:"status"`
	StopReason     string     `json:"stop_reason,omitempty"`
	ScheduledAt    time.Time  `json:"scheduled_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
	OutputPath     string     `json:"output_path,omitempty"`
	PageCount      int64      `json:"page_count"`
	FetchedCount   int64      `json:"fetched_count"`
	NewCount       int64      `json:"new_count"`
	DuplicateCount int64      `json:"duplicate_count"`
	NextCursor     string     `json:"next_cursor,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}

type memorySchedulerStore struct {
	mu       sync.RWMutex
	tasks    map[string]*task
	taskRuns map[string][]taskRun
}

type FakeSchedulerStore struct {
	mu       sync.RWMutex
	tasks    map[string]*task
	taskRuns map[string][]taskRun

	lastCreatedTask        *task
	lastGetTaskID          string
	lastListTaskRunsTaskID string
	lastListTaskRunsLimit  int
}

type fakeSchedulerStore = FakeSchedulerStore

func newSchedulerStore() schedulerStore {
	return newMemorySchedulerStore()
}

func NewSchedulerStore() SchedulerStore {
	return newMemorySchedulerStore()
}

func newMemorySchedulerStore() *memorySchedulerStore {
	return &memorySchedulerStore{
		tasks:    make(map[string]*task),
		taskRuns: make(map[string][]taskRun),
	}
}

func NewFakeSchedulerStore() *FakeSchedulerStore {
	return &FakeSchedulerStore{
		tasks:    make(map[string]*task),
		taskRuns: make(map[string][]taskRun),
	}
}

func newFakeSchedulerStore() *fakeSchedulerStore {
	return NewFakeSchedulerStore()
}

func (s *memorySchedulerStore) CreateTask(_ context.Context, item *task) (*task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[item.TaskID]; exists {
		return nil, false
	}

	clone := cloneTask(item)
	s.tasks[item.TaskID] = clone
	return cloneTask(clone), true
}

func (s *memorySchedulerStore) GetTask(_ context.Context, taskID string) (*task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.tasks[taskID]
	if !exists {
		return nil, false
	}
	return cloneTask(item), true
}

func (s *memorySchedulerStore) ListTaskRuns(_ context.Context, taskID string, limit int) ([]taskRun, bool) {
	s.mu.RLock()
	runs := s.taskRuns[taskID]
	taskExists := s.tasks[taskID] != nil
	s.mu.RUnlock()

	if !taskExists {
		return nil, false
	}

	items := cloneAndSortTaskRuns(runs)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, true
}

func (s *FakeSchedulerStore) CreateTask(_ context.Context, item *task) (*task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[item.TaskID]; exists {
		return nil, false
	}

	clone := cloneTask(item)
	s.tasks[item.TaskID] = clone
	s.lastCreatedTask = cloneTask(clone)
	return cloneTask(clone), true
}

func (s *FakeSchedulerStore) GetTask(_ context.Context, taskID string) (*task, bool) {
	s.mu.RLock()
	item, exists := s.tasks[taskID]
	s.mu.RUnlock()

	s.mu.Lock()
	s.lastGetTaskID = taskID
	s.mu.Unlock()

	if !exists {
		return nil, false
	}
	return cloneTask(item), true
}

func (s *FakeSchedulerStore) ListTaskRuns(_ context.Context, taskID string, limit int) ([]taskRun, bool) {
	s.mu.RLock()
	runs := s.taskRuns[taskID]
	taskExists := s.tasks[taskID] != nil
	s.mu.RUnlock()

	s.mu.Lock()
	s.lastListTaskRunsTaskID = taskID
	s.lastListTaskRunsLimit = limit
	s.mu.Unlock()

	if !taskExists {
		return nil, false
	}

	items := cloneAndSortTaskRuns(runs)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, true
}

func (s *FakeSchedulerStore) addTaskRun(run taskRun) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.taskRuns[run.TaskID] = append(s.taskRuns[run.TaskID], *cloneTaskRun(&run))
}

func (s *FakeSchedulerStore) LastCreatedTaskCreatedBy() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.lastCreatedTask == nil {
		return ""
	}
	return s.lastCreatedTask.CreatedBy
}

func cloneAndSortTaskRuns(runs []taskRun) []taskRun {
	items := make([]taskRun, 0, len(runs))
	for _, run := range runs {
		items = append(items, *cloneTaskRun(&run))
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].ScheduledAt.Equal(items[j].ScheduledAt) {
			return items[i].RunNo > items[j].RunNo
		}
		return items[i].ScheduledAt.After(items[j].ScheduledAt)
	})

	return items
}

func cloneTask(src *task) *task {
	if src == nil {
		return nil
	}

	dst := *src
	dst.FrequencySeconds = cloneInt32Ptr(src.FrequencySeconds)
	dst.RequiredCount = cloneInt64Ptr(src.RequiredCount)
	dst.NextRunAt = cloneTimePtr(src.NextRunAt)
	dst.LastRunAt = cloneTimePtr(src.LastRunAt)
	return &dst
}

func cloneTaskRun(src *taskRun) *taskRun {
	if src == nil {
		return nil
	}

	dst := *src
	dst.StartedAt = cloneTimePtr(src.StartedAt)
	dst.EndedAt = cloneTimePtr(src.EndedAt)
	return &dst
}

func cloneInt32Ptr(src *int32) *int32 {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func cloneInt64Ptr(src *int64) *int64 {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func cloneTimePtr(src *time.Time) *time.Time {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}
