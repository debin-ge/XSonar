package internal

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"xsonar/pkg/shared"
)

type SchedulerStore interface {
	Close(ctx context.Context) error
	CreateTask(ctx context.Context, item *task) (*task, *serviceError)
	GetTask(ctx context.Context, taskID string) (*task, *serviceError)
	ListTaskRuns(ctx context.Context, taskID string, limit int) ([]taskRun, *serviceError)
	CreateRun(ctx context.Context, item *taskRun) (*taskRun, *serviceError)
	MarkTaskRunning(ctx context.Context, taskID, runID string, startedAt time.Time) (*task, *serviceError)
	TryBecomeLeader(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ReleaseLeader(ctx context.Context, owner string) error
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
	CompletedCount   int64      `json:"completed_count"`
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
	mu            sync.RWMutex
	tasks         map[string]*task
	taskRuns      map[string][]taskRun
	leaderOwner   string
	leaderExpires time.Time
}

type FakeSchedulerStore struct {
	mu            sync.RWMutex
	tasks         map[string]*task
	taskRuns      map[string][]taskRun
	leaderOwner   string
	leaderExpires time.Time

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

func (s *memorySchedulerStore) Close(context.Context) error {
	return nil
}

func (s *memorySchedulerStore) CreateTask(_ context.Context, item *task) (*task, *serviceError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[item.TaskID]; exists {
		return nil, schedulerConflict("task already exists")
	}

	clone := cloneTask(item)
	s.tasks[item.TaskID] = clone
	return cloneTask(clone), nil
}

func (s *memorySchedulerStore) GetTask(_ context.Context, taskID string) (*task, *serviceError) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.tasks[taskID]
	if !exists {
		return nil, schedulerNotFound("task not found")
	}
	return cloneTask(item), nil
}

func (s *memorySchedulerStore) ListTaskRuns(_ context.Context, taskID string, limit int) ([]taskRun, *serviceError) {
	s.mu.RLock()
	runs := s.taskRuns[taskID]
	taskExists := s.tasks[taskID] != nil
	s.mu.RUnlock()

	if !taskExists {
		return nil, schedulerNotFound("task not found")
	}

	items := cloneAndSortTaskRuns(runs)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *memorySchedulerStore) CreateRun(_ context.Context, item *taskRun) (*taskRun, *serviceError) {
	if item == nil {
		return nil, schedulerInvalidRequest("run is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	taskID := strings.TrimSpace(item.TaskID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}
	if s.tasks[taskID] == nil {
		return nil, schedulerNotFound("task not found")
	}

	clone := cloneTaskRun(item)
	if strings.TrimSpace(clone.RunID) == "" {
		clone.RunID = shared.NewID("run")
	}
	if clone.RunNo == 0 {
		clone.RunNo = int64(len(s.taskRuns[taskID]) + 1)
	}
	if clone.Status == "" {
		clone.Status = RunStatusQueued
	}
	if clone.ScheduledAt.IsZero() {
		clone.ScheduledAt = time.Now().UTC()
	} else {
		clone.ScheduledAt = clone.ScheduledAt.UTC()
	}

	for _, run := range s.taskRuns[taskID] {
		if run.RunID == clone.RunID || run.RunNo == clone.RunNo {
			return nil, schedulerConflict("run already exists")
		}
	}

	s.taskRuns[taskID] = append(s.taskRuns[taskID], *cloneTaskRun(clone))
	return cloneTaskRun(clone), nil
}

func (s *memorySchedulerStore) MarkTaskRunning(_ context.Context, taskID, runID string, startedAt time.Time) (*task, *serviceError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.tasks[strings.TrimSpace(taskID)]
	if !exists {
		return nil, schedulerNotFound("task not found")
	}

	startedAt = startedAt.UTC()
	item.Status = TaskStatusRunning
	item.LastRunAt = cloneTimePtr(&startedAt)
	item.UpdatedAt = time.Now().UTC()

	if runID != "" {
		found := false
		for idx := range s.taskRuns[item.TaskID] {
			if s.taskRuns[item.TaskID][idx].RunID == strings.TrimSpace(runID) {
				s.taskRuns[item.TaskID][idx].Status = RunStatusRunning
				s.taskRuns[item.TaskID][idx].StartedAt = cloneTimePtr(&startedAt)
				found = true
				break
			}
		}
		if !found {
			return nil, schedulerNotFound("run not found")
		}
	}

	return cloneTask(item), nil
}

func (s *memorySchedulerStore) TryBecomeLeader(_ context.Context, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if s.leaderOwner != "" && now.Before(s.leaderExpires) && s.leaderOwner != owner {
		return false, nil
	}

	s.leaderOwner = owner
	s.leaderExpires = now.Add(normalizeLeaderTTL(ttl))
	return true, nil
}

func (s *memorySchedulerStore) ReleaseLeader(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaderOwner == owner {
		s.leaderOwner = ""
		s.leaderExpires = time.Time{}
	}
	return nil
}

func (s *FakeSchedulerStore) Close(context.Context) error {
	return nil
}

func (s *FakeSchedulerStore) CreateTask(_ context.Context, item *task) (*task, *serviceError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[item.TaskID]; exists {
		return nil, schedulerConflict("task already exists")
	}

	clone := cloneTask(item)
	s.tasks[item.TaskID] = clone
	s.lastCreatedTask = cloneTask(clone)
	return cloneTask(clone), nil
}

func (s *FakeSchedulerStore) GetTask(_ context.Context, taskID string) (*task, *serviceError) {
	s.mu.RLock()
	item, exists := s.tasks[taskID]
	s.mu.RUnlock()

	s.mu.Lock()
	s.lastGetTaskID = taskID
	s.mu.Unlock()

	if !exists {
		return nil, schedulerNotFound("task not found")
	}
	return cloneTask(item), nil
}

func (s *FakeSchedulerStore) ListTaskRuns(_ context.Context, taskID string, limit int) ([]taskRun, *serviceError) {
	s.mu.RLock()
	runs := s.taskRuns[taskID]
	taskExists := s.tasks[taskID] != nil
	s.mu.RUnlock()

	s.mu.Lock()
	s.lastListTaskRunsTaskID = taskID
	s.lastListTaskRunsLimit = limit
	s.mu.Unlock()

	if !taskExists {
		return nil, schedulerNotFound("task not found")
	}

	items := cloneAndSortTaskRuns(runs)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *FakeSchedulerStore) CreateRun(ctx context.Context, item *taskRun) (*taskRun, *serviceError) {
	_ = ctx
	if item == nil {
		return nil, schedulerInvalidRequest("run is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	taskID := strings.TrimSpace(item.TaskID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}
	if s.tasks[taskID] == nil {
		return nil, schedulerNotFound("task not found")
	}

	clone := cloneTaskRun(item)
	if strings.TrimSpace(clone.RunID) == "" {
		clone.RunID = shared.NewID("run")
	}
	if clone.RunNo == 0 {
		clone.RunNo = int64(len(s.taskRuns[taskID]) + 1)
	}
	if clone.Status == "" {
		clone.Status = RunStatusQueued
	}
	if clone.ScheduledAt.IsZero() {
		clone.ScheduledAt = time.Now().UTC()
	} else {
		clone.ScheduledAt = clone.ScheduledAt.UTC()
	}

	for _, run := range s.taskRuns[taskID] {
		if run.RunID == clone.RunID || run.RunNo == clone.RunNo {
			return nil, schedulerConflict("run already exists")
		}
	}

	s.taskRuns[taskID] = append(s.taskRuns[taskID], *cloneTaskRun(clone))
	return cloneTaskRun(clone), nil
}

func (s *FakeSchedulerStore) MarkTaskRunning(_ context.Context, taskID, runID string, startedAt time.Time) (*task, *serviceError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.tasks[strings.TrimSpace(taskID)]
	if !exists {
		return nil, schedulerNotFound("task not found")
	}

	startedAt = startedAt.UTC()
	item.Status = TaskStatusRunning
	item.LastRunAt = cloneTimePtr(&startedAt)
	item.UpdatedAt = time.Now().UTC()

	if runID != "" {
		found := false
		for idx := range s.taskRuns[item.TaskID] {
			if s.taskRuns[item.TaskID][idx].RunID == strings.TrimSpace(runID) {
				s.taskRuns[item.TaskID][idx].Status = RunStatusRunning
				s.taskRuns[item.TaskID][idx].StartedAt = cloneTimePtr(&startedAt)
				found = true
				break
			}
		}
		if !found {
			return nil, schedulerNotFound("run not found")
		}
	}

	return cloneTask(item), nil
}

func (s *FakeSchedulerStore) TryBecomeLeader(_ context.Context, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if s.leaderOwner != "" && now.Before(s.leaderExpires) && s.leaderOwner != owner {
		return false, nil
	}

	s.leaderOwner = owner
	s.leaderExpires = now.Add(normalizeLeaderTTL(ttl))
	return true, nil
}

func (s *FakeSchedulerStore) ReleaseLeader(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaderOwner == owner {
		s.leaderOwner = ""
		s.leaderExpires = time.Time{}
	}
	return nil
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

func normalizeLeaderTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 15 * time.Second
	}
	return ttl
}
