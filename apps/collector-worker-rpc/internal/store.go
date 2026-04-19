package internal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type workerStore interface {
	LeaseRun(ctx context.Context, runID, workerID string, ttl time.Duration) (bool, error)
	RenewRunLease(ctx context.Context, runID, workerID string, ttl time.Duration) (bool, error)
	LoadRunTask(ctx context.Context, runID string) (runTaskView, error)
	ListTaskSeenPosts(ctx context.Context, taskID string, postIDs []string) (map[string]bool, error)
	RecordTaskSeenPost(ctx context.Context, taskID, postID, runID string, seenAt time.Time) (bool, error)
	RecordKeywordMonthlyUsage(ctx context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error)
	UpdateRunProgress(ctx context.Context, params updateRunProgressParams) error
	MarkRunFinished(ctx context.Context, params finishRunParams) error
}

type runTaskView struct {
	Task workerTask
	Run  workerRun
}

type workerTask struct {
	TaskID           string
	TaskType         string
	Keyword          string
	CreatedBy        string
	Priority         int32
	FrequencySeconds *int32
	Since            string
	Until            string
	RequiredCount    *int64
	PerRunCount      *int64
	ResumeCursor     string
	ResumeOffset     int64
	CompletedCount   int64
	Status           string
}

type workerRun struct {
	RunID          string
	TaskID         string
	RunNo          int64
	Status         string
	StopReason     string
	ScheduledAt    time.Time
	StartedAt      *time.Time
	EndedAt        *time.Time
	OutputPath     string
	PageCount      int64
	FetchedCount   int64
	NewCount       int64
	DuplicateCount int64
	NextCursor     string
	ResumeCursor   string
	ResumeOffset   int64
	ErrorMessage   string
}

type updateRunProgressParams struct {
	RunID          string
	Status         string
	StartedAt      *time.Time
	OutputPath     string
	PageCount      int64
	FetchedCount   int64
	NewCount       int64
	DuplicateCount int64
	NextCursor     string
	ResumeCursor   string
	ResumeOffset   int64
	UpdateResume   bool
}

type finishRunParams struct {
	RunID          string
	TaskID         string
	RunStatus      string
	TaskStatus     string
	StopReason     string
	OutputPath     string
	PageCount      int64
	FetchedCount   int64
	NewCount       int64
	DuplicateCount int64
	NextCursor     string
	ResumeCursor   string
	ResumeOffset   int64
	UpdateResume   bool
	ErrorMessage   string
	EndedAt        time.Time
	CompletedCount *int64
}

type memoryWorkerStore struct {
	mu            sync.RWMutex
	views         map[string]runTaskView
	taskSeenPosts map[string]map[string]bool
	monthlyUsage  map[string]map[string]bool
	leases        map[string]workerLease
}

type workerLease struct {
	WorkerID  string
	ExpiresAt time.Time
}

func newMemoryWorkerStore() *memoryWorkerStore {
	return &memoryWorkerStore{
		views:         make(map[string]runTaskView),
		taskSeenPosts: make(map[string]map[string]bool),
		monthlyUsage:  make(map[string]map[string]bool),
		leases:        make(map[string]workerLease),
	}
}

func (s *memoryWorkerStore) LeaseRun(_ context.Context, runID, workerID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runID = strings.TrimSpace(runID)
	workerID = strings.TrimSpace(workerID)
	if runID == "" || workerID == "" {
		return false, errors.New("run_id and worker_id are required")
	}

	now := time.Now().UTC()
	if lease, ok := s.leases[runID]; ok && now.Before(lease.ExpiresAt) && lease.WorkerID != workerID {
		return false, nil
	}

	lease := workerLease{
		WorkerID:  workerID,
		ExpiresAt: now.Add(normalizeWorkerLeaseTTL(ttl)),
	}
	s.leases[runID] = lease

	view, exists := s.views[runID]
	if exists {
		view.Run.Status = "leased"
		s.views[runID] = cloneRunTaskView(view)
	}
	return true, nil
}

func (s *memoryWorkerStore) RenewRunLease(_ context.Context, runID, workerID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runID = strings.TrimSpace(runID)
	workerID = strings.TrimSpace(workerID)
	if runID == "" || workerID == "" {
		return false, errors.New("run_id and worker_id are required")
	}

	lease, ok := s.leases[runID]
	if !ok || lease.WorkerID != workerID {
		return false, nil
	}

	lease.ExpiresAt = time.Now().UTC().Add(normalizeWorkerLeaseTTL(ttl))
	s.leases[runID] = lease
	return true, nil
}

func (s *memoryWorkerStore) LoadRunTask(_ context.Context, runID string) (runTaskView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, ok := s.views[strings.TrimSpace(runID)]
	if !ok {
		return runTaskView{}, errors.New("run not found")
	}
	return cloneRunTaskView(view), nil
}

func (s *memoryWorkerStore) ListTaskSeenPosts(_ context.Context, taskID string, postIDs []string) (map[string]bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, errors.New("task_id is required")
	}

	seenPosts := s.taskSeenPosts[taskID]
	result := make(map[string]bool)
	if len(seenPosts) == 0 {
		return result, nil
	}

	for _, postID := range postIDs {
		postID = strings.TrimSpace(postID)
		if postID == "" {
			continue
		}
		if seenPosts[postID] {
			result[postID] = true
		}
	}

	return result, nil
}

func (s *memoryWorkerStore) RecordTaskSeenPost(_ context.Context, taskID, postID, runID string, seenAt time.Time) (bool, error) {
	_ = runID
	_ = seenAt

	s.mu.Lock()
	defer s.mu.Unlock()

	taskID = strings.TrimSpace(taskID)
	postID = strings.TrimSpace(postID)
	if taskID == "" || postID == "" {
		return false, errors.New("task_id and post_id are required")
	}

	if s.taskSeenPosts[taskID] == nil {
		s.taskSeenPosts[taskID] = make(map[string]bool)
	}
	if s.taskSeenPosts[taskID][postID] {
		return false, nil
	}
	s.taskSeenPosts[taskID][postID] = true
	return true, nil
}

func (s *memoryWorkerStore) RecordKeywordMonthlyUsage(_ context.Context, keyword, usageMonth, postID, taskID string, seenAt time.Time) (bool, error) {
	_ = taskID
	_ = seenAt

	keyword = strings.TrimSpace(keyword)
	usageMonth = strings.TrimSpace(usageMonth)
	postID = strings.TrimSpace(postID)
	if keyword == "" || usageMonth == "" || postID == "" {
		return false, errors.New("keyword, usage_month, and post_id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := keyword + ":" + usageMonth
	if s.monthlyUsage[key] == nil {
		s.monthlyUsage[key] = make(map[string]bool)
	}
	if s.monthlyUsage[key][postID] {
		return false, nil
	}
	s.monthlyUsage[key][postID] = true
	return true, nil
}

func (s *memoryWorkerStore) UpdateRunProgress(_ context.Context, params updateRunProgressParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	view, ok := s.views[strings.TrimSpace(params.RunID)]
	if !ok {
		return errors.New("run not found")
	}

	if status := strings.TrimSpace(params.Status); status != "" {
		view.Run.Status = status
	}
	if params.StartedAt != nil {
		startedAt := params.StartedAt.UTC()
		view.Run.StartedAt = cloneTimePtr(&startedAt)
	}
	if outputPath := strings.TrimSpace(params.OutputPath); outputPath != "" {
		view.Run.OutputPath = outputPath
	}
	view.Run.PageCount = params.PageCount
	view.Run.FetchedCount = params.FetchedCount
	view.Run.NewCount = params.NewCount
	view.Run.DuplicateCount = params.DuplicateCount
	view.Run.NextCursor = strings.TrimSpace(params.NextCursor)
	if params.UpdateResume {
		view.Run.ResumeCursor = strings.TrimSpace(params.ResumeCursor)
		view.Run.ResumeOffset = params.ResumeOffset
	}
	s.views[params.RunID] = cloneRunTaskView(view)
	return nil
}

func (s *memoryWorkerStore) MarkRunFinished(_ context.Context, params finishRunParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	view, ok := s.views[strings.TrimSpace(params.RunID)]
	if !ok {
		return errors.New("run not found")
	}

	view.Run.Status = strings.TrimSpace(params.RunStatus)
	view.Run.StopReason = strings.TrimSpace(params.StopReason)
	view.Run.OutputPath = strings.TrimSpace(params.OutputPath)
	view.Run.PageCount = params.PageCount
	view.Run.FetchedCount = params.FetchedCount
	view.Run.NewCount = params.NewCount
	view.Run.DuplicateCount = params.DuplicateCount
	view.Run.NextCursor = strings.TrimSpace(params.NextCursor)
	if params.UpdateResume {
		view.Run.ResumeCursor = strings.TrimSpace(params.ResumeCursor)
		view.Run.ResumeOffset = params.ResumeOffset
	}
	view.Run.ErrorMessage = strings.TrimSpace(params.ErrorMessage)
	endedAt := params.EndedAt.UTC()
	view.Run.EndedAt = cloneTimePtr(&endedAt)
	if taskStatus := strings.TrimSpace(params.TaskStatus); taskStatus != "" {
		view.Task.Status = taskStatus
	}
	if params.CompletedCount != nil {
		view.Task.CompletedCount = *params.CompletedCount
	}
	if params.UpdateResume {
		view.Task.ResumeCursor = strings.TrimSpace(params.ResumeCursor)
		view.Task.ResumeOffset = params.ResumeOffset
	}

	s.views[params.RunID] = cloneRunTaskView(view)
	return nil
}

func (s *memoryWorkerStore) seedRunTask(view runTaskView) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.views[view.Run.RunID] = cloneRunTaskView(view)
}

func cloneRunTaskView(src runTaskView) runTaskView {
	dst := src
	dst.Task.FrequencySeconds = cloneInt32Ptr(src.Task.FrequencySeconds)
	dst.Task.RequiredCount = cloneInt64Ptr(src.Task.RequiredCount)
	dst.Task.PerRunCount = cloneInt64Ptr(src.Task.PerRunCount)
	dst.Run.StartedAt = cloneTimePtr(src.Run.StartedAt)
	dst.Run.EndedAt = cloneTimePtr(src.Run.EndedAt)
	return dst
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

func normalizeWorkerLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 2 * time.Minute
	}
	return ttl
}
