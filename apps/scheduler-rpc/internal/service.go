package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/pkg/model"
	"xsonar/pkg/proto/schedulerpb"
	"xsonar/pkg/xlog"
)

const (
	schedulerTaskTypePeriodic  = "periodic"
	schedulerTaskTypeRange     = "range"
	schedulerTaskStatusPending = "pending"
)

const (
	defaultDispatchScanIntervalMS   = 1000
	defaultQueueBacklogSoftLimit    = 100
	defaultQueueBacklogHardLimit    = 1000
	defaultQueueBacklogMaxLagMS     = 60000
	defaultLeaderLockTTLMS          = 30000
	defaultListTaskRunsDefaultLimit = 20
)

type serviceError struct {
	statusCode int
	code       int
	message    string
}

type SchedulerService interface {
	CreateTask(ctx context.Context, req CreateTaskRequest) (any, *serviceError)
	GetTask(ctx context.Context, req GetTaskRequest) (any, *serviceError)
	ListTaskRuns(ctx context.Context, req ListTaskRunsRequest) (any, *serviceError)
}

type schedulerService struct {
	cfg    config.Config
	logger *xlog.Logger
	store  *schedulerStore
}

type schedulerStore struct {
	mu       sync.RWMutex
	tasks    map[string]*task
	taskRuns map[string][]taskRun
}

type createTaskRequest struct {
	TaskID           string
	TaskType         string
	Keyword          string
	Priority         int32
	FrequencySeconds *int32
	Since            string
	Until            string
	RequiredCount    *int64
	CreatedBy        string
}

type CreateTaskRequest = createTaskRequest

type getTaskRequest struct {
	TaskID string
}

type GetTaskRequest = getTaskRequest

type listTaskRunsRequest struct {
	TaskID string
}

type ListTaskRunsRequest = listTaskRunsRequest

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

func defaultConfig() config.Config {
	return config.Config{
		DispatchScanIntervalMS:   defaultDispatchScanIntervalMS,
		QueueBacklogSoftLimit:    defaultQueueBacklogSoftLimit,
		QueueBacklogHardLimit:    defaultQueueBacklogHardLimit,
		QueueBacklogMaxLagMS:     defaultQueueBacklogMaxLagMS,
		LeaderLockTTLMS:          defaultLeaderLockTTLMS,
		ListTaskRunsDefaultLimit: defaultListTaskRunsDefaultLimit,
	}
}

func normalizeSchedulerConfig(cfg config.Config) config.Config {
	defaults := defaultConfig()
	if cfg.DispatchScanIntervalMS <= 0 {
		cfg.DispatchScanIntervalMS = defaults.DispatchScanIntervalMS
	}
	if cfg.QueueBacklogSoftLimit <= 0 {
		cfg.QueueBacklogSoftLimit = defaults.QueueBacklogSoftLimit
	}
	if cfg.QueueBacklogHardLimit <= 0 {
		cfg.QueueBacklogHardLimit = defaults.QueueBacklogHardLimit
	}
	if cfg.QueueBacklogMaxLagMS <= 0 {
		cfg.QueueBacklogMaxLagMS = defaults.QueueBacklogMaxLagMS
	}
	if cfg.LeaderLockTTLMS <= 0 {
		cfg.LeaderLockTTLMS = defaults.LeaderLockTTLMS
	}
	if cfg.ListTaskRunsDefaultLimit <= 0 {
		cfg.ListTaskRunsDefaultLimit = defaults.ListTaskRunsDefaultLimit
	}
	return cfg
}

func newSchedulerService(cfg config.Config, logger *xlog.Logger) *schedulerService {
	if logger == nil {
		logger = xlog.NewStdout("scheduler-rpc")
	}

	return &schedulerService{
		cfg:    normalizeSchedulerConfig(cfg),
		logger: logger,
		store:  newSchedulerStore(),
	}
}

func NewSchedulerService(cfg config.Config, logger *xlog.Logger) SchedulerService {
	return newSchedulerService(cfg, logger)
}

func newSchedulerStore() *schedulerStore {
	return &schedulerStore{
		tasks:    make(map[string]*task),
		taskRuns: make(map[string][]taskRun),
	}
}

func (s *schedulerStore) createTask(item *task) (*task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[item.TaskID]; exists {
		return nil, false
	}

	clone := cloneTask(item)
	s.tasks[item.TaskID] = clone
	return cloneTask(clone), true
}

func (s *schedulerStore) getTask(taskID string) (*task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.tasks[taskID]
	if !exists {
		return nil, false
	}
	return cloneTask(item), true
}

func (s *schedulerStore) addTaskRun(run taskRun) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runs := append(s.taskRuns[run.TaskID], *cloneTaskRun(&run))
	s.taskRuns[run.TaskID] = runs
}

func (s *schedulerStore) listTaskRuns(taskID string, limit int) ([]taskRun, bool) {
	s.mu.RLock()
	runs := s.taskRuns[taskID]
	taskExists := s.tasks[taskID] != nil
	s.mu.RUnlock()

	if !taskExists {
		return nil, false
	}

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

	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}

	return items, true
}

func (s *schedulerService) createTask(ctx context.Context, req createTaskRequest) (any, *serviceError) {
	_ = ctx

	if strings.TrimSpace(req.TaskID) == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}
	if strings.TrimSpace(req.Keyword) == "" {
		return nil, schedulerInvalidRequest("keyword is required")
	}
	if req.Priority < 0 || req.Priority > 100 {
		return nil, schedulerInvalidRequest("priority must be between 0 and 100")
	}

	taskType := strings.ToLower(strings.TrimSpace(req.TaskType))
	switch taskType {
	case schedulerTaskTypePeriodic:
		if req.FrequencySeconds == nil || *req.FrequencySeconds <= 0 {
			return nil, schedulerInvalidRequest("frequency_seconds is required for periodic tasks")
		}
	case schedulerTaskTypeRange:
		if strings.TrimSpace(req.Since) == "" || strings.TrimSpace(req.Until) == "" {
			return nil, schedulerInvalidRequest("since and until are required for range tasks")
		}
	default:
		return nil, schedulerInvalidRequest("task_type must be periodic or range")
	}

	now := time.Now().UTC()
	item := &task{
		TaskID:           strings.TrimSpace(req.TaskID),
		TaskType:         taskType,
		Keyword:          strings.TrimSpace(req.Keyword),
		Priority:         req.Priority,
		FrequencySeconds: cloneInt32Ptr(req.FrequencySeconds),
		Since:            strings.TrimSpace(req.Since),
		Until:            strings.TrimSpace(req.Until),
		RequiredCount:    cloneInt64Ptr(req.RequiredCount),
		CreatedBy:        strings.TrimSpace(req.CreatedBy),
		Status:           schedulerTaskStatusPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	created, ok := s.store.createTask(item)
	if !ok {
		return nil, schedulerConflict("task already exists")
	}

	return created, nil
}

func (s *schedulerService) CreateTask(ctx context.Context, req CreateTaskRequest) (any, *serviceError) {
	return s.createTask(ctx, req)
}

func (s *schedulerService) getTask(ctx context.Context, req getTaskRequest) (any, *serviceError) {
	_ = ctx

	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}

	item, ok := s.store.getTask(taskID)
	if !ok {
		return nil, schedulerNotFound("task not found")
	}

	return item, nil
}

func (s *schedulerService) GetTask(ctx context.Context, req GetTaskRequest) (any, *serviceError) {
	return s.getTask(ctx, req)
}

func (s *schedulerService) listTaskRuns(ctx context.Context, req listTaskRunsRequest) (any, *serviceError) {
	_ = ctx

	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		return nil, schedulerInvalidRequest("task_id is required")
	}

	items, ok := s.store.listTaskRuns(taskID, s.cfg.ListTaskRunsDefaultLimit)
	if !ok {
		return nil, schedulerNotFound("task not found")
	}

	return map[string]any{"items": items}, nil
}

func (s *schedulerService) ListTaskRuns(ctx context.Context, req ListTaskRunsRequest) (any, *serviceError) {
	return s.listTaskRuns(ctx, req)
}

func encodeSchedulerResponse(data any, svcErr *serviceError) *schedulerpb.JsonResponse {
	if svcErr != nil {
		return &schedulerpb.JsonResponse{
			Code:    int32(svcErr.code),
			Message: svcErr.message,
		}
	}

	return &schedulerpb.JsonResponse{
		Code:     int32(model.CodeOK),
		Message:  "ok",
		DataJson: marshalSchedulerData(data),
	}
}

func EncodeSchedulerResponse(data any, svcErr *serviceError) *schedulerpb.JsonResponse {
	return encodeSchedulerResponse(data, svcErr)
}

func marshalSchedulerData(data any) string {
	if data == nil {
		return "null"
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return "null"
	}
	return string(payload)
}

func schedulerInvalidRequest(message string) *serviceError {
	return &serviceError{statusCode: http.StatusBadRequest, code: model.CodeInvalidRequest, message: message}
}

func schedulerConflict(message string) *serviceError {
	return &serviceError{statusCode: http.StatusConflict, code: model.CodeConflict, message: message}
}

func schedulerNotFound(message string) *serviceError {
	return &serviceError{statusCode: http.StatusNotFound, code: model.CodeNotFound, message: message}
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
