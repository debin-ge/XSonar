package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
	defaultQueueBacklogSoftLimit    = 5000
	defaultQueueBacklogHardLimit    = 20000
	defaultQueueBacklogMaxLagMS     = 300000
	defaultLeaderLockTTLMS          = 15000
	defaultListTaskRunsDefaultLimit = 50
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
	store  schedulerStore
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
	return newSchedulerServiceWithStore(cfg, logger, nil)
}

func newSchedulerServiceWithStore(cfg config.Config, logger *xlog.Logger, store schedulerStore) *schedulerService {
	if logger == nil {
		logger = xlog.NewStdout("scheduler-rpc")
	}
	if store == nil {
		store = newSchedulerStore()
	}

	return &schedulerService{
		cfg:    normalizeSchedulerConfig(cfg),
		logger: logger,
		store:  store,
	}
}

func NewSchedulerService(cfg config.Config, logger *xlog.Logger) SchedulerService {
	return newSchedulerService(cfg, logger)
}

func NewSchedulerServiceWithStore(cfg config.Config, logger *xlog.Logger, store SchedulerStore) SchedulerService {
	return newSchedulerServiceWithStore(cfg, logger, store)
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

	created, ok := s.store.CreateTask(ctx, item)
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

	item, ok := s.store.GetTask(ctx, taskID)
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

	items, ok := s.store.ListTaskRuns(ctx, taskID, s.cfg.ListTaskRunsDefaultLimit)
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
