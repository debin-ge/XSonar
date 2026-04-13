package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/proto/collectorworkerpb"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

const (
	defaultQueueStream             = "collector:runs"
	defaultQueueGroup              = "collector-workers"
	defaultQueueBlockMS            = 2000
	defaultRunLeaseTTLMS           = 120000
	defaultLeaseRenewIntervalMS    = 30000
	defaultPeriodicRunMaxPages     = 20
	defaultNDJSONFlushEveryRecords = 100
	defaultOutputRootDir           = "runtime/collector"
)

type serviceError struct {
	statusCode int
	code       int
	message    string
}

type CollectorWorkerService interface {
	Start(ctx context.Context)
	GetWorkerState(ctx context.Context, req GetWorkerStateRequest) (any, *serviceError)
	Close(ctx context.Context) error
}

type collectorWorkerService struct {
	cfg    config.Config
	logger *xlog.Logger
	runner *workerRunner
}

type getWorkerStateRequest struct {
	WorkerID string
}

type GetWorkerStateRequest = getWorkerStateRequest

type workerRunner struct {
	logger         *xlog.Logger
	policyClient   clients.PolicyRPC
	providerClient clients.ProviderRPC
	workerID       string
	queueStream    string
	queueGroup     string
	outputRootDir  string
	queueBlock     time.Duration

	running atomic.Bool
	healthy atomic.Bool

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func normalizeCollectorWorkerConfig(cfg config.Config) config.Config {
	if strings.TrimSpace(cfg.QueueStream) == "" {
		cfg.QueueStream = defaultQueueStream
	}
	if strings.TrimSpace(cfg.QueueGroup) == "" {
		cfg.QueueGroup = defaultQueueGroup
	}
	if cfg.QueueBlockMS <= 0 {
		cfg.QueueBlockMS = defaultQueueBlockMS
	}
	if cfg.RunLeaseTTLMS <= 0 {
		cfg.RunLeaseTTLMS = defaultRunLeaseTTLMS
	}
	if cfg.LeaseRenewIntervalMS <= 0 {
		cfg.LeaseRenewIntervalMS = defaultLeaseRenewIntervalMS
	}
	if cfg.PeriodicRunMaxPages <= 0 {
		cfg.PeriodicRunMaxPages = defaultPeriodicRunMaxPages
	}
	if cfg.NDJSONFlushEveryRecords <= 0 {
		cfg.NDJSONFlushEveryRecords = defaultNDJSONFlushEveryRecords
	}
	if strings.TrimSpace(cfg.OutputRootDir) == "" {
		cfg.OutputRootDir = defaultOutputRootDir
	}
	return cfg
}

func NewCollectorWorkerService(cfg config.Config, logger *xlog.Logger, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) CollectorWorkerService {
	return newCollectorWorkerServiceWithRunner(cfg, logger, newWorkerRunner(cfg, logger, policyClient, providerClient))
}

func newCollectorWorkerServiceWithRunner(cfg config.Config, logger *xlog.Logger, runner *workerRunner) *collectorWorkerService {
	if logger == nil {
		logger = xlog.NewStdout("collector-worker-rpc")
	}
	cfg = normalizeCollectorWorkerConfig(cfg)
	if runner == nil {
		runner = newWorkerRunner(cfg, logger, nil, nil)
	}

	return &collectorWorkerService{
		cfg:    cfg,
		logger: logger,
		runner: runner,
	}
}

func (s *collectorWorkerService) Start(ctx context.Context) {
	if s == nil || s.runner == nil {
		return
	}
	s.runner.start(ctx)
}

func (s *collectorWorkerService) Close(context.Context) error {
	if s == nil || s.runner == nil {
		return nil
	}
	s.runner.stop()
	return nil
}

func (s *collectorWorkerService) getWorkerState(_ context.Context, req getWorkerStateRequest) (any, *serviceError) {
	if s == nil || s.runner == nil {
		return nil, collectorWorkerInternalError("collector worker service unavailable")
	}
	if requested := strings.TrimSpace(req.WorkerID); requested != "" && requested != s.runner.workerID {
		return nil, collectorWorkerNotFound("worker not found")
	}
	return s.runner.snapshot(), nil
}

func (s *collectorWorkerService) GetWorkerState(ctx context.Context, req GetWorkerStateRequest) (any, *serviceError) {
	return s.getWorkerState(ctx, req)
}

func newWorkerRunner(cfg config.Config, logger *xlog.Logger, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) *workerRunner {
	cfg = normalizeCollectorWorkerConfig(cfg)
	if logger == nil {
		logger = xlog.NewStdout("collector-worker-rpc")
	}

	workerID := strings.TrimSpace(cfg.WorkerID)
	if workerID == "" {
		workerID = shared.NewID("collector-worker")
	}

	return &workerRunner{
		logger:         logger,
		policyClient:   policyClient,
		providerClient: providerClient,
		workerID:       workerID,
		queueStream:    cfg.QueueStream,
		queueGroup:     cfg.QueueGroup,
		outputRootDir:  cfg.OutputRootDir,
		queueBlock:     time.Duration(cfg.QueueBlockMS) * time.Millisecond,
	}
}

func (r *workerRunner) start(ctx context.Context) {
	if r == nil {
		return
	}

	r.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		r.running.Store(true)
		r.healthy.Store(true)
		r.wg.Add(1)

		go func() {
			defer r.wg.Done()

			ticker := time.NewTicker(r.queueBlock)
			defer ticker.Stop()

			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	})
}

func (r *workerRunner) stop() {
	if r == nil {
		return
	}

	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
		r.running.Store(false)
		r.healthy.Store(false)
	})
}

func (r *workerRunner) snapshot() map[string]any {
	if r == nil {
		return map[string]any{
			"healthy":            false,
			"queue_loop_running": false,
		}
	}

	running := r.running.Load()
	return map[string]any{
		"worker_id":          r.workerID,
		"healthy":            r.healthy.Load(),
		"queue_group":        r.queueGroup,
		"consumer_group":     r.queueGroup,
		"queue_stream":       r.queueStream,
		"output_root_dir":    r.outputRootDir,
		"queue_loop_running": running,
	}
}

func encodeCollectorWorkerResponse(data any, svcErr *serviceError) *collectorworkerpb.JsonResponse {
	if svcErr != nil {
		return &collectorworkerpb.JsonResponse{
			Code:    int32(svcErr.code),
			Message: svcErr.message,
		}
	}

	payload, _ := json.Marshal(data)
	return &collectorworkerpb.JsonResponse{
		Code:     0,
		Message:  "success",
		DataJson: string(payload),
	}
}

func EncodeCollectorWorkerResponse(data any, svcErr *serviceError) *collectorworkerpb.JsonResponse {
	return encodeCollectorWorkerResponse(data, svcErr)
}

func collectorWorkerInvalidRequest(message string) *serviceError {
	return &serviceError{
		statusCode: http.StatusBadRequest,
		code:       model.CodeInvalidRequest,
		message:    message,
	}
}

func collectorWorkerNotFound(message string) *serviceError {
	return &serviceError{
		statusCode: http.StatusNotFound,
		code:       model.CodeNotFound,
		message:    message,
	}
}

func collectorWorkerInternalError(message string) *serviceError {
	return &serviceError{
		statusCode: http.StatusInternalServerError,
		code:       model.CodeInternalError,
		message:    message,
	}
}
