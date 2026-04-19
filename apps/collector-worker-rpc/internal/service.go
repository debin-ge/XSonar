package internal

import (
	"context"
	"encoding/json"
	"errors"
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
	logger        *xlog.Logger
	workerID      string
	queueStream   string
	queueGroup    string
	outputRootDir string
	queueBlock    time.Duration
	worker        *worker
	storeCloser   func() error
	queueCloser   func() error

	running atomic.Bool
	healthy atomic.Bool

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopErr   error
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
	runner, err := newConfiguredWorkerRunner(cfg, logger, policyClient, providerClient)
	if err != nil {
		panic(err)
	}
	return newCollectorWorkerServiceWithRunner(cfg, logger, runner)
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
	return s.runner.stop()
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
		logger:        logger,
		workerID:      workerID,
		queueStream:   cfg.QueueStream,
		queueGroup:    cfg.QueueGroup,
		outputRootDir: cfg.OutputRootDir,
		queueBlock:    time.Duration(cfg.QueueBlockMS) * time.Millisecond,
	}
}

func newConfiguredWorkerRunner(cfg config.Config, logger *xlog.Logger, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) (*workerRunner, error) {
	runtime := newWorkerRunner(cfg, logger, policyClient, providerClient)
	storeCfg := loadWorkerStoreConfig()
	if err := validateWorkerStoreConfig(storeCfg); err != nil {
		return nil, err
	}
	if storeCfg.Backend != "pgredis" || policyClient == nil || providerClient == nil {
		return runtime, nil
	}

	store, err := newPGRedisWorkerStore(storeCfg, logger)
	if err != nil {
		return nil, err
	}
	queue, err := newRedisRunQueue(storeCfg, runtime.queueStream, runtime.queueGroup)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	runtime.worker = newWorker(cfg, logger, store, queue, newRunner(cfg, logger, store, policyClient, providerClient, runtime.workerID), runtime.workerID)
	runtime.storeCloser = store.Close
	runtime.queueCloser = queue.Close
	return runtime, nil
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

			if r.worker != nil {
				for {
					if runCtx.Err() != nil {
						return
					}
					if err := r.worker.processOnce(runCtx); err != nil {
						if runCtx.Err() != nil {
							return
						}
						r.logger.Error("collector worker loop failed", map[string]any{
							"worker_id": r.workerID,
							"error":     err.Error(),
						})
						select {
						case <-runCtx.Done():
							return
						case <-time.After(200 * time.Millisecond):
						}
					}
				}
			}

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

func (r *workerRunner) stop() error {
	if r == nil {
		return nil
	}

	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
		r.running.Store(false)
		r.healthy.Store(false)
		r.stopErr = joinCloseErrors(r.queueCloser, r.storeCloser)
	})
	return r.stopErr
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
		code, err := shared.Int32FromInt(svcErr.code)
		if err != nil {
			code = model.CodeInternalError
		}
		return &collectorworkerpb.JsonResponse{
			Code:    code,
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

func joinCloseErrors(closers ...func() error) error {
	var combined error
	for _, closeFn := range closers {
		if closeFn == nil {
			continue
		}
		if err := closeFn(); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}
