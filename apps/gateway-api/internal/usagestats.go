package internal

import (
	"context"
	"sync"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/pkg/xlog"
)

const (
	defaultUsageStatQueueSize = 1024
	defaultUsageStatWorkers   = 2
	defaultUsageStatTimeout   = 500 * time.Millisecond
)

type usageStatRecorder interface {
	Record(accessservice.RecordUsageStatRequest)
	Close()
}

type inlineUsageStatRecorder struct {
	accessClient gatewayAccessClient
}

type asyncUsageStatRecorder struct {
	logger       *xlog.Logger
	accessClient gatewayAccessClient
	timeout      time.Duration
	queue        chan accessservice.RecordUsageStatRequest
	closeOnce    sync.Once
	workersWG    sync.WaitGroup
}

func newInlineUsageStatRecorder(accessClient gatewayAccessClient) usageStatRecorder {
	return &inlineUsageStatRecorder{accessClient: accessClient}
}

func (r *inlineUsageStatRecorder) Record(req accessservice.RecordUsageStatRequest) {
	if r == nil || r.accessClient == nil {
		return
	}
	_, _ = r.accessClient.RecordUsageStat(context.Background(), &req)
}

func (r *inlineUsageStatRecorder) Close() {}

func newAsyncUsageStatRecorder(logger *xlog.Logger, accessClient gatewayAccessClient, queueSize, workers int, timeout time.Duration) usageStatRecorder {
	if accessClient == nil {
		return &inlineUsageStatRecorder{}
	}
	if queueSize <= 0 {
		queueSize = defaultUsageStatQueueSize
	}
	if workers <= 0 {
		workers = defaultUsageStatWorkers
	}
	if timeout <= 0 {
		timeout = defaultUsageStatTimeout
	}

	recorder := &asyncUsageStatRecorder{
		logger:       logger,
		accessClient: accessClient,
		timeout:      timeout,
		queue:        make(chan accessservice.RecordUsageStatRequest, queueSize),
	}

	recorder.workersWG.Add(workers)
	for i := 0; i < workers; i++ {
		go recorder.runWorker()
	}

	return recorder
}

func (r *asyncUsageStatRecorder) Record(req accessservice.RecordUsageStatRequest) {
	if r == nil {
		return
	}

	select {
	case r.queue <- req:
	default:
		if r.logger != nil {
			r.logger.Error("gateway usage stat dropped", map[string]any{
				"request_id": req.RequestId,
				"app_id":     req.AppId,
				"policy_key": req.PolicyKey,
				"reason":     "queue_full",
			})
		}
	}
}

func (r *asyncUsageStatRecorder) Close() {
	if r == nil {
		return
	}

	r.closeOnce.Do(func() {
		close(r.queue)
		r.workersWG.Wait()
	})
}

func (r *asyncUsageStatRecorder) runWorker() {
	defer r.workersWG.Done()

	for req := range r.queue {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		_, err := r.accessClient.RecordUsageStat(ctx, &req)
		cancel()
		if err != nil && r.logger != nil {
			r.logger.Error("gateway usage stat write failed", map[string]any{
				"request_id": req.RequestId,
				"app_id":     req.AppId,
				"policy_key": req.PolicyKey,
				"error":      err.Error(),
			})
		}
	}
}
