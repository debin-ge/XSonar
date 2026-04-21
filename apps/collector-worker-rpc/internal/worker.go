package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/pkg/xlog"
)

type runQueueMessage struct {
	ID    string
	RunID string
}

type runQueue interface {
	EnsureGroup(ctx context.Context) error
	Read(ctx context.Context, stream, group, consumer string, block time.Duration, count int64) ([]runQueueMessage, error)
	ClaimStale(ctx context.Context, stream, group, consumer string, idle time.Duration, count int64) ([]runQueueMessage, error)
	Ack(ctx context.Context, stream, group string, ids ...string) error
}

type worker struct {
	logger        *xlog.Logger
	store         workerStore
	queue         runQueue
	runner        runExecutor
	workerID      string
	queueStream   string
	queueGroup    string
	queueBlock    time.Duration
	runLeaseTTL   time.Duration
	renewInterval time.Duration

	ensureGroupMu sync.Mutex
	groupReady    bool
}

type redisRunQueue struct {
	client      *redis.Client
	queueStream string
	queueGroup  string
}

func newWorker(cfg config.Config, logger *xlog.Logger, store workerStore, queue runQueue, runner runExecutor, workerID string) *worker {
	cfg = normalizeCollectorWorkerConfig(cfg)
	if logger == nil {
		logger = xlog.NewStdout("collector-worker-rpc")
	}

	return &worker{
		logger:        logger,
		store:         store,
		queue:         queue,
		runner:        runner,
		workerID:      strings.TrimSpace(workerID),
		queueStream:   cfg.QueueStream,
		queueGroup:    cfg.QueueGroup,
		queueBlock:    time.Duration(cfg.QueueBlockMS) * time.Millisecond,
		runLeaseTTL:   time.Duration(cfg.RunLeaseTTLMS) * time.Millisecond,
		renewInterval: time.Duration(cfg.LeaseRenewIntervalMS) * time.Millisecond,
	}
}

func newRedisRunQueue(storeCfg workerStoreConfig, queueStream, queueGroup string) (*redisRunQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     storeCfg.RedisAddr,
		Password: storeCfg.RedisPassword,
		DB:       storeCfg.RedisDB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping run queue redis: %w", err)
	}
	return &redisRunQueue{
		client:      client,
		queueStream: strings.TrimSpace(queueStream),
		queueGroup:  strings.TrimSpace(queueGroup),
	}, nil
}

func (q *redisRunQueue) Close() error {
	if q == nil || q.client == nil {
		return nil
	}
	return q.client.Close()
}

func (q *redisRunQueue) EnsureGroup(ctx context.Context) error {
	if q == nil || q.client == nil {
		return errors.New("run queue redis is not configured")
	}
	err := q.client.XGroupCreateMkStream(ctx, q.queueStream, q.queueGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *redisRunQueue) Read(ctx context.Context, stream, group, consumer string, block time.Duration, count int64) ([]runQueueMessage, error) {
	if q == nil || q.client == nil {
		return nil, errors.New("run queue redis is not configured")
	}

	items, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    strings.TrimSpace(group),
		Consumer: strings.TrimSpace(consumer),
		Streams:  []string{strings.TrimSpace(stream), ">"},
		Block:    block,
		Count:    count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var messages []runQueueMessage
	for _, streamItems := range items {
		messages = append(messages, decodeRunQueueMessages(streamItems.Messages)...)
	}
	return messages, nil
}

func (q *redisRunQueue) ClaimStale(ctx context.Context, stream, group, consumer string, idle time.Duration, count int64) ([]runQueueMessage, error) {
	if q == nil || q.client == nil {
		return nil, errors.New("run queue redis is not configured")
	}

	items, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   strings.TrimSpace(stream),
		Group:    strings.TrimSpace(group),
		Consumer: strings.TrimSpace(consumer),
		MinIdle:  idle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeRunQueueMessages(items), nil
}

func (q *redisRunQueue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if q == nil || q.client == nil {
		return errors.New("run queue redis is not configured")
	}
	if len(ids) == 0 {
		return nil
	}
	return q.client.XAck(ctx, strings.TrimSpace(stream), strings.TrimSpace(group), ids...).Err()
}

func (w *worker) processOnce(ctx context.Context) error {
	if w == nil || w.queue == nil || w.store == nil || w.runner == nil {
		return nil
	}
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}

	messages, err := w.readMessages(ctx)
	if isRunQueueNoGroupError(err) {
		if recoverErr := w.recoverGroup(ctx); recoverErr != nil {
			return fmt.Errorf("recover run queue consumer group after NOGROUP: %w", recoverErr)
		}
		messages, err = w.readMessages(ctx)
	}
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		if err := w.handleMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (w *worker) readMessages(ctx context.Context) ([]runQueueMessage, error) {
	messages, err := w.queue.Read(ctx, w.queueStream, w.queueGroup, w.workerID, w.queueBlock, 1)
	if err != nil || len(messages) > 0 {
		return messages, err
	}
	return w.queue.ClaimStale(ctx, w.queueStream, w.queueGroup, w.workerID, w.runLeaseTTL, 1)
}

func (w *worker) ensureGroup(ctx context.Context) error {
	w.ensureGroupMu.Lock()
	defer w.ensureGroupMu.Unlock()
	if w.groupReady {
		return nil
	}
	if err := w.queue.EnsureGroup(ctx); err != nil {
		return err
	}
	w.groupReady = true
	return nil
}

func (w *worker) recoverGroup(ctx context.Context) error {
	w.ensureGroupMu.Lock()
	defer w.ensureGroupMu.Unlock()
	w.groupReady = false
	if err := w.queue.EnsureGroup(ctx); err != nil {
		return err
	}
	w.groupReady = true
	return nil
}

func isRunQueueNoGroupError(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOGROUP")
}

func (w *worker) handleMessage(ctx context.Context, message runQueueMessage) error {
	if strings.TrimSpace(message.RunID) == "" {
		return errors.New("run_id is required")
	}

	acquired, err := w.store.LeaseRun(ctx, message.RunID, w.workerID, w.runLeaseTTL)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}

	renewCtx, cancel := context.WithCancel(ctx)
	renewErrCh := make(chan error, 1)
	var renewWG sync.WaitGroup
	renewWG.Add(1)
	go func() {
		defer renewWG.Done()
		w.renewLeaseLoop(renewCtx, message.RunID, renewErrCh)
	}()

	runErr := w.runner.Run(ctx, message.RunID)
	cancel()
	renewWG.Wait()

	select {
	case renewErr := <-renewErrCh:
		if renewErr != nil && runErr == nil {
			runErr = renewErr
		}
	default:
	}

	if runErr != nil {
		if markErr := w.markRunFailed(ctx, message.RunID, runErr); markErr != nil {
			return markErr
		}
		return w.queue.Ack(ctx, w.queueStream, w.queueGroup, message.ID)
	}
	return w.queue.Ack(ctx, w.queueStream, w.queueGroup, message.ID)
}

func (w *worker) markRunFailed(ctx context.Context, runID string, runErr error) error {
	if w == nil || w.store == nil {
		return runErr
	}

	view, err := w.store.LoadRunTask(ctx, runID)
	if err != nil {
		return err
	}

	completedCount := view.Task.CompletedCount + view.Run.NewCount
	return w.store.MarkRunFinished(ctx, finishRunParams{
		RunID:          view.Run.RunID,
		TaskID:         view.Task.TaskID,
		RunStatus:      RunStatusFailed,
		TaskStatus:     TaskStatusFailed,
		StopReason:     view.Run.StopReason,
		OutputPath:     view.Run.OutputPath,
		PageCount:      view.Run.PageCount,
		FetchedCount:   view.Run.FetchedCount,
		NewCount:       view.Run.NewCount,
		DuplicateCount: view.Run.DuplicateCount,
		NextCursor:     view.Run.NextCursor,
		ErrorMessage:   strings.TrimSpace(runErr.Error()),
		EndedAt:        time.Now().UTC(),
		CompletedCount: &completedCount,
	})
}

func (w *worker) renewLeaseLoop(ctx context.Context, runID string, errCh chan<- error) {
	ticker := time.NewTicker(w.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := w.store.RenewRunLease(ctx, runID, w.workerID, w.runLeaseTTL)
			if err != nil {
				sendRenewError(errCh, err)
				return
			}
			if !renewed {
				sendRenewError(errCh, fmt.Errorf("run lease lost: %s", runID))
				return
			}
		}
	}
}

func sendRenewError(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}

func decodeRunQueueMessages(items []redis.XMessage) []runQueueMessage {
	messages := make([]runQueueMessage, 0, len(items))
	for _, item := range items {
		runID, _ := item.Values["run_id"].(string)
		if runID == "" {
			runID = fmt.Sprint(item.Values["run_id"])
		}
		messages = append(messages, runQueueMessage{
			ID:    item.ID,
			RunID: strings.TrimSpace(runID),
		})
	}
	return messages
}
