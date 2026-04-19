package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

const defaultDispatchBatchSize = 100

type dispatcher struct {
	logger        *xlog.Logger
	store         schedulerStore
	workerID      string
	scanInterval  time.Duration
	softLimit     int64
	hardLimit     int64
	maxLag        time.Duration
	leaderLockTTL time.Duration
	batchSize     int

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func newDispatcher(cfg config.Config, logger *xlog.Logger, store schedulerStore) *dispatcher {
	cfg = normalizeSchedulerConfig(cfg)
	if logger == nil {
		logger = xlog.NewStdout("scheduler-rpc")
	}

	return &dispatcher{
		logger:        logger,
		store:         store,
		workerID:      shared.NewID("scheduler"),
		scanInterval:  time.Duration(cfg.DispatchScanIntervalMS) * time.Millisecond,
		softLimit:     int64(cfg.QueueBacklogSoftLimit),
		hardLimit:     int64(cfg.QueueBacklogHardLimit),
		maxLag:        time.Duration(cfg.QueueBacklogMaxLagMS) * time.Millisecond,
		leaderLockTTL: time.Duration(cfg.LeaderLockTTLMS) * time.Millisecond,
		batchSize:     defaultDispatchBatchSize,
	}
}

func (d *dispatcher) start(ctx context.Context) {
	if d == nil || d.store == nil {
		return
	}

	d.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		d.cancel = cancel
		d.wg.Add(1)

		go func() {
			defer d.wg.Done()

			d.runTick(runCtx, time.Now().UTC())

			ticker := time.NewTicker(d.scanInterval)
			defer ticker.Stop()

			for {
				select {
				case <-runCtx.Done():
					return
				case now := <-ticker.C:
					d.runTick(runCtx, now.UTC())
				}
			}
		}()
	})
}

func (d *dispatcher) stop() {
	if d == nil {
		return
	}

	d.stopOnce.Do(func() {
		if d.cancel != nil {
			d.cancel()
		}
		d.wg.Wait()

		if d.store != nil {
			if err := d.store.ReleaseLeader(context.Background(), d.workerID); err != nil && d.logger != nil {
				d.logger.Error("scheduler dispatcher failed to release leader lock", map[string]any{
					"worker_id": d.workerID,
					"error":     err.Error(),
				})
			}
		}
	})
}

func (d *dispatcher) runTick(ctx context.Context, now time.Time) {
	if err := d.tick(ctx, now); err != nil && d.logger != nil {
		d.logger.Error("scheduler dispatcher tick failed", map[string]any{
			"worker_id": d.workerID,
			"error":     err.Error(),
		})
	}
}

func (d *dispatcher) tick(ctx context.Context, now time.Time) error {
	if d == nil || d.store == nil {
		return nil
	}

	leader, err := d.store.TryBecomeLeader(ctx, d.workerID, d.leaderLockTTL)
	if err != nil {
		return err
	}
	if !leader {
		return nil
	}

	backlog, err := d.store.QueueBacklog(ctx, now)
	if err != nil {
		return err
	}
	if d.softLimit > 0 && backlog.PendingCount >= d.softLimit && d.logger != nil {
		d.logger.Info("scheduler queue backlog reached soft limit", map[string]any{
			"worker_id":     d.workerID,
			"pending_count": backlog.PendingCount,
			"oldest_age_ms": backlog.OldestAge.Milliseconds(),
		})
	}
	if d.shouldPause(backlog) {
		return nil
	}

	return d.dispatchDueTasks(ctx, now)
}

func (d *dispatcher) shouldPause(backlog queueBacklog) bool {
	if d.hardLimit > 0 && backlog.PendingCount >= d.hardLimit {
		return true
	}
	if d.maxLag > 0 && backlog.OldestAge >= d.maxLag {
		return true
	}
	return false
}

func (d *dispatcher) dispatchDueTasks(ctx context.Context, now time.Time) error {
	items, svcErr := d.store.ListDueTasks(ctx, now, d.batchSize)
	if svcErr != nil {
		return fmt.Errorf("list due tasks: %s", svcErr.message)
	}

	for _, item := range items {
		if err := d.dispatchTask(ctx, now, item); err != nil {
			return err
		}
	}
	return nil
}

func (d *dispatcher) dispatchTask(ctx context.Context, now time.Time, item task) error {
	switch item.TaskType {
	case TaskTypePeriodic:
		return d.dispatchPeriodicTask(ctx, now, item)
	case TaskTypeRange:
		return d.dispatchRangeTask(ctx, now, item)
	default:
		return nil
	}
}

func (d *dispatcher) dispatchPeriodicTask(ctx context.Context, now time.Time, item task) error {
	if item.FrequencySeconds == nil || *item.FrequencySeconds <= 0 {
		return fmt.Errorf("periodic task %s is missing frequency_seconds", item.TaskID)
	}

	openRun, svcErr := d.store.HasOpenRun(ctx, item.TaskID)
	if svcErr != nil {
		return fmt.Errorf("check open run for %s: %s", item.TaskID, svcErr.message)
	}
	if openRun {
		return nil
	}

	runNo, svcErr := d.store.NextRunNo(ctx, item.TaskID)
	if svcErr != nil {
		return fmt.Errorf("allocate run number for %s: %s", item.TaskID, svcErr.message)
	}

	createdRun, svcErr := d.store.CreateRun(ctx, &taskRun{
		TaskID:      item.TaskID,
		RunNo:       runNo,
		Status:      RunStatusQueued,
		ScheduledAt: now,
	})
	if svcErr != nil {
		return fmt.Errorf("create periodic run for %s: %s", item.TaskID, svcErr.message)
	}
	if err := d.store.EnqueueRun(ctx, createdRun.RunID); err != nil {
		return err
	}

	nextRunAt := now.Add(time.Duration(*item.FrequencySeconds) * time.Second)
	if _, svcErr := d.store.UpdateTaskDispatch(ctx, item.TaskID, TaskStatusPending, &nextRunAt); svcErr != nil {
		return fmt.Errorf("advance periodic task %s: %s", item.TaskID, svcErr.message)
	}
	return nil
}

func (d *dispatcher) dispatchRangeTask(ctx context.Context, now time.Time, item task) error {
	openRun, svcErr := d.store.HasOpenRun(ctx, item.TaskID)
	if svcErr != nil {
		return fmt.Errorf("check open run for %s: %s", item.TaskID, svcErr.message)
	}
	if openRun {
		return nil
	}

	runNo, svcErr := d.store.NextRunNo(ctx, item.TaskID)
	if svcErr != nil {
		return fmt.Errorf("allocate run number for %s: %s", item.TaskID, svcErr.message)
	}

	createdRun, svcErr := d.store.CreateRun(ctx, &taskRun{
		TaskID:      item.TaskID,
		RunNo:       runNo,
		Status:      RunStatusQueued,
		ScheduledAt: now,
	})
	if svcErr != nil {
		return fmt.Errorf("create range run for %s: %s", item.TaskID, svcErr.message)
	}
	if err := d.store.EnqueueRun(ctx, createdRun.RunID); err != nil {
		return err
	}
	if _, svcErr := d.store.UpdateTaskDispatch(ctx, item.TaskID, TaskStatusRunning, nil); svcErr != nil {
		return fmt.Errorf("mark range task running %s: %s", item.TaskID, svcErr.message)
	}
	return nil
}
