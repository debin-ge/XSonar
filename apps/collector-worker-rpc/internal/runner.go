package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

const (
	collectorPolicyPath   = "/v1/search/tweets"
	collectorPolicyMethod = http.MethodGet
	collectorPolicyKey    = "search_tweets_v1"
	stopReasonPageLimit   = "page_limit"
	stopReasonPerRun      = "per_run_count_reached"
	stopReasonRequired    = "required_count_reached"
	stopReasonEmptyPage   = "empty_page"
	stopReasonEmptyCursor = "empty_page_with_cursor"
	stopReasonStopped     = "task_stopped"
	stopReasonTaskFailed  = "task_failed"
	stopReasonTaskDone    = "task_completed"
)

type policyResolver interface {
	ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error)
}

type providerExecutor interface {
	ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error)
}

type runExecutor interface {
	Run(ctx context.Context, runID string) error
}

type runnerFlusher interface {
	Flush() error
}

type runnerRecordWriter interface {
	Append(record any) error
	Flush() error
	Commit() error
	Close() error
	beginBatchAppend(batchSize int) func()
}

type runner struct {
	logger              *xlog.Logger
	store               workerStore
	policyResolver      policyResolver
	providerExecutor    providerExecutor
	workerID            string
	outputRootDir       string
	periodicRunMaxPages int64
	ndjsonFlushEvery    int
	ndjsonFsyncOnClose  bool
	newWriter           func(finalPath string, flushEvery int, fsyncOnClose bool) (runnerRecordWriter, error)
}

type resolvedPolicy struct {
	PolicyKey      string         `json:"policy_key"`
	UpstreamMethod string         `json:"upstream_method"`
	UpstreamPath   string         `json:"upstream_path"`
	DefaultParams  map[string]any `json:"default_params"`
	ProviderName   string         `json:"provider_name"`
	ProviderAPIKey string         `json:"provider_api_key"`
}

type providerExecutionPayload struct {
	StatusCode         int             `json:"status_code"`
	ResultCode         string          `json:"result_code"`
	Body               json.RawMessage `json:"body"`
	UpstreamDurationMS int64           `json:"upstream_duration_ms"`
}

type collectorOutputRecord struct {
	TaskID    string          `json:"task_id"`
	RunID     string          `json:"run_id"`
	Keyword   string          `json:"keyword"`
	PostID    string          `json:"post_id"`
	WorkerID  string          `json:"worker_id,omitempty"`
	Collected string          `json:"collected_at"`
	Raw       json.RawMessage `json:"raw"`
}

type pendingCollectedPost struct {
	extractedPost
	SeenAt     time.Time
	UsageMonth string
}

func newRunner(cfg config.Config, logger *xlog.Logger, store workerStore, policyResolver policyResolver, providerExecutor providerExecutor, workerID string) *runner {
	cfg = normalizeCollectorWorkerConfig(cfg)
	if logger == nil {
		logger = xlog.NewStdout("collector-worker-rpc")
	}
	if strings.TrimSpace(workerID) == "" {
		workerID = shared.NewID("collector-worker")
	}

	return &runner{
		logger:              logger,
		store:               store,
		policyResolver:      policyResolver,
		providerExecutor:    providerExecutor,
		workerID:            strings.TrimSpace(workerID),
		outputRootDir:       cfg.OutputRootDir,
		periodicRunMaxPages: int64(cfg.PeriodicRunMaxPages),
		ndjsonFlushEvery:    cfg.NDJSONFlushEveryRecords,
		ndjsonFsyncOnClose:  cfg.NDJSONFsyncOnClose,
		newWriter: func(finalPath string, flushEvery int, fsyncOnClose bool) (runnerRecordWriter, error) {
			return newNDJSONWriter(finalPath, flushEvery, fsyncOnClose)
		},
	}
}

func (r *runner) Run(ctx context.Context, runID string) error {
	return r.run(ctx, runID)
}

func (r *runner) run(ctx context.Context, runID string) error {
	if r == nil || r.store == nil {
		return errors.New("runner store is not configured")
	}

	view, err := r.store.LoadRunTask(ctx, strings.TrimSpace(runID))
	if err != nil {
		return err
	}

	outputPath := r.resolveOutputPath(view)
	startedAt := time.Now().UTC()
	if view.Run.StartedAt != nil {
		startedAt = view.Run.StartedAt.UTC()
	}

	requiredCount := int64(0)
	if view.Task.RequiredCount != nil && *view.Task.RequiredCount > 0 {
		requiredCount = *view.Task.RequiredCount
	}
	perRunCount := int64(0)
	if view.Task.PerRunCount != nil && *view.Task.PerRunCount > 0 {
		perRunCount = *view.Task.PerRunCount
	}

	if committedOutput, err := committedRunOutputExists(outputPath); err != nil {
		return err
	} else if committedOutput {
		if runAlreadyFinished(view.Run) {
			return nil
		}
		return r.finishCommittedRunRecovery(ctx, view, outputPath, requiredCount, perRunCount)
	}

	if r.policyResolver == nil {
		return errors.New("policy resolver is not configured")
	}
	if r.providerExecutor == nil {
		return errors.New("provider executor is not configured")
	}
	if r.newWriter == nil {
		return errors.New("runner writer is not configured")
	}

	policy, err := r.resolvePolicy(ctx)
	if err != nil {
		return err
	}

	writer, err := r.newWriter(outputPath, r.ndjsonFlushEvery, r.ndjsonFsyncOnClose)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = writer.Close()
	}()

	pageCount := view.Run.PageCount
	fetchedCount := view.Run.FetchedCount
	newCount := view.Run.NewCount
	duplicateCount := view.Run.DuplicateCount
	nextCursor := strings.TrimSpace(view.Run.NextCursor)
	resumeCursor, resumeOffset := initialResumeState(view)
	updateResume := view.Task.TaskType == TaskTypePeriodic

	if err := r.persistRunProgress(ctx, writer, updateRunProgressParams{
		RunID:          view.Run.RunID,
		Status:         RunStatusRunning,
		StartedAt:      &startedAt,
		OutputPath:     outputPath,
		PageCount:      pageCount,
		FetchedCount:   fetchedCount,
		NewCount:       newCount,
		DuplicateCount: duplicateCount,
		NextCursor:     nextCursor,
		ResumeCursor:   resumeCursor,
		ResumeOffset:   resumeOffset,
		UpdateResume:   updateResume,
	}); err != nil {
		return err
	}

	stopReason := ""
	taskStatusOverride := ""
	for {
		latestView, err := r.store.LoadRunTask(ctx, view.Run.RunID)
		if err != nil {
			return err
		}
		view.Task = latestView.Task
		if requiredCount > 0 && view.Task.CompletedCount+newCount >= requiredCount {
			stopReason = stopReasonRequired
			taskStatusOverride = TaskStatusSucceeded
			break
		}
		if perRunCount > 0 && newCount >= perRunCount {
			stopReason = stopReasonPerRun
			break
		}
		if overrideStatus, overrideReason := inactiveTaskState(view.Task.Status); overrideStatus != "" {
			taskStatusOverride = overrideStatus
			stopReason = overrideReason
			break
		}

		currentCursor := strings.TrimSpace(resumeCursor)
		currentOffset := normalizeResumeOffset(resumeOffset)
		providerPayload, err := r.fetchPage(ctx, policy, view.Task, currentCursor)
		if err != nil {
			return err
		}

		page, err := extractPage(providerPayload.Body)
		if err != nil {
			return err
		}

		pageCount++
		pageSize := int64(len(page.Posts))
		fetchedCount += pageSize
		nextCursor = strings.TrimSpace(page.NextCursor)

		if pageSize == 0 {
			resumeCursor = ""
			resumeOffset = 0
			if view.Task.TaskType == TaskTypePeriodic && nextCursor != "" {
				stopReason = stopReasonEmptyCursor
				taskStatusOverride = TaskStatusPaused
			} else {
				stopReason = stopReasonEmptyPage
			}
			if err := r.persistRunProgress(ctx, writer, updateRunProgressParams{
				RunID:          view.Run.RunID,
				Status:         RunStatusRunning,
				StartedAt:      &startedAt,
				OutputPath:     outputPath,
				PageCount:      pageCount,
				FetchedCount:   fetchedCount,
				NewCount:       newCount,
				DuplicateCount: duplicateCount,
				NextCursor:     nextCursor,
				ResumeCursor:   resumeCursor,
				ResumeOffset:   resumeOffset,
				UpdateResume:   updateResume,
			}); err != nil {
				return err
			}
			break
		}

		if currentOffset >= pageSize {
			resumeCursor = nextCursor
			resumeOffset = 0
			if err := r.persistRunProgress(ctx, writer, updateRunProgressParams{
				RunID:          view.Run.RunID,
				Status:         RunStatusRunning,
				StartedAt:      &startedAt,
				OutputPath:     outputPath,
				PageCount:      pageCount,
				FetchedCount:   fetchedCount,
				NewCount:       newCount,
				DuplicateCount: duplicateCount,
				NextCursor:     nextCursor,
				ResumeCursor:   resumeCursor,
				ResumeOffset:   resumeOffset,
				UpdateResume:   updateResume,
			}); err != nil {
				return err
			}
			if view.Task.TaskType == TaskTypePeriodic && pageCount >= r.periodicRunMaxPages {
				stopReason = stopReasonPageLimit
				break
			}
			if resumeCursor == "" {
				break
			}
			continue
		}

		requiredRemaining := remainingCount(requiredCount, view.Task.CompletedCount+newCount)
		perRunRemaining := remainingCount(perRunCount, newCount)
		pending, processedOffset, duplicateDelta, requiredReached, err := r.collectPendingPosts(ctx, view, page.Posts, currentOffset, requiredRemaining, perRunRemaining)
		if err != nil {
			return err
		}
		if err := r.appendAndFlushPendingPosts(writer, view, pending); err != nil {
			return err
		}
		if err := r.commitPendingPosts(ctx, view, pending); err != nil {
			return err
		}
		newCount += int64(len(pending))
		duplicateCount += duplicateDelta
		if requiredReached {
			if requiredCount > 0 && view.Task.CompletedCount+newCount >= requiredCount {
				stopReason = stopReasonRequired
				taskStatusOverride = TaskStatusSucceeded
			} else if perRunCount > 0 && newCount >= perRunCount {
				stopReason = stopReasonPerRun
			}
		}

		if requiredReached {
			if processedOffset >= pageSize {
				resumeCursor = nextCursor
				resumeOffset = 0
			} else {
				resumeCursor = currentCursor
				resumeOffset = processedOffset
			}
		} else {
			resumeCursor = nextCursor
			resumeOffset = 0
		}
		if err := r.persistRunProgress(ctx, writer, updateRunProgressParams{
			RunID:          view.Run.RunID,
			Status:         RunStatusRunning,
			StartedAt:      &startedAt,
			OutputPath:     outputPath,
			PageCount:      pageCount,
			FetchedCount:   fetchedCount,
			NewCount:       newCount,
			DuplicateCount: duplicateCount,
			NextCursor:     nextCursor,
			ResumeCursor:   resumeCursor,
			ResumeOffset:   resumeOffset,
			UpdateResume:   updateResume,
		}); err != nil {
			return err
		}

		if requiredReached {
			break
		}
		if view.Task.TaskType == TaskTypePeriodic && pageCount >= r.periodicRunMaxPages {
			stopReason = stopReasonPageLimit
			break
		}
		if nextCursor == "" {
			break
		}
	}

	if err := writer.Commit(); err != nil {
		return err
	}
	committed = true

	taskStatus := finalTaskStatus(view.Task, stopReason, taskStatusOverride)
	completedCount := view.Task.CompletedCount + newCount
	if err := r.store.MarkRunFinished(ctx, finishRunParams{
		RunID:          view.Run.RunID,
		TaskID:         view.Task.TaskID,
		RunStatus:      RunStatusSucceeded,
		TaskStatus:     taskStatus,
		StopReason:     stopReason,
		OutputPath:     outputPath,
		PageCount:      pageCount,
		FetchedCount:   fetchedCount,
		NewCount:       newCount,
		DuplicateCount: duplicateCount,
		NextCursor:     nextCursor,
		ResumeCursor:   resumeCursor,
		ResumeOffset:   resumeOffset,
		UpdateResume:   updateResume,
		EndedAt:        time.Now().UTC(),
		CompletedCount: &completedCount,
	}); err != nil {
		return err
	}

	return nil
}

func (r *runner) resolvePolicy(ctx context.Context) (resolvedPolicy, error) {
	// Keep policy freshness centralized in policy-rpc. The worker must not add
	// a second process-local TTL cache here, otherwise policy publishes cannot
	// take effect immediately for routing or provider credential rotation.
	resp, err := r.policyResolver.ResolvePolicy(ctx, &policyservice.ResolvePolicyRequest{
		Path:   collectorPolicyPath,
		Method: collectorPolicyMethod,
	})
	if err != nil {
		return resolvedPolicy{}, err
	}
	if resp == nil {
		return resolvedPolicy{}, errors.New("policy resolver returned nil response")
	}

	var policy resolvedPolicy
	if err := json.Unmarshal(resp.Data, &policy); err != nil {
		return resolvedPolicy{}, fmt.Errorf("decode policy response: %w", err)
	}
	if strings.TrimSpace(policy.PolicyKey) == "" {
		policy.PolicyKey = collectorPolicyKey
	}
	if strings.TrimSpace(policy.UpstreamMethod) == "" || strings.TrimSpace(policy.UpstreamPath) == "" {
		return resolvedPolicy{}, errors.New("policy response is missing upstream route")
	}
	return policy, nil
}

func committedRunOutputExists(outputPath string) (bool, error) {
	outputPath = filepath.Clean(strings.TrimSpace(outputPath))
	if outputPath == "" {
		return false, nil
	}

	partPath := outputPath + ".part"
	if _, err := os.Stat(partPath); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	if _, err := os.Stat(outputPath); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func runAlreadyFinished(run workerRun) bool {
	if run.EndedAt != nil {
		return true
	}
	return strings.TrimSpace(run.Status) == RunStatusSucceeded
}

func (r *runner) finishCommittedRunRecovery(ctx context.Context, view runTaskView, outputPath string, requiredCount, perRunCount int64) error {
	stopReason, taskStatusOverride := r.deriveCommittedRunOutcome(view, requiredCount, perRunCount)
	taskStatus := finalTaskStatus(view.Task, stopReason, taskStatusOverride)
	completedCount := view.Task.CompletedCount + view.Run.NewCount

	return r.store.MarkRunFinished(ctx, finishRunParams{
		RunID:          view.Run.RunID,
		TaskID:         view.Task.TaskID,
		RunStatus:      RunStatusSucceeded,
		TaskStatus:     taskStatus,
		StopReason:     stopReason,
		OutputPath:     outputPath,
		PageCount:      view.Run.PageCount,
		FetchedCount:   view.Run.FetchedCount,
		NewCount:       view.Run.NewCount,
		DuplicateCount: view.Run.DuplicateCount,
		NextCursor:     view.Run.NextCursor,
		ResumeCursor:   view.Run.ResumeCursor,
		ResumeOffset:   view.Run.ResumeOffset,
		UpdateResume:   view.Task.TaskType == TaskTypePeriodic,
		EndedAt:        time.Now().UTC(),
		CompletedCount: &completedCount,
	})
}

func (r *runner) deriveCommittedRunOutcome(view runTaskView, requiredCount, perRunCount int64) (string, string) {
	if requiredCount > 0 && view.Task.CompletedCount+view.Run.NewCount >= requiredCount {
		return stopReasonRequired, TaskStatusSucceeded
	}
	if perRunCount > 0 && view.Run.NewCount >= perRunCount {
		return stopReasonPerRun, ""
	}
	if overrideStatus, overrideReason := inactiveTaskState(view.Task.Status); overrideStatus != "" {
		return overrideReason, overrideStatus
	}
	if view.Task.TaskType == TaskTypePeriodic && view.Run.PageCount >= r.periodicRunMaxPages {
		return stopReasonPageLimit, ""
	}
	if strings.TrimSpace(view.Run.NextCursor) != "" && strings.TrimSpace(view.Run.ResumeCursor) == "" && view.Run.ResumeOffset == 0 {
		if view.Task.TaskType == TaskTypePeriodic {
			return stopReasonEmptyCursor, TaskStatusPaused
		}
		return stopReasonEmptyPage, ""
	}
	if view.Run.PageCount > 0 && view.Run.FetchedCount == 0 {
		return stopReasonEmptyPage, ""
	}
	return strings.TrimSpace(view.Run.StopReason), ""
}

func finalTaskStatus(task workerTask, stopReason, taskStatusOverride string) string {
	if taskStatusOverride != "" {
		return taskStatusOverride
	}
	if task.TaskType == TaskTypePeriodic && stopReason != stopReasonRequired {
		return TaskStatusPending
	}
	return TaskStatusSucceeded
}

func (r *runner) collectPendingPosts(ctx context.Context, view runTaskView, posts []extractedPost, currentOffset, requiredRemaining, perRunRemaining int64) ([]pendingCollectedPost, int64, int64, bool, error) {
	if currentOffset >= int64(len(posts)) {
		return nil, currentOffset, 0, false, nil
	}

	postIDs := make([]string, 0, len(posts)-int(currentOffset))
	for _, post := range posts[int(currentOffset):] {
		postIDs = append(postIDs, post.PostID)
	}

	historicalSeen, err := r.store.ListTaskSeenPosts(ctx, view.Task.TaskID, postIDs)
	if err != nil {
		return nil, currentOffset, 0, false, err
	}

	pageSeen := make(map[string]struct{}, len(postIDs))
	pending := make([]pendingCollectedPost, 0, len(postIDs))
	processedOffset := currentOffset
	duplicateCountDelta := int64(0)
	limit := firstPositiveLimit(requiredRemaining, perRunRemaining)
	limitReached := false

	for _, post := range posts[int(currentOffset):] {
		processedOffset++
		postID := strings.TrimSpace(post.PostID)
		if historicalSeen[postID] {
			duplicateCountDelta++
			continue
		}
		if _, exists := pageSeen[postID]; exists {
			duplicateCountDelta++
			continue
		}
		pageSeen[postID] = struct{}{}

		seenAt := time.Now().UTC()
		pending = append(pending, pendingCollectedPost{
			extractedPost: post,
			SeenAt:        seenAt,
			UsageMonth:    time.Date(seenAt.Year(), seenAt.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
		})

		if limit > 0 && int64(len(pending)) >= limit {
			limitReached = true
			break
		}
	}

	return pending, processedOffset, duplicateCountDelta, limitReached, nil
}

func (r *runner) appendAndFlushPendingPosts(writer runnerRecordWriter, view runTaskView, pending []pendingCollectedPost) error {
	if len(pending) == 0 {
		return nil
	}

	restoreBatch := writer.beginBatchAppend(len(pending))

	for _, post := range pending {
		if err := writer.Append(collectorOutputRecord{
			TaskID:    view.Task.TaskID,
			RunID:     view.Run.RunID,
			Keyword:   view.Task.Keyword,
			PostID:    post.PostID,
			WorkerID:  r.workerID,
			Collected: post.SeenAt.Format(time.RFC3339Nano),
			Raw:       post.RawPayload,
		}); err != nil {
			restoreBatch()
			return err
		}
	}

	restoreBatch()
	return writer.Flush()
}

func (r *runner) commitPendingPosts(ctx context.Context, view runTaskView, pending []pendingCollectedPost) error {
	for _, post := range pending {
		if _, err := r.store.RecordKeywordMonthlyUsage(ctx, view.Task.Keyword, post.UsageMonth, post.PostID, view.Task.TaskID, post.SeenAt); err != nil {
			return err
		}
		if _, err := r.store.RecordTaskSeenPost(ctx, view.Task.TaskID, post.PostID, view.Run.RunID, post.SeenAt); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) persistRunProgress(ctx context.Context, writer runnerFlusher, params updateRunProgressParams) error {
	if writer != nil {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return r.store.UpdateRunProgress(ctx, params)
}

func remainingCount(limit, used int64) int64 {
	if limit <= 0 {
		return 0
	}
	if used >= limit {
		return 0
	}
	return limit - used
}

func firstPositiveLimit(values ...int64) int64 {
	limit := int64(0)
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if limit == 0 || value < limit {
			limit = value
		}
	}
	return limit
}

func (w *ndjsonWriter) beginBatchAppend(batchSize int) func() {
	if w == nil || batchSize <= 0 {
		return func() {}
	}

	previousFlushEvery := w.flushEvery
	safeFlushEvery := previousFlushEvery
	requiredFlushEvery := w.pendingLines + batchSize + 1
	if requiredFlushEvery > safeFlushEvery {
		safeFlushEvery = requiredFlushEvery
	}
	w.flushEvery = safeFlushEvery

	return func() {
		w.flushEvery = previousFlushEvery
	}
}

func (r *runner) fetchPage(ctx context.Context, policy resolvedPolicy, task workerTask, cursor string) (providerExecutionPayload, error) {
	query := make(map[string]any, len(policy.DefaultParams)+4)
	for key, value := range policy.DefaultParams {
		query[key] = value
	}
	query["words"] = task.Keyword
	if task.Since != "" {
		query["since"] = task.Since
	}
	if task.Until != "" {
		query["until"] = task.Until
	}
	if strings.TrimSpace(cursor) != "" {
		query["cursor"] = strings.TrimSpace(cursor)
	}

	// Apply default product=Top for search_tweets_v1, matching gateway behavior
	if policy.PolicyKey == "search_tweets_v1" && policy.UpstreamPath == "/base/apitools/search" {
		if !hasNonEmptyValue(query["product"]) {
			query["product"] = "Top"
		}
	}

	// Apply default resFormat=json when not specified, matching gateway behavior
	if !hasNonEmptyValue(query["resFormat"]) {
		query["resFormat"] = "json"
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return providerExecutionPayload{}, fmt.Errorf("encode provider query: %w", err)
	}

	resp, err := r.providerExecutor.ExecutePolicy(ctx, &providerservice.ExecutePolicyRequest{
		RequestId:      shared.NewID("collector-run"),
		PolicyKey:      policy.PolicyKey,
		UpstreamMethod: policy.UpstreamMethod,
		UpstreamPath:   policy.UpstreamPath,
		QueryJson:      string(queryJSON),
		ProviderName:   policy.ProviderName,
		ProviderApiKey: policy.ProviderAPIKey,
	})
	if err != nil {
		return providerExecutionPayload{}, err
	}
	if resp == nil {
		return providerExecutionPayload{}, errors.New("provider executor returned nil response")
	}

	var payload providerExecutionPayload
	if err := json.Unmarshal(resp.Data, &payload); err != nil {
		return providerExecutionPayload{}, fmt.Errorf("decode provider response: %w", err)
	}
	if payload.StatusCode < 200 || payload.StatusCode >= 300 {
		return providerExecutionPayload{}, fmt.Errorf("provider returned non-success status %d", payload.StatusCode)
	}
	if len(payload.Body) == 0 {
		return providerExecutionPayload{}, errors.New("provider response body is empty")
	}
	return payload, nil
}

func (r *runner) resolveOutputPath(view runTaskView) string {
	if path := strings.TrimSpace(view.Run.OutputPath); path != "" {
		return filepath.Clean(path)
	}

	taskDir := filepath.Join(r.outputRootDir, view.Task.TaskID)
	if view.Task.TaskType == TaskTypeRange {
		return filepath.Join(taskDir, view.Task.TaskID+".ndjson")
	}

	scheduledAt := view.Run.ScheduledAt.UTC()
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().UTC()
	}
	name := fmt.Sprintf("%s_%s_run_%d.ndjson", view.Task.TaskID, scheduledAt.Format("20060102T150405Z"), view.Run.RunNo)
	return filepath.Join(taskDir, name)
}

func initialResumeState(view runTaskView) (string, int64) {
	if shouldSeedRunResumeFromTask(view.Run) {
		return strings.TrimSpace(view.Task.ResumeCursor), normalizeResumeOffset(view.Task.ResumeOffset)
	}

	resumeCursor := strings.TrimSpace(view.Run.ResumeCursor)
	resumeOffset := normalizeResumeOffset(view.Run.ResumeOffset)
	if resumeCursor == "" && resumeOffset == 0 && strings.TrimSpace(view.Run.NextCursor) != "" {
		return strings.TrimSpace(view.Run.NextCursor), 0
	}
	return resumeCursor, resumeOffset
}

func shouldSeedRunResumeFromTask(run workerRun) bool {
	return strings.TrimSpace(run.ResumeCursor) == "" &&
		run.ResumeOffset == 0 &&
		strings.TrimSpace(run.NextCursor) == "" &&
		strings.TrimSpace(run.OutputPath) == "" &&
		run.PageCount == 0 &&
		run.FetchedCount == 0 &&
		run.NewCount == 0 &&
		run.DuplicateCount == 0 &&
		run.StartedAt == nil &&
		run.EndedAt == nil
}

func normalizeResumeOffset(offset int64) int64 {
	if offset < 0 {
		return 0
	}
	return offset
}

func inactiveTaskState(status string) (taskStatus string, stopReason string) {
	switch strings.TrimSpace(status) {
	case TaskStatusPaused:
		return TaskStatusPaused, stopReasonStopped
	case TaskStatusFailed:
		return TaskStatusFailed, stopReasonTaskFailed
	case TaskStatusSucceeded:
		return TaskStatusSucceeded, stopReasonTaskDone
	default:
		return "", ""
	}
}

func hasNonEmptyValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []string:
		for _, item := range typed {
			if strings.TrimSpace(item) != "" {
				return true
			}
		}
		return false
	case []any:
		for _, item := range typed {
			if hasNonEmptyValue(item) {
				return true
			}
		}
		return false
	default:
		return true
	}
}
