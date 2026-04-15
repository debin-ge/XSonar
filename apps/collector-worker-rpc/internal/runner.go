package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	stopReasonRequired    = "required_count_reached"
	stopReasonEmptyPage   = "empty_page"
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
	}
}

func (r *runner) Run(ctx context.Context, runID string) error {
	return r.run(ctx, runID)
}

func (r *runner) run(ctx context.Context, runID string) error {
	if r == nil || r.store == nil {
		return errors.New("runner store is not configured")
	}
	if r.policyResolver == nil {
		return errors.New("policy resolver is not configured")
	}
	if r.providerExecutor == nil {
		return errors.New("provider executor is not configured")
	}

	view, err := r.store.LoadRunTask(ctx, strings.TrimSpace(runID))
	if err != nil {
		return err
	}

	policy, err := r.resolvePolicy(ctx)
	if err != nil {
		return err
	}

	outputPath := r.resolveOutputPath(view)
	startedAt := time.Now().UTC()
	if view.Run.StartedAt != nil {
		startedAt = view.Run.StartedAt.UTC()
	}

	writer, err := newNDJSONWriter(outputPath, r.ndjsonFlushEvery, r.ndjsonFsyncOnClose)
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

	if err := r.store.UpdateRunProgress(ctx, updateRunProgressParams{
		RunID:          view.Run.RunID,
		Status:         RunStatusRunning,
		StartedAt:      &startedAt,
		OutputPath:     outputPath,
		PageCount:      pageCount,
		FetchedCount:   fetchedCount,
		NewCount:       newCount,
		DuplicateCount: duplicateCount,
		NextCursor:     nextCursor,
	}); err != nil {
		return err
	}

	stopReason := ""
	requiredCount := int64(0)
	if view.Task.RequiredCount != nil && *view.Task.RequiredCount > 0 {
		requiredCount = *view.Task.RequiredCount
	}
	for {
		providerPayload, err := r.fetchPage(ctx, policy, view.Task, nextCursor)
		if err != nil {
			return err
		}

		page, err := extractPage(providerPayload.Body)
		if err != nil {
			return err
		}

		pageCount++
		fetchedCount += int64(len(page.Posts))

		seenAt := time.Now().UTC()
		usageMonth := time.Date(seenAt.Year(), seenAt.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		requiredReached := false
		for _, post := range page.Posts {
			if _, err := r.store.RecordKeywordMonthlyUsage(ctx, view.Task.Keyword, usageMonth, post.PostID, view.Task.TaskID, seenAt); err != nil {
				return err
			}

			firstSeen, err := r.store.RecordTaskSeenPost(ctx, view.Task.TaskID, post.PostID, view.Run.RunID, seenAt)
			if err != nil {
				return err
			}
			if !firstSeen {
				duplicateCount++
				continue
			}

			newCount++
			if err := writer.Append(collectorOutputRecord{
				TaskID:    view.Task.TaskID,
				RunID:     view.Run.RunID,
				Keyword:   view.Task.Keyword,
				PostID:    post.PostID,
				WorkerID:  r.workerID,
				Collected: seenAt.Format(time.RFC3339Nano),
				Raw:       post.RawPayload,
			}); err != nil {
				return err
			}
			if requiredCount > 0 && view.Task.CompletedCount+newCount >= requiredCount {
				stopReason = stopReasonRequired
				requiredReached = true
				break
			}
		}

		nextCursor = strings.TrimSpace(page.NextCursor)
		if err := r.store.UpdateRunProgress(ctx, updateRunProgressParams{
			RunID:          view.Run.RunID,
			Status:         RunStatusRunning,
			StartedAt:      &startedAt,
			OutputPath:     outputPath,
			PageCount:      pageCount,
			FetchedCount:   fetchedCount,
			NewCount:       newCount,
			DuplicateCount: duplicateCount,
			NextCursor:     nextCursor,
		}); err != nil {
			return err
		}

		if len(page.Posts) == 0 {
			stopReason = stopReasonEmptyPage
			break
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

	taskStatus := TaskStatusSucceeded
	if view.Task.TaskType == TaskTypePeriodic && stopReason != stopReasonRequired {
		taskStatus = TaskStatusPending
	}
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
		EndedAt:        time.Now().UTC(),
		CompletedCount: &completedCount,
	}); err != nil {
		return err
	}

	return nil
}

func (r *runner) resolvePolicy(ctx context.Context) (resolvedPolicy, error) {
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
		if !hasNonEmptyProduct(query["product"]) {
			query["product"] = "Top"
		}
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

func hasNonEmptyProduct(value any) bool {
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
			if hasNonEmptyProduct(item) {
				return true
			}
		}
		return false
	default:
		return true
	}
}
