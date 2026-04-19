package testkit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"xsonar/apps/scheduler-rpc/schedulerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

func TestTestkitRoutesMatchGeneratedGatewayRoutes(t *testing.T) {
	expected := extractRoutePaths(t, filepath.Join("..", "internal", "handler", "routes.go"), regexp.MustCompile(`Path:\s+"(/v1/[^"]+)"`))
	actual := extractRoutePaths(t, "handler.go", regexp.MustCompile(`"(?:GET|POST) (/v1/[^"]+)"`))
	for i := range actual {
		actual[i] = normalizeCollectorRoutePath(actual[i])
	}
	actual = slices.Compact(actual)
	slices.Sort(actual)

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("testkit routes do not match generated gateway routes:\nactual=%#v\nexpected=%#v", actual, expected)
	}
}

func TestTestkitIncludesCollectorRoutes(t *testing.T) {
	actual := extractRoutePaths(t, "handler.go", regexp.MustCompile(`"(?:GET|POST) (/v1/collector/tasks[^"]*)"`))
	for i := range actual {
		actual[i] = normalizeCollectorRoutePath(actual[i])
	}
	actual = slices.Compact(actual)
	expected := []string{
		"/v1/collector/tasks/:id",
		"/v1/collector/tasks/:id/runs",
		"/v1/collector/tasks/:id/stop",
		"/v1/collector/tasks/periodic",
		"/v1/collector/tasks/range",
	}

	slices.Sort(actual)
	slices.Sort(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("testkit collector routes mismatch:\nactual=%#v\nexpected=%#v", actual, expected)
	}
}

func TestTestkitCollectorTaskRoutesReachRequests(t *testing.T) {
	t.Run("get task", func(t *testing.T) {
		var recordedTaskID string
		handler := NewHandlerWithClientsAndMode(
			xlog.NewStdout("gateway-testkit"),
			nil,
			nil,
			nil,
			stubSchedulerRPC{
				getTaskFunc: func(_ context.Context, req *schedulerservice.GetTaskRequest) (*clients.EnvelopeResponse, error) {
					recordedTaskID = req.TaskId
					return okEnvelopeResponse(t, map[string]any{"task_id": req.TaskId}), nil
				},
			},
			"test-secret",
			"test-issuer",
			"",
		)

		req := httptest.NewRequest(http.MethodGet, "/v1/collector/tasks/task-1", nil)
		req.Header.Set("Authorization", "Bearer "+mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1"))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if recordedTaskID != "task-1" {
			t.Fatalf("expected task id task-1, got %q", recordedTaskID)
		}
	})

	t.Run("list task runs", func(t *testing.T) {
		var recordedTaskID string
		handler := NewHandlerWithClientsAndMode(
			xlog.NewStdout("gateway-testkit"),
			nil,
			nil,
			nil,
			stubSchedulerRPC{
				listTaskRunsFunc: func(_ context.Context, req *schedulerservice.ListTaskRunsRequest) (*clients.EnvelopeResponse, error) {
					recordedTaskID = req.TaskId
					return okEnvelopeResponse(t, map[string]any{"runs": []any{}}), nil
				},
			},
			"test-secret",
			"test-issuer",
			"",
		)

		req := httptest.NewRequest(http.MethodGet, "/v1/collector/tasks/task-1/runs", nil)
		req.Header.Set("Authorization", "Bearer "+mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1"))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if recordedTaskID != "task-1" {
			t.Fatalf("expected task id task-1, got %q", recordedTaskID)
		}
	})
}

func extractRoutePaths(t *testing.T, relativePath string, pattern *regexp.Regexp) []string {
	t.Helper()

	content, err := os.ReadFile(relativePath)
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}

	matches := pattern.FindAllStringSubmatch(string(content), -1)
	if len(matches) == 0 {
		t.Fatalf("no route paths matched in %s", relativePath)
	}

	paths := make([]string, 0, len(matches))
	for _, match := range matches {
		paths = append(paths, match[1])
	}
	slices.Sort(paths)
	return paths
}

func normalizeCollectorRoutePath(path string) string {
	replacer := strings.NewReplacer("{id}", ":id", ":id", ":id")
	return replacer.Replace(path)
}

type stubSchedulerRPC struct {
	getTaskFunc      func(ctx context.Context, req *schedulerservice.GetTaskRequest) (*clients.EnvelopeResponse, error)
	listTaskRunsFunc func(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*clients.EnvelopeResponse, error)
	stopTaskFunc     func(ctx context.Context, req *schedulerservice.StopTaskRequest) (*clients.EnvelopeResponse, error)
}

func (s stubSchedulerRPC) Health(context.Context) (*clients.EnvelopeResponse, error) {
	return okEnvelopeResponse(nil, map[string]any{"ok": true}), nil
}

func (s stubSchedulerRPC) CreateTask(context.Context, *schedulerservice.CreateTaskRequest) (*clients.EnvelopeResponse, error) {
	return nil, nil
}

func (s stubSchedulerRPC) GetTask(ctx context.Context, req *schedulerservice.GetTaskRequest) (*clients.EnvelopeResponse, error) {
	if s.getTaskFunc == nil {
		return nil, nil
	}
	return s.getTaskFunc(ctx, req)
}

func (s stubSchedulerRPC) ListTaskRuns(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*clients.EnvelopeResponse, error) {
	if s.listTaskRunsFunc == nil {
		return nil, nil
	}
	return s.listTaskRunsFunc(ctx, req)
}

func (s stubSchedulerRPC) StopTask(ctx context.Context, req *schedulerservice.StopTaskRequest) (*clients.EnvelopeResponse, error) {
	if s.stopTaskFunc == nil {
		return nil, nil
	}
	return s.stopTaskFunc(ctx, req)
}

func mustSignAdminJWT(t *testing.T, secret, issuer, subject string) string {
	t.Helper()

	token, err := shared.SignJWT(secret, issuer, subject, "gateway_app", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return token
}

func okEnvelopeResponse(t *testing.T, data any) *clients.EnvelopeResponse {
	if data == nil {
		return &clients.EnvelopeResponse{Code: model.CodeOK, Message: "success"}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		if t != nil {
			t.Fatalf("marshal envelope data: %v", err)
		}
		return nil
	}
	return &clients.EnvelopeResponse{
		Code:    model.CodeOK,
		Message: "success",
		Data:    raw,
	}
}
