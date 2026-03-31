package shared

import (
	"testing"
	"time"
)

func TestRequestLogFieldsIncludesCoreFields(t *testing.T) {
	start := time.Now().Add(-25 * time.Millisecond).UTC()
	fields := RequestLogFields("req-test-1", start, map[string]any{
		"status_code":   200,
		"result_code":   "UPSTREAM_OK",
		"error_summary": "",
		"tenant_id":     nil,
	})

	if fields["request_id"] != "req-test-1" {
		t.Fatalf("unexpected request_id: %#v", fields["request_id"])
	}
	if _, ok := fields["created_at"].(string); !ok {
		t.Fatalf("expected created_at to be present, got %#v", fields["created_at"])
	}
	durationMS, ok := fields["duration_ms"].(int64)
	if !ok {
		t.Fatalf("expected duration_ms int64, got %#v", fields["duration_ms"])
	}
	if durationMS < 0 {
		t.Fatalf("expected non-negative duration, got %d", durationMS)
	}
	if fields["status_code"] != 200 {
		t.Fatalf("unexpected status_code: %#v", fields["status_code"])
	}
	if fields["result_code"] != "UPSTREAM_OK" {
		t.Fatalf("unexpected result_code: %#v", fields["result_code"])
	}
	if _, exists := fields["error_summary"]; exists {
		t.Fatalf("expected empty error_summary to be skipped, got %#v", fields["error_summary"])
	}
	if _, exists := fields["tenant_id"]; exists {
		t.Fatalf("expected nil tenant_id to be skipped, got %#v", fields["tenant_id"])
	}
}
