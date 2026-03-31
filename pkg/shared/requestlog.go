package shared

import (
	"strings"
	"time"

	"xsonar/pkg/xlog"
)

func RequestLogFields(requestID string, startedAt time.Time, fields map[string]any) map[string]any {
	record := map[string]any{
		"request_id": requestID,
	}
	if !startedAt.IsZero() {
		record["created_at"] = startedAt.UTC().Format(time.RFC3339Nano)
		record["duration_ms"] = time.Since(startedAt).Milliseconds()
	}

	for key, value := range fields {
		if shouldSkipLogField(value) {
			continue
		}
		record[key] = value
	}

	return record
}

func LogRequestInfo(logger *xlog.Logger, message, requestID string, startedAt time.Time, fields map[string]any) {
	if logger == nil {
		return
	}
	logger.Info(message, RequestLogFields(requestID, startedAt, fields))
}

func LogRequestError(logger *xlog.Logger, message, requestID string, startedAt time.Time, fields map[string]any) {
	if logger == nil {
		return
	}
	logger.Error(message, RequestLogFields(requestID, startedAt, fields))
}

func shouldSkipLogField(value any) bool {
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) == ""
	}
	return false
}
