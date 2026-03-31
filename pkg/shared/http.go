package shared

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"xsonar/pkg/model"
)

func EnsureRequestID(w http.ResponseWriter, r *http.Request) string {
	const header = "X-Request-ID"
	requestID := r.Header.Get(header)
	if requestID == "" {
		requestID = newRequestID()
	}
	w.Header().Set(header, requestID)
	return requestID
}

func WriteEnvelope(w http.ResponseWriter, statusCode, code int, message string, data any, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	if requestID != "" {
		w.Header().Set("X-Request-ID", requestID)
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(model.Envelope{
		Code:      code,
		Message:   message,
		Data:      data,
		RequestID: requestID,
	})
}

func WriteRawEnvelope(w http.ResponseWriter, statusCode, code int, message string, data json.RawMessage, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	if requestID != "" {
		w.Header().Set("X-Request-ID", requestID)
	}
	w.WriteHeader(statusCode)

	type rawEnvelope struct {
		Code      int             `json:"code"`
		Message   string          `json:"message"`
		Data      json.RawMessage `json:"data,omitempty"`
		RequestID string          `json:"request_id,omitempty"`
	}

	_ = json.NewEncoder(w).Encode(rawEnvelope{
		Code:      code,
		Message:   message,
		Data:      data,
		RequestID: requestID,
	})
}

func WriteRawOK(w http.ResponseWriter, data json.RawMessage, requestID string) {
	WriteRawEnvelope(w, http.StatusOK, model.CodeOK, "ok", data, requestID)
}

func newRequestID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(buffer)
}
