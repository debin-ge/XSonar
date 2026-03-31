package shared

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"xsonar/pkg/model"
)

func DecodeJSONBody(r *http.Request, dst any) error {
	if r.Body == nil {
		return io.EOF
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dst); err != nil {
		return err
	}

	if decoder.More() {
		return errors.New("request body must contain a single JSON value")
	}

	return nil
}

func WriteError(w http.ResponseWriter, statusCode, code int, message, requestID string) {
	WriteEnvelope(w, statusCode, code, message, nil, requestID)
}

func WriteOK(w http.ResponseWriter, data any, requestID string) {
	WriteEnvelope(w, http.StatusOK, model.CodeOK, "ok", data, requestID)
}
