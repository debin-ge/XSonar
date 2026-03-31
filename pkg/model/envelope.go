package model

const (
	CodeOK             = 0
	CodeNotImplemented = 100001
	CodeInternalError  = 100500
)

type Envelope struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Data      any    `json:"data,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}
