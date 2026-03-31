package internal

import (
	"net/http"
	"strings"

	"github.com/zeromicro/go-zero/rest/pathvar"
)

func pathParam(r *http.Request, key string) string {
	if r == nil {
		return ""
	}
	if value := strings.TrimSpace(r.PathValue(key)); value != "" {
		return value
	}
	if value := strings.TrimSpace(pathvar.Vars(r)[key]); value != "" {
		return value
	}
	return ""
}
