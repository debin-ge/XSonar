package shared

import (
	"encoding/json"
	"net/http"

	"github.com/zeromicro/go-zero/rest"
)

// AddHealthzRoute registers a lightweight process health endpoint for local probes.
func AddHealthzRoute(server *rest.Server, service string) {
	server.AddRoute(rest.Route{
		Method:  http.MethodGet,
		Path:    "/healthz",
		Handler: HealthzHandler(service),
	})
}

func HealthzHandler(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": service,
		})
	}
}
