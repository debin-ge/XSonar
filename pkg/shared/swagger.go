package shared

import (
	"embed"
	"net/http"
	"path"
	"strings"

	"github.com/zeromicro/go-zero/rest"
)

const (
	defaultSwaggerRoutePrefix = "/swagger"
)

// SwaggerDocSource provides the raw swagger document bytes for the shared UI routes.
type SwaggerDocSource interface {
	ReadSwaggerDoc() ([]byte, error)
}

//go:embed swaggerui/index.html swaggerui/oauth2-redirect.html swaggerui/swagger-ui.css swaggerui/swagger-ui-bundle.js swaggerui/swagger-ui-standalone-preset.js
var swaggerUIAssets embed.FS

// AddSwaggerRoutes registers the shared Swagger document and UI routes.
func AddSwaggerRoutes(server *rest.Server, docSource SwaggerDocSource) {
	server.AddRoutes(SwaggerRoutes(defaultSwaggerRoutePrefix, docSource))
}

// SwaggerRoutes builds the shared Swagger document and UI routes for the provided prefix.
func SwaggerRoutes(prefix string, docSource SwaggerDocSource) []rest.Route {
	routePrefix := normalizeSwaggerPrefix(prefix)

	return []rest.Route{
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/doc.json",
			Handler: swaggerDocHandler(docSource),
		},
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/index.html",
			Handler: swaggerAssetHandler("swaggerui/index.html"),
		},
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/swagger-ui.css",
			Handler: swaggerAssetHandler("swaggerui/swagger-ui.css"),
		},
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/swagger-ui-bundle.js",
			Handler: swaggerAssetHandler("swaggerui/swagger-ui-bundle.js"),
		},
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/swagger-ui-standalone-preset.js",
			Handler: swaggerAssetHandler("swaggerui/swagger-ui-standalone-preset.js"),
		},
		{
			Method:  http.MethodGet,
			Path:    routePrefix + "/oauth2-redirect.html",
			Handler: swaggerAssetHandler("swaggerui/oauth2-redirect.html"),
		},
	}
}

func normalizeSwaggerPrefix(prefix string) string {
	normalized := strings.TrimSpace(prefix)
	if normalized == "" {
		return defaultSwaggerRoutePrefix
	}

	normalized = strings.TrimRight(normalized, "/")
	if normalized == "" {
		return defaultSwaggerRoutePrefix
	}

	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}

	return normalized
}

func swaggerDocHandler(docSource SwaggerDocSource) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if docSource == nil {
			http.Error(w, "swagger document source is not configured", http.StatusInternalServerError)
			return
		}

		doc, err := docSource.ReadSwaggerDoc()
		if err != nil {
			http.Error(w, "failed to read swagger document", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(doc)
	}
}

func swaggerAssetHandler(assetPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := swaggerUIAssets.ReadFile(assetPath)
		if err != nil {
			http.Error(w, "swagger asset is unavailable", http.StatusInternalServerError)
			return
		}

		name := path.Base(assetPath)
		contentType := contentTypeByName(name)
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}
}

func contentTypeByName(name string) string {
	switch path.Ext(name) {
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".html":
		return "text/html; charset=utf-8"
	default:
		return ""
	}
}
