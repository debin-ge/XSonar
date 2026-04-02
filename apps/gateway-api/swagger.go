package main

import (
	_ "embed"
	"encoding/json"
	"strings"

	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/rest"
)

//go:embed docs/swagger.json
var gatewaySwaggerDoc []byte

type gatewaySwaggerDocSource struct {
	doc  []byte
	mode string
}

func (s gatewaySwaggerDocSource) ReadSwaggerDoc() ([]byte, error) {
	return rewriteGatewaySwaggerDoc(s.doc, s.mode)
}

func addSwaggerRoutes(server *rest.Server, mode string) {
	shared.AddSwaggerRoutes(server, gatewaySwaggerDocSource{
		doc:  gatewaySwaggerDoc,
		mode: mode,
	})
}

func rewriteGatewaySwaggerDoc(doc []byte, mode string) ([]byte, error) {
	var root map[string]any
	if err := json.Unmarshal(doc, &root); err != nil {
		return nil, err
	}

	paths, ok := root["paths"].(map[string]any)
	if !ok {
		return doc, nil
	}

	requiredAuthHeaders := gatewaySwaggerRequiredHeaders(mode)
	for _, rawPathItem := range paths {
		pathItem, ok := rawPathItem.(map[string]any)
		if !ok {
			continue
		}
		for _, rawOperation := range pathItem {
			operation, ok := rawOperation.(map[string]any)
			if !ok {
				continue
			}
			parameters, ok := operation["parameters"].([]any)
			if !ok {
				continue
			}

			filtered := make([]any, 0, len(parameters))
			for _, rawParameter := range parameters {
				parameter, ok := rawParameter.(map[string]any)
				if !ok {
					filtered = append(filtered, rawParameter)
					continue
				}

				name, _ := parameter["name"].(string)
				if !isGatewaySwaggerAuthHeader(name) {
					filtered = append(filtered, parameter)
					continue
				}
				if !requiredAuthHeaders[name] {
					continue
				}

				parameter["in"] = "header"
				parameter["required"] = true
				filtered = append(filtered, parameter)
			}
			operation["parameters"] = filtered
		}
	}

	return json.Marshal(root)
}

func gatewaySwaggerRequiredHeaders(mode string) map[string]bool {
	if gatewaySwaggerUsesDevelopmentAuth(mode) {
		return map[string]bool{
			"AppKey":    true,
			"AppSecret": true,
		}
	}

	return map[string]bool{
		"AppKey":    true,
		"Timestamp": true,
		"Nonce":     true,
		"Signature": true,
	}
}

func isGatewaySwaggerAuthHeader(name string) bool {
	switch name {
	case "AppKey", "AppSecret", "Timestamp", "Nonce", "Signature":
		return true
	default:
		return false
	}
}

func gatewaySwaggerUsesDevelopmentAuth(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "dev", "test":
		return true
	default:
		return false
	}
}
