package testkit

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"
)

func TestTestkitRoutesMatchGeneratedGatewayRoutes(t *testing.T) {
	expected := extractRoutePaths(t, filepath.Join("..", "internal", "handler", "routes.go"), regexp.MustCompile(`Path:\s+"([^"]+)"`))
	actual := extractRoutePaths(t, "handler.go", regexp.MustCompile(`"GET ([^"]+)"`))

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("testkit routes do not match generated gateway routes:\nactual=%#v\nexpected=%#v", actual, expected)
	}
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
