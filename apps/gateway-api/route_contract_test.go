package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/zeromicro/go-zero/rest"
)

func TestGatewaySwaggerDeclaresExactQueryContractForAllPublicRoutes(t *testing.T) {
	paths := loadGatewaySwaggerPaths(t, "dev")

	expected := map[string]map[string]bool{
		"/v1/communities":             {"screenName": true},
		"/v1/lists":                   {"screenName": false, "userId": false},
		"/v1/search/box":              {"searchType": false, "words": true},
		"/v1/search/entertainment":    {},
		"/v1/search/explore":          {},
		"/v1/search/news":             {},
		"/v1/search/sports":           {},
		"/v1/search/trending":         {},
		"/v1/search/trends":           {"id": true},
		"/v1/search/tweets":           {"any": false, "count": false, "cursor": false, "from": false, "likes": false, "mentioning": false, "none": false, "phrase": false, "product": false, "replies": false, "retweets": false, "since": false, "tag": false, "to": false, "until": false, "words": true},
		"/v1/tweets/brief":            {"cursor": false, "tweetId": true},
		"/v1/tweets/by-ids":           {"tweetIds": true},
		"/v1/tweets/detail":           {"cursor": false, "tweetId": true},
		"/v1/tweets/favoriters":       {"authToken": false, "cursor": false, "tweetId": true},
		"/v1/tweets/quotes":           {"authToken": false, "cursor": false, "tweetId": true},
		"/v1/tweets/replies":          {"cursor": false, "userId": true},
		"/v1/tweets/retweeters":       {"authToken": false, "cursor": false, "tweetId": true},
		"/v1/tweets/timeline":         {"cursor": false, "userId": true},
		"/v1/users/account-analytics": {"authToken": true, "csrfToken": false, "restId": true},
		"/v1/users/articles-tweets":   {"authToken": false, "cursor": false, "userId": true},
		"/v1/users/by-id":             {"cursor": false, "userId": true},
		"/v1/users/by-ids":            {"userIds": true},
		"/v1/users/by-username":       {"screenName": true},
		"/v1/users/followers":         {"cursor": false, "userId": true},
		"/v1/users/followings":        {"cursor": false, "userId": true},
		"/v1/users/highlights":        {"authToken": false, "cursor": false, "userId": true},
		"/v1/users/likes":             {"authToken": false, "cursor": false, "userId": true},
		"/v1/users/mentions-timeline": {"authToken": true, "csrfToken": false, "includeEntities": false, "maxId": false, "sinceId": false, "trimUser": false},
		"/v1/users/username-changes":  {"screenName": true},
	}

	if len(paths) != len(expected) {
		t.Fatalf("expected %d swagger paths, got %d", len(expected), len(paths))
	}

	for path, expectedQuery := range expected {
		operation := assertOperation(t, paths, path, http.MethodGet)
		assertExactQueryParams(t, assertParameters(t, operation), expectedQuery)
	}
}

func TestGeneratedRoutesMatchSwaggerPaths(t *testing.T) {
	paths := loadGatewaySwaggerPaths(t, "dev")

	expected := make([]string, 0, len(paths))
	for path := range paths {
		expected = append(expected, path)
	}
	slices.Sort(expected)

	actual := extractRoutePathsFromSource(t, filepath.Join("internal", "handler", "routes.go"), regexp.MustCompile(`Path:\s+"(/v1/[^"]+)"`))

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("generated routes do not match swagger paths:\nactual=%#v\nexpected=%#v", actual, expected)
	}
}

func TestManualAdminCollectorRoutesExistSeparatelyFromSwaggerContract(t *testing.T) {
	actual := extractRoutePathsFromSource(t, filepath.Join("internal", "handler", "routes.go"), regexp.MustCompile(`Method:\s+http\.Method(?:Get|Post),[\s\S]*?Path:\s+"(/admin/v1/collector/tasks[^"]*)"`))
	expected := []string{
		"/admin/v1/collector/tasks",
		"/admin/v1/collector/tasks/:id",
		"/admin/v1/collector/tasks/:id/runs",
	}

	slices.Sort(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("manual admin routes mismatch:\nactual=%#v\nexpected=%#v", actual, expected)
	}
}

func loadGatewaySwaggerPaths(t *testing.T, mode string) map[string]any {
	t.Helper()

	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	addSwaggerRoutes(server, mode)

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	rec := httptest.NewRecorder()
	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode swagger doc: %v", err)
	}

	paths, ok := response["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object in swagger doc, got %#v", response["paths"])
	}

	return paths
}

func assertExactQueryParams(t *testing.T, parameters []any, expected map[string]bool) {
	t.Helper()

	actual := make(map[string]bool)
	for _, rawParameter := range parameters {
		parameter, ok := rawParameter.(map[string]any)
		if !ok || parameter["in"] != "query" {
			continue
		}

		name, _ := parameter["name"].(string)
		required, _ := parameter["required"].(bool)
		actual[name] = required
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected query params: actual=%#v expected=%#v", actual, expected)
	}
}

func extractRoutePathsFromSource(t *testing.T, relativePath string, pattern *regexp.Regexp) []string {
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
