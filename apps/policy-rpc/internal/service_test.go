package internal

import (
	"context"
	"slices"
	"testing"

	"xsonar/pkg/xlog"
)

func TestPolicyServiceSeedAndBinding(t *testing.T) {
	t.Setenv("POLICY_RPC_PROVIDER_API_KEY", "test-provider-key")

	svc := newPolicyService(xlog.NewStdout("policy-rpc-test"))
	ctx := context.Background()

	resolved, resolveErr := svc.resolvePolicy(ctx, resolvePolicyRequest{
		Path:   "/v1/users/by-ids",
		Method: "GET",
	})
	if resolveErr != nil {
		t.Fatalf("resolvePolicy returned error: %v", resolveErr)
	}

	resolvedMap := resolved.(map[string]any)
	if resolvedMap["upstream_path"] != "/base/apitools/usersByIdRestIds" {
		t.Fatalf("unexpected upstream path: %#v", resolvedMap["upstream_path"])
	}
	requiredParams := resolvedMap["required_params"].([]string)
	if len(requiredParams) != 1 || requiredParams[0] != "userIds" {
		t.Fatalf("unexpected required params: %#v", resolvedMap["required_params"])
	}

	if _, bindErr := svc.bindAppPolicies(ctx, bindAppPoliciesRequest{
		AppID:      "app_123",
		PolicyKeys: []string{"users_by_ids_v1"},
	}); bindErr != nil {
		t.Fatalf("bindAppPolicies returned error: %v", bindErr)
	}

	access, accessErr := svc.checkAppPolicyAccess(ctx, checkAppPolicyAccessRequest{
		AppID:     "app_123",
		PolicyKey: "users_by_ids_v1",
	})
	if accessErr != nil {
		t.Fatalf("checkAppPolicyAccess returned error: %v", accessErr)
	}
	if !access.(map[string]any)["allowed"].(bool) {
		t.Fatal("expected bound policy to be allowed")
	}
}

func TestDefaultPoliciesIncludePublicReadonlyProviderRoutes(t *testing.T) {
	policies := defaultPolicies("provider_credential_fapi_uk")
	byKey := make(map[string]policyDefinition, len(policies))
	for _, policy := range policies {
		byKey[policy.PolicyKey] = policy
	}

	cases := []struct {
		key          string
		publicPath   string
		upstreamPath string
		allowed      []string
		required     []string
	}{
		{"tweets_brief_v1", "/v1/tweets/brief", "/base/apitools/tweetSimple", []string{"tweetId", "cursor"}, []string{"tweetId"}},
		{"tweets_quotes_v1", "/v1/tweets/quotes", "/base/apitools/quotesV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}},
		{"tweets_retweeters_v1", "/v1/tweets/retweeters", "/base/apitools/retweetersV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}},
		{"tweets_favoriters_v1", "/v1/tweets/favoriters", "/base/apitools/favoritersV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}},
		{"users_mentions_timeline_v1", "/v1/users/mentions-timeline", "/base/apitools/mentionsTimeline", []string{"authToken", "csrfToken", "sinceId", "maxId", "includeEntities", "trimUser"}, []string{"authToken"}},
		{"search_box_v1", "/v1/search/box", "/base/apitools/searchBox", []string{"words", "searchType"}, []string{"words"}},
		{"search_explore_v1", "/v1/search/explore", "/base/apitools/explore", []string{}, nil},
		{"search_news_v1", "/v1/search/news", "/base/apitools/news", []string{}, nil},
		{"search_sports_v1", "/v1/search/sports", "/base/apitools/sports", []string{}, nil},
		{"search_entertainment_v1", "/v1/search/entertainment", "/base/apitools/entertainment", []string{}, nil},
		{"users_username_changes_v1", "/v1/users/username-changes", "/base/apitools/usernameChanges", []string{"screenName"}, []string{"screenName"}},
		{"users_likes_v1", "/v1/users/likes", "/base/apitools/userLikeV2", []string{"userId", "cursor", "authToken"}, []string{"userId"}},
		{"users_highlights_v1", "/v1/users/highlights", "/base/apitools/highlightsV2", []string{"userId", "cursor", "authToken"}, []string{"userId"}},
		{"users_articles_tweets_v1", "/v1/users/articles-tweets", "/base/apitools/UserArticlesTweets", []string{"userId", "cursor", "authToken"}, []string{"userId"}},
		{"users_account_analytics_v1", "/v1/users/account-analytics", "/base/apitools/accountAnalytics", []string{"restId", "authToken", "csrfToken"}, []string{"restId", "authToken"}},
		{"users_followers_v1", "/v1/users/followers", "/base/apitools/followersListV2", []string{"userId", "cursor"}, []string{"userId"}},
		{"users_followings_v1", "/v1/users/followings", "/base/apitools/followingsListV2", []string{"userId", "cursor"}, []string{"userId"}},
		{"lists_v1", "/v1/lists", "/base/apitools/listByUserIdOrScreenName", []string{"userId", "screenName"}, nil},
		{"communities_v1", "/v1/communities", "/base/apitools/getCommunitiesByScreenName", []string{"screenName"}, []string{"screenName"}},
	}

	for _, tc := range cases {
		policy, ok := byKey[tc.key]
		if !ok {
			t.Fatalf("expected policy %q to be seeded", tc.key)
		}
		if policy.PublicPath != tc.publicPath {
			t.Fatalf("policy %q public path = %q, want %q", tc.key, policy.PublicPath, tc.publicPath)
		}
		if policy.UpstreamPath != tc.upstreamPath {
			t.Fatalf("policy %q upstream path = %q, want %q", tc.key, policy.UpstreamPath, tc.upstreamPath)
		}
		if !slices.Equal(policy.AllowedParams, tc.allowed) {
			t.Fatalf("policy %q allowed params = %#v, want %#v", tc.key, policy.AllowedParams, tc.allowed)
		}
		if !slices.Equal(policy.RequiredParams, tc.required) {
			t.Fatalf("policy %q required params = %#v, want %#v", tc.key, policy.RequiredParams, tc.required)
		}
		if policy.DefaultParams["resFormat"] != "json" {
			t.Fatalf("policy %q should inject resFormat=json, got %#v", tc.key, policy.DefaultParams)
		}
	}
}

func TestPublishPolicyConfigIncrementsVersion(t *testing.T) {
	t.Setenv("POLICY_RPC_PROVIDER_API_KEY", "test-provider-key")

	svc := newPolicyService(xlog.NewStdout("policy-rpc-test"))
	ctx := context.Background()

	first, firstErr := svc.publishPolicyConfig(ctx, publishPolicyConfigRequest{
		PolicyKey:            "custom_policy",
		DisplayName:          "Custom Policy",
		PublicMethod:         "GET",
		PublicPath:           "/v1/custom",
		UpstreamMethod:       "GET",
		UpstreamPath:         "/base/custom",
		AllowedParams:        []string{"foo"},
		RequiredParams:       []string{"foo"},
		DeniedParams:         []string{"proxyUrl"},
		DefaultParams:        map[string]string{"resFormat": "json"},
		ProviderCredentialID: "provider_credential_fapi_uk",
	})
	if firstErr != nil {
		t.Fatalf("first publishPolicyConfig returned error: %v", firstErr)
	}
	if first.(*policyDefinition).Version != 1 {
		t.Fatalf("expected version 1, got %d", first.(*policyDefinition).Version)
	}
	if len(first.(*policyDefinition).RequiredParams) != 1 || first.(*policyDefinition).RequiredParams[0] != "foo" {
		t.Fatalf("unexpected required params: %#v", first.(*policyDefinition).RequiredParams)
	}

	second, secondErr := svc.publishPolicyConfig(ctx, publishPolicyConfigRequest{
		PolicyKey:            "custom_policy",
		DisplayName:          "Custom Policy v2",
		PublicMethod:         "GET",
		PublicPath:           "/v1/custom-v2",
		UpstreamMethod:       "GET",
		UpstreamPath:         "/base/custom-v2",
		AllowedParams:        []string{"foo", "bar"},
		RequiredParams:       []string{"foo", "bar"},
		DeniedParams:         []string{"proxyUrl"},
		DefaultParams:        map[string]string{"resFormat": "json"},
		ProviderCredentialID: "provider_credential_fapi_uk",
	})
	if secondErr != nil {
		t.Fatalf("second publishPolicyConfig returned error: %v", secondErr)
	}
	if second.(*policyDefinition).Version != 2 {
		t.Fatalf("expected version 2, got %d", second.(*policyDefinition).Version)
	}
	if len(second.(*policyDefinition).RequiredParams) != 2 {
		t.Fatalf("unexpected required params: %#v", second.(*policyDefinition).RequiredParams)
	}
}
