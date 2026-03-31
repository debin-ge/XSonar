package internal

import (
	"context"
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
