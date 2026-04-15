package internal

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"xsonar/apps/console-api/internal/types"
)

const (
	maxTenantNameLen           = 32
	maxAppNameLen              = 64
	maxUsernameLen             = 64
	maxPasswordLen             = 128
	maxDailyQuota              = int64(1_000_000_000)
	maxQPSLimit                = 1_000_000
	maxPolicyKeyLen            = 64
	maxDisplayNameLen          = 64
	maxPathLen                 = 256
	maxParamNameLen            = 64
	maxParamValueLen           = 256
	maxPolicyBindingCount      = 64
	maxPolicyParamCount        = 64
	maxDefaultParamCount       = 64
	maxProviderCredentialIDLen = 64
	maxGatewayTokenTTLSeconds  = int64((1<<63 - 1) / int64(time.Second))
)

func validateLoginReq(req *types.LoginReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.Username = strings.TrimSpace(req.Username)
	if err := requireStringInRange("username", req.Username, 1, maxUsernameLen); err != nil {
		return err
	}
	if err := requireStringInRange("password", req.Password, 1, maxPasswordLen); err != nil {
		return err
	}
	return nil
}

func validateCreateTenantReq(req *types.CreateTenantReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.Name = strings.TrimSpace(req.Name)
	return requireStringInRange("name", req.Name, 1, maxTenantNameLen)
}

func validateCreateTenantAppReq(req *types.CreateTenantAppReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.Name = strings.TrimSpace(req.Name)
	if err := requireStringInRange("name", req.Name, 1, maxAppNameLen); err != nil {
		return err
	}
	if req.DailyQuota < 0 || req.DailyQuota > maxDailyQuota {
		return fmt.Errorf("daily_quota must be between 0 and %d", maxDailyQuota)
	}
	if req.QpsLimit < 0 || req.QpsLimit > maxQPSLimit {
		return fmt.Errorf("qps_limit must be between 0 and %d", maxQPSLimit)
	}
	return nil
}

func validateIssueGatewayTokenReq(req *types.IssueGatewayTokenReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.AppID = strings.TrimSpace(req.AppID)
	if err := requireStringInRange("tenant_id", req.TenantID, 1, maxTenantNameLen*2); err != nil {
		return err
	}
	if err := requireStringInRange("app_id", req.AppID, 1, maxAppNameLen*2); err != nil {
		return err
	}
	if req.TTL < 0 || req.TTL > maxGatewayTokenTTLSeconds {
		return fmt.Errorf("ttl must be between 0 and %d", maxGatewayTokenTTLSeconds)
	}
	return nil
}

func validateUpdateAppStatusReq(req *types.UpdateAppStatusReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.Status = strings.ToLower(strings.TrimSpace(req.Status))
	if req.Status != "active" && req.Status != "disabled" {
		return errors.New("status must be active or disabled")
	}
	return nil
}

func validateUpdateAppQuotaReq(req *types.UpdateAppQuotaReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	if req.DailyQuota < 0 || req.DailyQuota > maxDailyQuota {
		return fmt.Errorf("daily_quota must be between 0 and %d", maxDailyQuota)
	}
	if req.QpsLimit < 0 || req.QpsLimit > maxQPSLimit {
		return fmt.Errorf("qps_limit must be between 0 and %d", maxQPSLimit)
	}
	return nil
}

func validatePublishPolicyConfigReq(req *types.PublishPolicyConfigReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	req.PolicyKey = strings.TrimSpace(req.PolicyKey)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	req.PublicMethod = strings.ToUpper(strings.TrimSpace(req.PublicMethod))
	req.PublicPath = strings.TrimSpace(req.PublicPath)
	req.UpstreamMethod = strings.ToUpper(strings.TrimSpace(req.UpstreamMethod))
	req.UpstreamPath = strings.TrimSpace(req.UpstreamPath)
	req.ProviderCredentialID = strings.TrimSpace(req.ProviderCredentialID)

	if err := requireStringInRange("policy_key", req.PolicyKey, 1, maxPolicyKeyLen); err != nil {
		return err
	}
	if err := requireStringInRange("display_name", req.DisplayName, 1, maxDisplayNameLen); err != nil {
		return err
	}
	if err := validateHTTPMethod("public_method", req.PublicMethod); err != nil {
		return err
	}
	if err := validateHTTPMethod("upstream_method", req.UpstreamMethod); err != nil {
		return err
	}
	if err := validatePath("public_path", req.PublicPath); err != nil {
		return err
	}
	if err := validatePath("upstream_path", req.UpstreamPath); err != nil {
		return err
	}
	if err := requireStringInRange("provider_credential_id", req.ProviderCredentialID, 1, maxProviderCredentialIDLen); err != nil {
		return err
	}
	if err := validateStringList("allowed_params", req.AllowedParams, maxPolicyParamCount); err != nil {
		return err
	}
	if err := validateStringList("denied_params", req.DeniedParams, maxPolicyParamCount); err != nil {
		return err
	}
	if len(req.DefaultParams) > maxDefaultParamCount {
		return fmt.Errorf("default_params supports at most %d entries", maxDefaultParamCount)
	}
	for key, value := range req.DefaultParams {
		trimmedKey := strings.TrimSpace(key)
		if err := requireStringInRange("default_params key", trimmedKey, 1, maxParamNameLen); err != nil {
			return err
		}
		if err := requireStringInRange("default_params value", value, 0, maxParamValueLen); err != nil {
			return err
		}
		if trimmedKey != key {
			delete(req.DefaultParams, key)
			req.DefaultParams[trimmedKey] = value
		}
	}
	return nil
}

func validateBindAppPoliciesReq(req *types.BindAppPoliciesReq) error {
	if req == nil {
		return errors.New("request body is required")
	}
	if len(req.PolicyKeys) == 0 {
		return errors.New("policy_keys is required")
	}
	if len(req.PolicyKeys) > maxPolicyBindingCount {
		return fmt.Errorf("policy_keys supports at most %d items", maxPolicyBindingCount)
	}
	for index, key := range req.PolicyKeys {
		trimmed := strings.TrimSpace(key)
		if err := requireStringInRange("policy_keys", trimmed, 1, maxPolicyKeyLen); err != nil {
			return err
		}
		req.PolicyKeys[index] = trimmed
	}
	return nil
}

func requireStringInRange(name, value string, minLen, maxLen int) error {
	length := len(value)
	if length < minLen {
		return fmt.Errorf("%s is required", name)
	}
	if length > maxLen {
		return fmt.Errorf("%s must be at most %d characters", name, maxLen)
	}
	return nil
}

func validateHTTPMethod(name, value string) error {
	switch value {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
		return nil
	default:
		return fmt.Errorf("%s must be one of GET, POST, PUT, PATCH, DELETE", name)
	}
}

func validatePath(name, value string) error {
	if err := requireStringInRange(name, value, 1, maxPathLen); err != nil {
		return err
	}
	if !strings.HasPrefix(value, "/") {
		return fmt.Errorf("%s must start with /", name)
	}
	return nil
}

func validateStringList(name string, values []string, maxItems int) error {
	if len(values) > maxItems {
		return fmt.Errorf("%s supports at most %d items", name, maxItems)
	}
	for index, value := range values {
		trimmed := strings.TrimSpace(value)
		if err := requireStringInRange(name, trimmed, 1, maxParamNameLen); err != nil {
			return err
		}
		values[index] = trimmed
	}
	return nil
}
