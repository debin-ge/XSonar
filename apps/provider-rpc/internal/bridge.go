package internal

import (
	"context"
	"encoding/json"
	"net/http"

	providerconfig "xsonar/apps/provider-rpc/internal/config"
	"xsonar/pkg/model"
	"xsonar/pkg/proto/providerpb"
	"xsonar/pkg/xlog"
)

type Bridge struct {
	svc *providerService
}

func NewBridge(cfg providerconfig.ProviderConfig, client *http.Client, logger *xlog.Logger) *Bridge {
	return &Bridge{
		svc: newProviderServiceWithConfigAndClient(cfg, client, logger),
	}
}

func (b *Bridge) ExecutePolicy(ctx context.Context, in *providerpb.ExecutePolicyRequest) (*providerpb.JsonResponse, error) {
	var query map[string]any
	if raw := in.GetQueryJson(); raw != "" {
		if err := json.Unmarshal([]byte(raw), &query); err != nil {
			return encodeProviderResponse(nil, providerInvalidRequest("invalid query_json payload")), nil
		}
	}

	return encodeProviderResponse(b.svc.executePolicy(ctx, executePolicyRequest{
		RequestID:      in.GetRequestId(),
		PolicyKey:      in.GetPolicyKey(),
		UpstreamMethod: in.GetUpstreamMethod(),
		UpstreamPath:   in.GetUpstreamPath(),
		Query:          query,
		ProviderName:   in.GetProviderName(),
		ProviderAPIKey: in.GetProviderApiKey(),
	})), nil
}

func (b *Bridge) HealthCheckProvider(ctx context.Context, in *providerpb.HealthCheckProviderRequest) (*providerpb.JsonResponse, error) {
	return encodeProviderResponse(b.svc.healthCheckProvider(ctx, healthCheckProviderRequest{
		ProviderName: in.GetProviderName(),
	})), nil
}

func encodeProviderResponse(data any, svcErr *providerServiceError) *providerpb.JsonResponse {
	if svcErr != nil {
		return &providerpb.JsonResponse{
			Code:    int32(svcErr.code),
			Message: svcErr.message,
		}
	}

	return &providerpb.JsonResponse{
		Code:     model.CodeOK,
		Message:  "ok",
		DataJson: mustMarshalProviderBridgeData(data),
	}
}

func mustMarshalProviderBridgeData(data any) string {
	if data == nil {
		return "null"
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return "null"
	}

	return string(payload)
}
