package internal

import (
	"context"
	"encoding/json"

	"xsonar/pkg/model"
	"xsonar/pkg/proto/policypb"
	"xsonar/pkg/xlog"
)

type Bridge struct {
	svc *policyService
}

func NewBridge(logger *xlog.Logger) *Bridge {
	return &Bridge{
		svc: newPolicyService(logger),
	}
}

func (b *Bridge) Close(ctx context.Context) error {
	if b == nil || b.svc == nil {
		return nil
	}

	return b.svc.Shutdown(ctx)
}

func (b *Bridge) ResolvePolicy(ctx context.Context, in *policypb.ResolvePolicyRequest) (*policypb.JsonResponse, error) {
	return encodePolicyResponse(b.svc.resolvePolicy(ctx, resolvePolicyRequest{
		Path:   in.GetPath(),
		Method: in.GetMethod(),
	})), nil
}

func (b *Bridge) CheckAppPolicyAccess(ctx context.Context, in *policypb.CheckAppPolicyAccessRequest) (*policypb.JsonResponse, error) {
	return encodePolicyResponse(b.svc.checkAppPolicyAccess(ctx, checkAppPolicyAccessRequest{
		AppID:     in.GetAppId(),
		PolicyKey: in.GetPolicyKey(),
	})), nil
}

func (b *Bridge) ListPolicies(ctx context.Context, _ *policypb.ListPoliciesRequest) (*policypb.JsonResponse, error) {
	return encodePolicyResponse(b.svc.listPolicies(ctx)), nil
}

func (b *Bridge) PublishPolicyConfig(ctx context.Context, in *policypb.PublishPolicyConfigRequest) (*policypb.JsonResponse, error) {
	return encodePolicyResponse(b.svc.publishPolicyConfig(ctx, publishPolicyConfigRequest{
		PolicyKey:            in.GetPolicyKey(),
		DisplayName:          in.GetDisplayName(),
		PublicMethod:         in.GetPublicMethod(),
		PublicPath:           in.GetPublicPath(),
		UpstreamMethod:       in.GetUpstreamMethod(),
		UpstreamPath:         in.GetUpstreamPath(),
		AllowedParams:        in.GetAllowedParams(),
		DeniedParams:         in.GetDeniedParams(),
		DefaultParams:        in.GetDefaultParams(),
		ProviderCredentialID: in.GetProviderCredentialId(),
	})), nil
}

func (b *Bridge) BindAppPolicies(ctx context.Context, in *policypb.BindAppPoliciesRequest) (*policypb.JsonResponse, error) {
	return encodePolicyResponse(b.svc.bindAppPolicies(ctx, bindAppPoliciesRequest{
		AppID:      in.GetAppId(),
		PolicyKeys: in.GetPolicyKeys(),
	})), nil
}

func encodePolicyResponse(data any, svcErr *policyServiceError) *policypb.JsonResponse {
	if svcErr != nil {
		return &policypb.JsonResponse{
			Code:    int32(svcErr.code),
			Message: svcErr.message,
		}
	}

	return &policypb.JsonResponse{
		Code:     model.CodeOK,
		Message:  "ok",
		DataJson: mustMarshalPolicyBridgeData(data),
	}
}

func mustMarshalPolicyBridgeData(data any) string {
	if data == nil {
		return "null"
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return "null"
	}

	return string(payload)
}
