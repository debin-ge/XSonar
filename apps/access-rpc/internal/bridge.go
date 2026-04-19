package internal

import (
	"context"
	"encoding/json"

	"xsonar/pkg/model"
	"xsonar/pkg/proto/accesspb"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type Bridge struct {
	svc *service
}

func NewBridge(logger *xlog.Logger) *Bridge {
	return &Bridge{
		svc: newService(logger),
	}
}

func (b *Bridge) Close(ctx context.Context) error {
	if b == nil || b.svc == nil {
		return nil
	}

	return b.svc.Shutdown(ctx)
}

func (b *Bridge) GetAppAuthContextByID(ctx context.Context, in *accesspb.GetAppAuthContextByIDRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.getAppAuthContextByID(ctx, getAppAuthContextByIDRequest{
		AppID: in.GetAppId(),
	})), nil
}

func (b *Bridge) CheckReplay(ctx context.Context, in *accesspb.CheckReplayRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.checkReplay(ctx, checkReplayRequest{
		AppID:     in.GetAppId(),
		Nonce:     in.GetNonce(),
		Timestamp: in.GetTimestamp(),
	})), nil
}

func (b *Bridge) CheckAndReserveQuota(ctx context.Context, in *accesspb.CheckAndReserveQuotaRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.checkAndReserveQuota(ctx, checkAndReserveQuotaRequest{
		AppID:     in.GetAppId(),
		PolicyKey: in.GetPolicyKey(),
		RequestID: in.GetRequestId(),
	})), nil
}

func (b *Bridge) ReleaseQuotaOnFailure(ctx context.Context, in *accesspb.ReleaseQuotaOnFailureRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.releaseQuotaOnFailure(ctx, releaseQuotaOnFailureRequest{
		AppID:     in.GetAppId(),
		RequestID: in.GetRequestId(),
	})), nil
}

func (b *Bridge) RecordUsageStat(ctx context.Context, in *accesspb.RecordUsageStatRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.recordUsageStat(ctx, recordUsageStatRequest{
		TenantID:           in.GetTenantId(),
		AppID:              in.GetAppId(),
		PolicyKey:          in.GetPolicyKey(),
		RequestID:          in.GetRequestId(),
		Success:            in.GetSuccess(),
		DurationMS:         in.GetDurationMs(),
		UpstreamDurationMS: in.GetUpstreamDurationMs(),
		StatusCode:         int(in.GetStatusCode()),
		ResultCode:         in.GetResultCode(),
	})), nil
}

func (b *Bridge) QueryUsageStats(ctx context.Context, in *accesspb.QueryUsageStatsRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.queryUsageStats(ctx, queryUsageStatsRequest{
		TenantID:  in.GetTenantId(),
		AppID:     in.GetAppId(),
		PolicyKey: in.GetPolicyKey(),
		StartUnix: in.GetStartUnix(),
		EndUnix:   in.GetEndUnix(),
	})), nil
}

func (b *Bridge) AuthenticateConsoleUser(ctx context.Context, in *accesspb.AuthenticateConsoleUserRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.authenticateConsoleUser(ctx, authenticateConsoleUserRequest{
		Username: in.GetUsername(),
		Password: in.GetPassword(),
	})), nil
}

func (b *Bridge) CreateTenant(ctx context.Context, in *accesspb.CreateTenantRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.createTenant(ctx, createTenantRequest{
		Name: in.GetName(),
	})), nil
}

func (b *Bridge) ListTenants(ctx context.Context, _ *accesspb.ListTenantsRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.listTenants(ctx)), nil
}

func (b *Bridge) CreateTenantApp(ctx context.Context, in *accesspb.CreateTenantAppRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.createTenantApp(ctx, createTenantAppRequest{
		TenantID:   in.GetTenantId(),
		Name:       in.GetName(),
		DailyQuota: in.GetDailyQuota(),
		QPSLimit:   int(in.GetQpsLimit()),
	})), nil
}

func (b *Bridge) ListTenantApps(ctx context.Context, in *accesspb.ListTenantAppsRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.listTenantApps(ctx, in.GetTenantId())), nil
}

func (b *Bridge) UpdateTenantAppStatus(ctx context.Context, in *accesspb.UpdateTenantAppStatusRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.updateTenantAppStatus(ctx, updateTenantAppStatusRequest{
		AppID:  in.GetAppId(),
		Status: in.GetStatus(),
	})), nil
}

func (b *Bridge) UpdateAppQuota(ctx context.Context, in *accesspb.UpdateAppQuotaRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.updateAppQuota(ctx, updateAppQuotaRequest{
		AppID:      in.GetAppId(),
		DailyQuota: in.GetDailyQuota(),
		QPSLimit:   int(in.GetQpsLimit()),
	})), nil
}

func (b *Bridge) CheckIPBan(ctx context.Context, in *accesspb.CheckIpBanRequest) (*accesspb.JsonResponse, error) {
	return encodeAccessResponse(b.svc.checkIPBan(ctx, checkIPBanRequest{
		IP: in.GetIp(),
	})), nil
}

func encodeAccessResponse(data any, svcErr *serviceError) *accesspb.JsonResponse {
	if svcErr != nil {
		code, err := shared.Int32FromInt(svcErr.code)
		if err != nil {
			code = model.CodeInternalError
		}
		return &accesspb.JsonResponse{
			Code:    code,
			Message: svcErr.message,
		}
	}

	return &accesspb.JsonResponse{
		Code:     model.CodeOK,
		Message:  "ok",
		DataJson: mustMarshalBridgeData(data),
	}
}

func mustMarshalBridgeData(data any) string {
	if data == nil {
		return "null"
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return "null"
	}

	return string(payload)
}
