package logic

import (
	"context"

	"xsonar/apps/provider-rpc/internal/svc"
	"xsonar/pkg/proto/providerpb"

	"github.com/zeromicro/go-zero/core/logx"
)

type HealthCheckProviderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHealthCheckProviderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HealthCheckProviderLogic {
	return &HealthCheckProviderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HealthCheckProviderLogic) HealthCheckProvider(in *providerpb.HealthCheckProviderRequest) (*providerpb.JsonResponse, error) {
	return l.svcCtx.Bridge.HealthCheckProvider(l.ctx, in)
}
