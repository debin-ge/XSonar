package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListTenantAppsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListTenantAppsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListTenantAppsLogic {
	return &ListTenantAppsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListTenantAppsLogic) ListTenantApps(in *accesspb.ListTenantAppsRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.ListTenantApps(l.ctx, in)
}
