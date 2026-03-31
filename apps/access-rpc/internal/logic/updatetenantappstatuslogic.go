package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateTenantAppStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateTenantAppStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateTenantAppStatusLogic {
	return &UpdateTenantAppStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateTenantAppStatusLogic) UpdateTenantAppStatus(in *accesspb.UpdateTenantAppStatusRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.UpdateTenantAppStatus(l.ctx, in)
}
