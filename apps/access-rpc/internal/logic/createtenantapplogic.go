package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateTenantAppLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateTenantAppLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateTenantAppLogic {
	return &CreateTenantAppLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateTenantAppLogic) CreateTenantApp(in *accesspb.CreateTenantAppRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.CreateTenantApp(l.ctx, in)
}
