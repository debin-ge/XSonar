package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListTenantsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListTenantsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListTenantsLogic {
	return &ListTenantsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListTenantsLogic) ListTenants(in *accesspb.ListTenantsRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.ListTenants(l.ctx, in)
}
