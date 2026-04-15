package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetAppAuthContextByIDLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetAppAuthContextByIDLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAppAuthContextByIDLogic {
	return &GetAppAuthContextByIDLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetAppAuthContextByIDLogic) GetAppAuthContextByID(in *accesspb.GetAppAuthContextByIDRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.GetAppAuthContextByID(l.ctx, in)
}
