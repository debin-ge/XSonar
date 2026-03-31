package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetAppAuthContextLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetAppAuthContextLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAppAuthContextLogic {
	return &GetAppAuthContextLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetAppAuthContextLogic) GetAppAuthContext(in *accesspb.GetAppAuthContextRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.GetAppAuthContext(l.ctx, in)
}
