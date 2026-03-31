package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckIpBanLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckIpBanLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckIpBanLogic {
	return &CheckIpBanLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CheckIpBanLogic) CheckIpBan(in *accesspb.CheckIpBanRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.CheckIPBan(l.ctx, in)
}
