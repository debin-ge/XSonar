package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckReplayLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckReplayLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckReplayLogic {
	return &CheckReplayLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CheckReplayLogic) CheckReplay(in *accesspb.CheckReplayRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.CheckReplay(l.ctx, in)
}
