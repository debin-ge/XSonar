package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type QueryUsageStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewQueryUsageStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *QueryUsageStatsLogic {
	return &QueryUsageStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *QueryUsageStatsLogic) QueryUsageStats(in *accesspb.QueryUsageStatsRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.QueryUsageStats(l.ctx, in)
}
