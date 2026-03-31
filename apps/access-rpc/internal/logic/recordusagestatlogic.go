package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecordUsageStatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRecordUsageStatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecordUsageStatLogic {
	return &RecordUsageStatLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RecordUsageStatLogic) RecordUsageStat(in *accesspb.RecordUsageStatRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.RecordUsageStat(l.ctx, in)
}
