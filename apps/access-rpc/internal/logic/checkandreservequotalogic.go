package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckAndReserveQuotaLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckAndReserveQuotaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckAndReserveQuotaLogic {
	return &CheckAndReserveQuotaLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CheckAndReserveQuotaLogic) CheckAndReserveQuota(in *accesspb.CheckAndReserveQuotaRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.CheckAndReserveQuota(l.ctx, in)
}
