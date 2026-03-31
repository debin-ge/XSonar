package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateAppQuotaLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateAppQuotaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateAppQuotaLogic {
	return &UpdateAppQuotaLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateAppQuotaLogic) UpdateAppQuota(in *accesspb.UpdateAppQuotaRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.UpdateAppQuota(l.ctx, in)
}
