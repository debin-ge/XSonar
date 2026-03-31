// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"xsonar/apps/console-api/internal/svc"
	"xsonar/apps/console-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateAppQuotaLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateAppQuotaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateAppQuotaLogic {
	return &UpdateAppQuotaLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateAppQuotaLogic) UpdateAppQuota(req *types.UpdateAppQuotaReq) (resp *types.Envelope, err error) {
	// todo: add your logic here and delete this line

	return
}
