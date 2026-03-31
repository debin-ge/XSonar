// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"xsonar/apps/console-api/internal/svc"
	"xsonar/apps/console-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type UsageReportLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUsageReportLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UsageReportLogic {
	return &UsageReportLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UsageReportLogic) UsageReport() (resp *types.Envelope, err error) {
	// todo: add your logic here and delete this line

	return
}
