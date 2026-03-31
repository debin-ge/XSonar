// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"xsonar/apps/gateway-api/internal/svc"
	"xsonar/apps/gateway-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type SearchTrendsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSearchTrendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SearchTrendsLogic {
	return &SearchTrendsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SearchTrendsLogic) SearchTrends() (resp *types.Envelope, err error) {
	// todo: add your logic here and delete this line

	return
}
