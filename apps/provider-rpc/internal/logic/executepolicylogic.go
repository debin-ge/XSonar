package logic

import (
	"context"

	"xsonar/apps/provider-rpc/internal/svc"
	"xsonar/pkg/proto/providerpb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ExecutePolicyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewExecutePolicyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ExecutePolicyLogic {
	return &ExecutePolicyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ExecutePolicyLogic) ExecutePolicy(in *providerpb.ExecutePolicyRequest) (*providerpb.JsonResponse, error) {
	return l.svcCtx.Bridge.ExecutePolicy(l.ctx, in)
}
