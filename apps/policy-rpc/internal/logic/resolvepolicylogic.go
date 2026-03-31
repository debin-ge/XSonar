package logic

import (
	"context"

	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ResolvePolicyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewResolvePolicyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ResolvePolicyLogic {
	return &ResolvePolicyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ResolvePolicyLogic) ResolvePolicy(in *policypb.ResolvePolicyRequest) (*policypb.JsonResponse, error) {
	return l.svcCtx.Bridge.ResolvePolicy(l.ctx, in)
}
