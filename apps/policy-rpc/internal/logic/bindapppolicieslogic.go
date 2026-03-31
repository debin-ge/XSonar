package logic

import (
	"context"

	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"

	"github.com/zeromicro/go-zero/core/logx"
)

type BindAppPoliciesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBindAppPoliciesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BindAppPoliciesLogic {
	return &BindAppPoliciesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BindAppPoliciesLogic) BindAppPolicies(in *policypb.BindAppPoliciesRequest) (*policypb.JsonResponse, error) {
	return l.svcCtx.Bridge.BindAppPolicies(l.ctx, in)
}
