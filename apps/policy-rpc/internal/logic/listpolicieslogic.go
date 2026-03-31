package logic

import (
	"context"

	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListPoliciesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListPoliciesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListPoliciesLogic {
	return &ListPoliciesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListPoliciesLogic) ListPolicies(in *policypb.ListPoliciesRequest) (*policypb.JsonResponse, error) {
	return l.svcCtx.Bridge.ListPolicies(l.ctx, in)
}
