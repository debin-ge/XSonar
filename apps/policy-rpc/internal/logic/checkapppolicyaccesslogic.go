package logic

import (
	"context"

	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckAppPolicyAccessLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckAppPolicyAccessLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckAppPolicyAccessLogic {
	return &CheckAppPolicyAccessLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CheckAppPolicyAccessLogic) CheckAppPolicyAccess(in *policypb.CheckAppPolicyAccessRequest) (*policypb.JsonResponse, error) {
	return l.svcCtx.Bridge.CheckAppPolicyAccess(l.ctx, in)
}
