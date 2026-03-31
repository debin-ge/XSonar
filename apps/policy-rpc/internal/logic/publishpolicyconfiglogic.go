package logic

import (
	"context"

	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"

	"github.com/zeromicro/go-zero/core/logx"
)

type PublishPolicyConfigLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPublishPolicyConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishPolicyConfigLogic {
	return &PublishPolicyConfigLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *PublishPolicyConfigLogic) PublishPolicyConfig(in *policypb.PublishPolicyConfigRequest) (*policypb.JsonResponse, error) {
	return l.svcCtx.Bridge.PublishPolicyConfig(l.ctx, in)
}
