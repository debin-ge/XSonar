// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"xsonar/apps/console-api/internal/svc"
	"xsonar/apps/console-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type PublishPolicyConfigLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPublishPolicyConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishPolicyConfigLogic {
	return &PublishPolicyConfigLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *PublishPolicyConfigLogic) PublishPolicyConfig(req *types.PublishPolicyConfigReq) (resp *types.Envelope, err error) {
	// todo: add your logic here and delete this line

	return
}
