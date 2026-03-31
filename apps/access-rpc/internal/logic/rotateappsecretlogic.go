package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type RotateAppSecretLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRotateAppSecretLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RotateAppSecretLogic {
	return &RotateAppSecretLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RotateAppSecretLogic) RotateAppSecret(in *accesspb.RotateAppSecretRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.RotateAppSecret(l.ctx, in)
}
