// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"xsonar/apps/console-api/internal/svc"
	"xsonar/apps/console-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type RotateAppSecretLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRotateAppSecretLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RotateAppSecretLogic {
	return &RotateAppSecretLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RotateAppSecretLogic) RotateAppSecret() (resp *types.Envelope, err error) {
	// todo: add your logic here and delete this line

	return
}
