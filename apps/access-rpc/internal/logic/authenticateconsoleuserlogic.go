package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type AuthenticateConsoleUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAuthenticateConsoleUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AuthenticateConsoleUserLogic {
	return &AuthenticateConsoleUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *AuthenticateConsoleUserLogic) AuthenticateConsoleUser(in *accesspb.AuthenticateConsoleUserRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.AuthenticateConsoleUser(l.ctx, in)
}
