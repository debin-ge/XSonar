package logic

import (
	"context"

	"xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/proto/accesspb"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReleaseQuotaOnFailureLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReleaseQuotaOnFailureLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReleaseQuotaOnFailureLogic {
	return &ReleaseQuotaOnFailureLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReleaseQuotaOnFailureLogic) ReleaseQuotaOnFailure(in *accesspb.ReleaseQuotaOnFailureRequest) (*accesspb.JsonResponse, error) {
	return l.svcCtx.Bridge.ReleaseQuotaOnFailure(l.ctx, in)
}
