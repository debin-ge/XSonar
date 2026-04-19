package server

import (
	"context"

	collectorworkerinternal "xsonar/apps/collector-worker-rpc/internal"
	"xsonar/apps/collector-worker-rpc/internal/svc"
	"xsonar/pkg/model"
	"xsonar/pkg/proto/collectorworkerpb"
)

type CollectorWorkerServiceServer struct {
	svcCtx *svc.ServiceContext
	collectorworkerpb.UnimplementedCollectorWorkerServiceServer
}

func NewCollectorWorkerServiceServer(svcCtx *svc.ServiceContext) *CollectorWorkerServiceServer {
	return &CollectorWorkerServiceServer{svcCtx: svcCtx}
}

func (s *CollectorWorkerServiceServer) GetWorkerState(ctx context.Context, in *collectorworkerpb.GetWorkerStateRequest) (*collectorworkerpb.JsonResponse, error) {
	if s.svcCtx == nil || s.svcCtx.Service == nil {
		return &collectorworkerpb.JsonResponse{Code: model.CodeInternalError, Message: "collector worker service unavailable"}, nil
	}

	data, svcErr := s.svcCtx.Service.GetWorkerState(ctx, collectorworkerinternal.GetWorkerStateRequest{
		WorkerID: in.GetWorkerId(),
	})
	return collectorworkerinternal.EncodeCollectorWorkerResponse(data, svcErr), nil
}
