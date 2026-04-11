package server

import (
	"context"

	schedulerinternal "xsonar/apps/scheduler-rpc/internal"
	"xsonar/apps/scheduler-rpc/internal/svc"
	"xsonar/pkg/model"
	"xsonar/pkg/proto/schedulerpb"
)

type SchedulerServiceServer struct {
	svcCtx *svc.ServiceContext
	schedulerpb.UnimplementedSchedulerServiceServer
}

func NewSchedulerServiceServer(svcCtx *svc.ServiceContext) *SchedulerServiceServer {
	return &SchedulerServiceServer{svcCtx: svcCtx}
}

func (s *SchedulerServiceServer) CreateTask(ctx context.Context, in *schedulerpb.CreateTaskRequest) (*schedulerpb.JsonResponse, error) {
	if s.svcCtx == nil || s.svcCtx.Service == nil {
		return &schedulerpb.JsonResponse{Code: model.CodeInternalError, Message: "scheduler service unavailable"}, nil
	}

	data, svcErr := s.svcCtx.Service.CreateTask(ctx, internalCreateTaskRequest(in))
	return schedulerinternal.EncodeSchedulerResponse(data, svcErr), nil
}

func (s *SchedulerServiceServer) GetTask(ctx context.Context, in *schedulerpb.GetTaskRequest) (*schedulerpb.JsonResponse, error) {
	if s.svcCtx == nil || s.svcCtx.Service == nil {
		return &schedulerpb.JsonResponse{Code: model.CodeInternalError, Message: "scheduler service unavailable"}, nil
	}

	data, svcErr := s.svcCtx.Service.GetTask(ctx, internalGetTaskRequest(in))
	return schedulerinternal.EncodeSchedulerResponse(data, svcErr), nil
}

func (s *SchedulerServiceServer) ListTaskRuns(ctx context.Context, in *schedulerpb.ListTaskRunsRequest) (*schedulerpb.JsonResponse, error) {
	if s.svcCtx == nil || s.svcCtx.Service == nil {
		return &schedulerpb.JsonResponse{Code: model.CodeInternalError, Message: "scheduler service unavailable"}, nil
	}

	data, svcErr := s.svcCtx.Service.ListTaskRuns(ctx, internalListTaskRunsRequest(in))
	return schedulerinternal.EncodeSchedulerResponse(data, svcErr), nil
}

func internalCreateTaskRequest(in *schedulerpb.CreateTaskRequest) schedulerinternal.CreateTaskRequest {
	if in == nil {
		return schedulerinternal.CreateTaskRequest{}
	}

	return schedulerinternal.CreateTaskRequest{
		TaskID:           in.GetTaskId(),
		TaskType:         in.GetTaskType(),
		Keyword:          in.GetKeyword(),
		Priority:         in.GetPriority(),
		FrequencySeconds: in.FrequencySeconds,
		Since:            in.GetSince(),
		Until:            in.GetUntil(),
		RequiredCount:    in.RequiredCount,
	}
}

func internalGetTaskRequest(in *schedulerpb.GetTaskRequest) schedulerinternal.GetTaskRequest {
	if in == nil {
		return schedulerinternal.GetTaskRequest{}
	}

	return schedulerinternal.GetTaskRequest{TaskID: in.GetTaskId()}
}

func internalListTaskRunsRequest(in *schedulerpb.ListTaskRunsRequest) schedulerinternal.ListTaskRunsRequest {
	if in == nil {
		return schedulerinternal.ListTaskRunsRequest{}
	}

	return schedulerinternal.ListTaskRunsRequest{TaskID: in.GetTaskId()}
}
