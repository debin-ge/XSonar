package clients

import (
	"context"
	"encoding/json"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/collector-worker-rpc/collectorworkerservice"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/apps/scheduler-rpc/schedulerservice"
	"xsonar/pkg/model"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type AccessRPC interface {
	Health(ctx context.Context) (*EnvelopeResponse, error)
	GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*EnvelopeResponse, error)
	CheckReplay(ctx context.Context, req *accessservice.CheckReplayRequest) (*EnvelopeResponse, error)
	CheckAndReserveQuota(ctx context.Context, req *accessservice.CheckAndReserveQuotaRequest) (*EnvelopeResponse, error)
	ReleaseQuotaOnFailure(ctx context.Context, req *accessservice.ReleaseQuotaOnFailureRequest) (*EnvelopeResponse, error)
	RecordUsageStat(ctx context.Context, req *accessservice.RecordUsageStatRequest) (*EnvelopeResponse, error)
	QueryUsageStats(ctx context.Context, req *accessservice.QueryUsageStatsRequest) (*EnvelopeResponse, error)
	AuthenticateConsoleUser(ctx context.Context, req *accessservice.AuthenticateConsoleUserRequest) (*EnvelopeResponse, error)
	CreateTenant(ctx context.Context, req *accessservice.CreateTenantRequest) (*EnvelopeResponse, error)
	ListTenants(ctx context.Context, req *accessservice.ListTenantsRequest) (*EnvelopeResponse, error)
	CreateTenantApp(ctx context.Context, req *accessservice.CreateTenantAppRequest) (*EnvelopeResponse, error)
	ListTenantApps(ctx context.Context, req *accessservice.ListTenantAppsRequest) (*EnvelopeResponse, error)
	UpdateTenantAppStatus(ctx context.Context, req *accessservice.UpdateTenantAppStatusRequest) (*EnvelopeResponse, error)
	UpdateAppQuota(ctx context.Context, req *accessservice.UpdateAppQuotaRequest) (*EnvelopeResponse, error)
	CheckIpBan(ctx context.Context, req *accessservice.CheckIpBanRequest) (*EnvelopeResponse, error)
}

type PolicyRPC interface {
	Health(ctx context.Context) (*EnvelopeResponse, error)
	ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*EnvelopeResponse, error)
	CheckAppPolicyAccess(ctx context.Context, req *policyservice.CheckAppPolicyAccessRequest) (*EnvelopeResponse, error)
	ListPolicies(ctx context.Context, req *policyservice.ListPoliciesRequest) (*EnvelopeResponse, error)
	PublishPolicyConfig(ctx context.Context, req *policyservice.PublishPolicyConfigRequest) (*EnvelopeResponse, error)
	BindAppPolicies(ctx context.Context, req *policyservice.BindAppPoliciesRequest) (*EnvelopeResponse, error)
}

type ProviderRPC interface {
	Health(ctx context.Context) (*EnvelopeResponse, error)
	ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*EnvelopeResponse, error)
	HealthCheckProvider(ctx context.Context, req *providerservice.HealthCheckProviderRequest) (*EnvelopeResponse, error)
}

type SchedulerRPC interface {
	Health(ctx context.Context) (*EnvelopeResponse, error)
	CreateTask(ctx context.Context, req *schedulerservice.CreateTaskRequest) (*EnvelopeResponse, error)
	GetTask(ctx context.Context, req *schedulerservice.GetTaskRequest) (*EnvelopeResponse, error)
	ListTaskRuns(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*EnvelopeResponse, error)
}

type CollectorWorkerRPC interface {
	Health(ctx context.Context) (*EnvelopeResponse, error)
	GetWorkerState(ctx context.Context, req *collectorworkerservice.GetWorkerStateRequest) (*EnvelopeResponse, error)
}

type accessRPCClient struct {
	rpcClient zrpc.Client
	client    accessservice.AccessService
}

type policyRPCClient struct {
	rpcClient zrpc.Client
	client    policyservice.PolicyService
}

type providerRPCClient struct {
	rpcClient zrpc.Client
	client    providerservice.ProviderService
}

type schedulerRPCClient struct {
	rpcClient zrpc.Client
	client    schedulerservice.SchedulerService
}

type collectorWorkerRPCClient struct {
	rpcClient zrpc.Client
	client    collectorworkerservice.CollectorWorkerService
}

type jsonRPCResponse interface {
	GetCode() int32
	GetMessage() string
	GetDataJson() string
}

func NewAccessRPC(conf zrpc.RpcClientConf) AccessRPC {
	cli := zrpc.MustNewClient(conf)
	return &accessRPCClient{
		rpcClient: cli,
		client:    accessservice.NewAccessService(cli),
	}
}

func NewPolicyRPC(conf zrpc.RpcClientConf) PolicyRPC {
	cli := zrpc.MustNewClient(conf)
	return &policyRPCClient{
		rpcClient: cli,
		client:    policyservice.NewPolicyService(cli),
	}
}

func NewProviderRPC(conf zrpc.RpcClientConf) ProviderRPC {
	cli := zrpc.MustNewClient(conf)
	return &providerRPCClient{
		rpcClient: cli,
		client:    providerservice.NewProviderService(cli),
	}
}

func NewSchedulerRPC(conf zrpc.RpcClientConf) SchedulerRPC {
	cli := zrpc.MustNewClient(conf)
	return &schedulerRPCClient{
		rpcClient: cli,
		client:    schedulerservice.NewSchedulerService(cli),
	}
}

func NewCollectorWorkerRPC(conf zrpc.RpcClientConf) CollectorWorkerRPC {
	cli := zrpc.MustNewClient(conf)
	return &collectorWorkerRPCClient{
		rpcClient: cli,
		client:    collectorworkerservice.NewCollectorWorkerService(cli),
	}
}

func (c *accessRPCClient) Health(ctx context.Context) (*EnvelopeResponse, error) {
	return grpcHealthEnvelope(ctx, c.rpcClient)
}

func (c *accessRPCClient) GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.GetAppAuthContextByID(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) CheckReplay(ctx context.Context, req *accessservice.CheckReplayRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CheckReplay(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) CheckAndReserveQuota(ctx context.Context, req *accessservice.CheckAndReserveQuotaRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CheckAndReserveQuota(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) ReleaseQuotaOnFailure(ctx context.Context, req *accessservice.ReleaseQuotaOnFailureRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ReleaseQuotaOnFailure(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) RecordUsageStat(ctx context.Context, req *accessservice.RecordUsageStatRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.RecordUsageStat(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) QueryUsageStats(ctx context.Context, req *accessservice.QueryUsageStatsRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.QueryUsageStats(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) AuthenticateConsoleUser(ctx context.Context, req *accessservice.AuthenticateConsoleUserRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.AuthenticateConsoleUser(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) CreateTenant(ctx context.Context, req *accessservice.CreateTenantRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CreateTenant(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) ListTenants(ctx context.Context, req *accessservice.ListTenantsRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ListTenants(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) CreateTenantApp(ctx context.Context, req *accessservice.CreateTenantAppRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CreateTenantApp(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) ListTenantApps(ctx context.Context, req *accessservice.ListTenantAppsRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ListTenantApps(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) UpdateTenantAppStatus(ctx context.Context, req *accessservice.UpdateTenantAppStatusRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.UpdateTenantAppStatus(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) UpdateAppQuota(ctx context.Context, req *accessservice.UpdateAppQuotaRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.UpdateAppQuota(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *accessRPCClient) CheckIpBan(ctx context.Context, req *accessservice.CheckIpBanRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CheckIpBan(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *policyRPCClient) Health(ctx context.Context) (*EnvelopeResponse, error) {
	return grpcHealthEnvelope(ctx, c.rpcClient)
}

func (c *policyRPCClient) ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ResolvePolicy(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *policyRPCClient) CheckAppPolicyAccess(ctx context.Context, req *policyservice.CheckAppPolicyAccessRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CheckAppPolicyAccess(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *policyRPCClient) ListPolicies(ctx context.Context, req *policyservice.ListPoliciesRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ListPolicies(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *policyRPCClient) PublishPolicyConfig(ctx context.Context, req *policyservice.PublishPolicyConfigRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.PublishPolicyConfig(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *policyRPCClient) BindAppPolicies(ctx context.Context, req *policyservice.BindAppPoliciesRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.BindAppPolicies(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *providerRPCClient) Health(ctx context.Context) (*EnvelopeResponse, error) {
	return grpcHealthEnvelope(ctx, c.rpcClient)
}

func (c *providerRPCClient) ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ExecutePolicy(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *providerRPCClient) HealthCheckProvider(ctx context.Context, req *providerservice.HealthCheckProviderRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.HealthCheckProvider(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *schedulerRPCClient) Health(ctx context.Context) (*EnvelopeResponse, error) {
	return grpcHealthEnvelope(ctx, c.rpcClient)
}

func (c *schedulerRPCClient) CreateTask(ctx context.Context, req *schedulerservice.CreateTaskRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.CreateTask(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *schedulerRPCClient) GetTask(ctx context.Context, req *schedulerservice.GetTaskRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.GetTask(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *schedulerRPCClient) ListTaskRuns(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.ListTaskRuns(ctx, req)
	return rpcEnvelope(resp, err)
}

func (c *collectorWorkerRPCClient) Health(ctx context.Context) (*EnvelopeResponse, error) {
	return grpcHealthEnvelope(ctx, c.rpcClient)
}

func (c *collectorWorkerRPCClient) GetWorkerState(ctx context.Context, req *collectorworkerservice.GetWorkerStateRequest) (*EnvelopeResponse, error) {
	resp, err := c.client.GetWorkerState(ctx, req)
	return rpcEnvelope(resp, err)
}

func rpcEnvelope(resp jsonRPCResponse, callErr error) (*EnvelopeResponse, error) {
	if callErr != nil {
		return nil, callErr
	}
	if resp == nil {
		return nil, nil
	}

	envelope := &EnvelopeResponse{
		Code:    int(resp.GetCode()),
		Message: resp.GetMessage(),
		Data:    json.RawMessage("null"),
	}
	if data := resp.GetDataJson(); data != "" {
		envelope.Data = json.RawMessage(data)
	}
	if resp.GetCode() != int32(model.CodeOK) {
		return envelope, &RPCError{
			Code:    envelope.Code,
			Message: envelope.Message,
		}
	}

	return envelope, nil
}

func grpcHealthEnvelope(ctx context.Context, rpcClient zrpc.Client) (*EnvelopeResponse, error) {
	healthClient := grpc_health_v1.NewHealthClient(rpcClient.Conn())
	resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"status":  resp.GetStatus().String(),
		"healthy": resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING,
	}
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return &EnvelopeResponse{
		Code:    model.CodeOK,
		Message: "ok",
		Data:    body,
	}, nil
}

type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}
