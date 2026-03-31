package internal

import (
	"context"
	"strings"
	"testing"
	"time"

	"xsonar/pkg/xlog"
)

func TestAccessServiceLifecycle(t *testing.T) {
	t.Setenv("ACCESS_RPC_SEED_ADMIN_USERNAME", "admin")
	t.Setenv("ACCESS_RPC_SEED_ADMIN_PASSWORD", "admin123456")

	svc := newService(xlog.NewStdout("access-rpc-test"))
	ctx := context.Background()
	if svc.consoleUsers["admin"].PasswordHash == "admin123456" {
		t.Fatal("expected seeded admin password to be hashed")
	}

	authData, authErr := svc.authenticateConsoleUser(ctx, authenticateConsoleUserRequest{
		Username: "admin",
		Password: "admin123456",
	})
	if authErr != nil {
		t.Fatalf("authenticateConsoleUser returned error: %v", authErr)
	}
	if authData == nil {
		t.Fatal("authenticateConsoleUser returned nil data")
	}
	authMap := authData.(map[string]any)
	if authMap["role"] != "platform_admin" {
		t.Fatalf("unexpected auth payload: %#v", authMap)
	}

	tenantData, tenantErr := svc.createTenant(ctx, createTenantRequest{Name: "Acme"})
	if tenantErr != nil {
		t.Fatalf("createTenant returned error: %v", tenantErr)
	}
	tenant := tenantData.(*tenant)

	appData, appErr := svc.createTenantApp(ctx, createTenantAppRequest{
		TenantID:   tenant.ID,
		Name:       "Acme App",
		DailyQuota: 1,
		QPSLimit:   10,
	})
	if appErr != nil {
		t.Fatalf("createTenantApp returned error: %v", appErr)
	}
	app := appData.(*tenantApp)

	contextData, contextErr := svc.getAppAuthContext(ctx, getAppAuthContextRequest{AppKey: app.AppKey})
	if contextErr != nil {
		t.Fatalf("getAppAuthContext returned error: %v", contextErr)
	}
	contextMap := contextData.(map[string]any)
	if contextMap["app_secret"] == "" {
		t.Fatal("expected app_secret in auth context")
	}

	if _, replayErr := svc.checkReplay(ctx, checkReplayRequest{AppID: app.ID, Nonce: "nonce-1", Timestamp: time.Now().Unix()}); replayErr != nil {
		t.Fatalf("first checkReplay returned error: %v", replayErr)
	}
	if _, replayErr := svc.checkReplay(ctx, checkReplayRequest{AppID: app.ID, Nonce: "nonce-1", Timestamp: time.Now().Unix()}); replayErr == nil {
		t.Fatal("expected duplicate nonce to be rejected")
	}

	if _, reserveErr := svc.checkAndReserveQuota(ctx, checkAndReserveQuotaRequest{AppID: app.ID, RequestID: "req-1", PolicyKey: "users_by_ids_v1"}); reserveErr != nil {
		t.Fatalf("checkAndReserveQuota returned error: %v", reserveErr)
	}
	if _, reserveErr := svc.checkAndReserveQuota(ctx, checkAndReserveQuotaRequest{AppID: app.ID, RequestID: "req-2", PolicyKey: "users_by_ids_v1"}); reserveErr == nil {
		t.Fatal("expected daily quota to be exhausted")
	}

	if _, releaseErr := svc.releaseQuotaOnFailure(ctx, releaseQuotaOnFailureRequest{AppID: app.ID, RequestID: "req-1"}); releaseErr != nil {
		t.Fatalf("releaseQuotaOnFailure returned error: %v", releaseErr)
	}
	if _, reserveErr := svc.checkAndReserveQuota(ctx, checkAndReserveQuotaRequest{AppID: app.ID, RequestID: "req-3", PolicyKey: "users_by_ids_v1"}); reserveErr != nil {
		t.Fatalf("expected quota reservation after release to succeed, got: %v", reserveErr)
	}

	if _, recordErr := svc.recordUsageStat(ctx, recordUsageStatRequest{
		TenantID:           tenant.ID,
		AppID:              app.ID,
		PolicyKey:          "users_by_ids_v1",
		RequestID:          "req-3",
		Success:            true,
		DurationMS:         25,
		UpstreamDurationMS: 12,
		StatusCode:         200,
		ResultCode:         "OK",
	}); recordErr != nil {
		t.Fatalf("recordUsageStat returned error: %v", recordErr)
	}

	statsData, statsErr := svc.queryUsageStats(ctx, queryUsageStatsRequest{
		TenantID:  tenant.ID,
		AppID:     app.ID,
		PolicyKey: "users_by_ids_v1",
	})
	if statsErr != nil {
		t.Fatalf("queryUsageStats returned error: %v", statsErr)
	}

	items := statsData.(map[string]any)["items"].([]usageStat)
	if len(items) != 1 {
		t.Fatalf("expected 1 usage stat, got %d", len(items))
	}
	if items[0].SuccessCount != 1 {
		t.Fatalf("expected success_count=1, got %d", items[0].SuccessCount)
	}
}

func TestCurrentUsageStatsBucketStart(t *testing.T) {
	now := time.Date(2026, 3, 29, 13, 7, 42, 0, time.UTC)
	bucketStart := currentUsageStatsBucketStart(now)
	expected := time.Date(2026, 3, 29, 13, 5, 0, 0, time.UTC)
	if !bucketStart.Equal(expected) {
		t.Fatalf("expected bucket start %s, got %s", expected, bucketStart)
	}
}

func TestMergeUsageStats(t *testing.T) {
	bucketStart := time.Date(2026, 3, 29, 13, 5, 0, 0, time.UTC)
	merged := mergeUsageStats(
		[]usageStat{{
			BucketStart:           bucketStart,
			TenantID:              "tenant_1",
			AppID:                 "app_1",
			PolicyKey:             "users_by_ids_v1",
			TotalCount:            2,
			SuccessCount:          1,
			FailureCount:          1,
			DurationSumMS:         20,
			UpstreamDurationSumMS: 10,
		}},
		[]usageStat{{
			BucketStart:           bucketStart,
			TenantID:              "tenant_1",
			AppID:                 "app_1",
			PolicyKey:             "users_by_ids_v1",
			TotalCount:            3,
			SuccessCount:          2,
			FailureCount:          1,
			DurationSumMS:         30,
			UpstreamDurationSumMS: 15,
		}},
	)

	if len(merged) != 1 {
		t.Fatalf("expected 1 merged item, got %d", len(merged))
	}
	if merged[0].TotalCount != 5 || merged[0].SuccessCount != 3 || merged[0].FailureCount != 2 {
		t.Fatalf("unexpected merged counters: %+v", merged[0])
	}
	if merged[0].DurationSumMS != 50 || merged[0].UpstreamDurationSumMS != 25 {
		t.Fatalf("unexpected merged durations: %+v", merged[0])
	}
}

func TestAccessServiceSkipsInvalidSeedPassword(t *testing.T) {
	t.Setenv("ACCESS_RPC_SEED_ADMIN_USERNAME", "admin")
	t.Setenv("ACCESS_RPC_SEED_ADMIN_PASSWORD", strings.Repeat("x", 80))

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("expected invalid seed password to be skipped without panic, got %v", recovered)
		}
	}()

	svc := newService(xlog.NewStdout("access-rpc-test"))
	if _, exists := svc.consoleUsers["admin"]; exists {
		t.Fatal("expected invalid bootstrap admin to be skipped")
	}
}

func TestAccessServicePanicsWhenPGRedisMasterKeyMissing(t *testing.T) {
	t.Setenv("COMMON_STORE_BACKEND", "pgredis")
	t.Setenv("COMMON_SECRET_MASTER_KEY", "")
	t.Setenv("COMMON_POSTGRES_DSN", "://")
	t.Setenv("COMMON_REDIS_ADDR", "127.0.0.1:1")

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected missing COMMON_SECRET_MASTER_KEY to abort pgredis startup")
		}
	}()

	_ = newService(xlog.NewStdout("access-rpc-test"))
}

func TestLoadAccessStoreConfigReadsAppMetadataCacheSettings(t *testing.T) {
	t.Setenv("ACCESS_RPC_APP_METADATA_CACHE_TTL_MS", "2500")
	t.Setenv("ACCESS_RPC_APP_METADATA_CACHE_PREFIX", "tenant-a:")

	cfg := loadAccessStoreConfig()
	if cfg.AppMetadataCacheTTL != 2500*time.Millisecond {
		t.Fatalf("expected app metadata cache ttl to be 2500ms, got %s", cfg.AppMetadataCacheTTL)
	}
	if cfg.AppMetadataCachePrefix != "tenant-a:" {
		t.Fatalf("expected app metadata cache prefix to be tenant-a:, got %q", cfg.AppMetadataCachePrefix)
	}
}
