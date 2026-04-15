package internal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPGRedisStoreGetAppAuthContextByIDUsesSnapshotCache(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	cached := tenantApp{
		ID:         "app_1",
		TenantID:   "tenant_1",
		Name:       "Acme App",
		AppKey:     "legacy_key",
		AppSecret:  "legacy_secret",
		Status:     "active",
		DailyQuota: 99,
		QPSLimit:   8,
	}
	payload, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal cached tenant app: %v", err)
	}
	if err := client.Set(ctx, appSnapshotCacheKey("app_1"), payload, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app snapshot cache: %v", err)
	}

	data, svcErr := store.getAppAuthContextByID(ctx, getAppAuthContextByIDRequest{AppID: "app_1"})
	if svcErr != nil {
		t.Fatalf("getAppAuthContextByID returned error: %v", svcErr)
	}

	payloadMap := data.(map[string]any)
	if payloadMap["tenant_id"] != "tenant_1" || payloadMap["app_id"] != "app_1" {
		t.Fatalf("unexpected cached auth payload: %#v", payloadMap)
	}
	if _, ok := payloadMap["app_key"]; ok {
		t.Fatalf("did not expect app_key in auth payload: %#v", payloadMap)
	}
	if _, ok := payloadMap["app_secret"]; ok {
		t.Fatalf("did not expect app_secret in auth payload: %#v", payloadMap)
	}
}

func TestPGRedisStoreGetTenantAppByIDUsesRedisCache(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	cached := tenantApp{
		ID:         "app_1",
		TenantID:   "tenant_1",
		Name:       "Acme App",
		AppKey:     "app_key_1",
		Status:     "active",
		DailyQuota: 50,
		QPSLimit:   20,
	}
	payload, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal cached tenant app: %v", err)
	}
	if err := client.Set(ctx, appSnapshotCacheKey("app_1"), payload, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app snapshot cache: %v", err)
	}

	item, svcErr := store.getTenantAppByID(ctx, "app_1")
	if svcErr != nil {
		t.Fatalf("getTenantAppByID returned error: %v", svcErr)
	}
	if item.TenantID != "tenant_1" || item.DailyQuota != 50 {
		t.Fatalf("unexpected cached tenant app: %#v", item)
	}
}

func TestPGRedisStoreInvalidateTenantAppCacheRemovesSnapshot(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	if err := client.Set(ctx, appSnapshotCacheKey("app_1"), `{"app_id":"app_1"}`, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app snapshot cache: %v", err)
	}

	if err := store.invalidateTenantAppCache(ctx, "app_1"); err != nil {
		t.Fatalf("invalidateTenantAppCache returned error: %v", err)
	}

	if server.Exists(appSnapshotCacheKey("app_1")) {
		t.Fatal("expected app snapshot cache to be invalidated")
	}
}

func TestPGRedisStoreCacheWritesUseConfiguredTTLAndPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis:                  client,
		appMetadataCacheTTL:    2500 * time.Millisecond,
		appMetadataCachePrefix: "tenant-a:",
	}

	if err := store.cacheTenantApp(ctx, tenantApp{
		ID:         "app_1",
		TenantID:   "tenant_1",
		Name:       "Acme App",
		AppKey:     "app_key_1",
		Status:     "active",
		DailyQuota: 50,
		QPSLimit:   20,
	}); err != nil {
		t.Fatalf("cacheTenantApp returned error: %v", err)
	}

	snapshotKey := store.appSnapshotCacheKey("app_1")

	if !server.Exists(snapshotKey) {
		t.Fatalf("expected prefixed snapshot cache key to exist, got keys=%v", server.Keys())
	}
	if ttl := server.TTL(snapshotKey); ttl != 2500*time.Millisecond {
		t.Fatalf("expected snapshot key ttl 2500ms, got %s", ttl)
	}
}

func TestReleaseUsageStatsFlushLockOnlyDeletesMatchingToken(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	if err := client.Set(ctx, usageStatsFlushLockKey, "new-owner", time.Minute).Err(); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	if err := releaseUsageStatsFlushLock(ctx, client, "old-owner"); err != nil {
		t.Fatalf("releaseUsageStatsFlushLock returned error: %v", err)
	}
	got, err := server.Get(usageStatsFlushLockKey)
	if err != nil {
		t.Fatalf("load lock after stale release: %v", err)
	}
	if got != "new-owner" {
		t.Fatalf("expected lock to remain owned by new token, got %q", got)
	}

	if err := releaseUsageStatsFlushLock(ctx, client, "new-owner"); err != nil {
		t.Fatalf("releaseUsageStatsFlushLock returned error: %v", err)
	}
	if server.Exists(usageStatsFlushLockKey) {
		t.Fatal("expected matching token to release lock")
	}
}
