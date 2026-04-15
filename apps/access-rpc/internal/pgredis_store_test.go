package internal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPGRedisStoreGetAppAuthContextUsesRedisCache(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	cached := appAuthContextCacheValue{
		TenantID:   "tenant_1",
		AppID:      "app_1",
		AppKey:     "app_key_1",
		AppSecret:  "secret_1",
		Status:     "active",
		DailyQuota: 99,
		QPSLimit:   8,
	}
	payload, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal cached auth context: %v", err)
	}
	if err := client.Set(ctx, appAuthContextCacheKey("app_key_1"), payload, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed auth cache: %v", err)
	}
	if err := client.Set(ctx, appKeyIndexCacheKey("app_1"), "app_key_1", defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app key index cache: %v", err)
	}

	data, svcErr := store.getAppAuthContext(ctx, getAppAuthContextRequest{AppKey: "app_key_1"})
	if svcErr != nil {
		t.Fatalf("getAppAuthContext returned error: %v", svcErr)
	}

	payloadMap := data.(map[string]any)
	if payloadMap["tenant_id"] != "tenant_1" || payloadMap["app_secret"] != "secret_1" {
		t.Fatalf("unexpected cached auth payload: %#v", payloadMap)
	}
}

func TestPGRedisStoreGetAppAuthContextByIDUsesRedisCacheIndex(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	cached := appAuthContextCacheValue{
		TenantID:   "tenant_1",
		AppID:      "app_1",
		AppKey:     "app_key_1",
		AppSecret:  "secret_1",
		Status:     "active",
		DailyQuota: 99,
		QPSLimit:   8,
	}
	payload, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal cached auth context: %v", err)
	}
	if err := client.Set(ctx, appAuthContextCacheKey("app_key_1"), payload, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed auth cache: %v", err)
	}
	if err := client.Set(ctx, appKeyIndexCacheKey("app_1"), "app_key_1", defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app key index cache: %v", err)
	}

	data, svcErr := store.getAppAuthContextByID(ctx, getAppAuthContextByIDRequest{AppID: "app_1"})
	if svcErr != nil {
		t.Fatalf("getAppAuthContextByID returned error: %v", svcErr)
	}

	payloadMap := data.(map[string]any)
	if payloadMap["tenant_id"] != "tenant_1" || payloadMap["app_secret"] != "secret_1" {
		t.Fatalf("unexpected cached auth payload: %#v", payloadMap)
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
	if err := client.Set(ctx, appKeyIndexCacheKey("app_1"), "app_key_1", defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app key index cache: %v", err)
	}

	item, svcErr := store.getTenantAppByID(ctx, "app_1")
	if svcErr != nil {
		t.Fatalf("getTenantAppByID returned error: %v", svcErr)
	}
	if item.AppKey != "app_key_1" || item.DailyQuota != 50 {
		t.Fatalf("unexpected cached tenant app: %#v", item)
	}
}

func TestPGRedisStoreInvalidateTenantAppCacheRemovesRedisEntries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx := context.Background()
	store := &pgRedisStore{
		redis: client,
	}

	if err := client.Set(ctx, appAuthContextCacheKey("app_key_1"), `{"app_id":"app_1"}`, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed auth cache: %v", err)
	}
	if err := client.Set(ctx, appSnapshotCacheKey("app_1"), `{"app_id":"app_1","app_key":"app_key_1"}`, defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app snapshot cache: %v", err)
	}
	if err := client.Set(ctx, appKeyIndexCacheKey("app_1"), "app_key_1", defaultAppMetadataCacheTTL).Err(); err != nil {
		t.Fatalf("seed app key index cache: %v", err)
	}

	if err := store.invalidateTenantAppCache(ctx, "app_1"); err != nil {
		t.Fatalf("invalidateTenantAppCache returned error: %v", err)
	}

	if server.Exists(appAuthContextCacheKey("app_key_1")) {
		t.Fatal("expected auth context cache to be invalidated")
	}
	if server.Exists(appSnapshotCacheKey("app_1")) {
		t.Fatal("expected app snapshot cache to be invalidated")
	}
	if server.Exists(appKeyIndexCacheKey("app_1")) {
		t.Fatal("expected app key index cache to be invalidated")
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

	if err := store.cacheAppAuthContext(ctx, appAuthContextCacheValue{
		TenantID:   "tenant_1",
		AppID:      "app_1",
		AppKey:     "app_key_1",
		AppSecret:  "secret_1",
		Status:     "active",
		DailyQuota: 99,
		QPSLimit:   8,
	}); err != nil {
		t.Fatalf("cacheAppAuthContext returned error: %v", err)
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

	authKey := store.appAuthContextCacheKey("app_key_1")
	snapshotKey := store.appSnapshotCacheKey("app_1")
	indexKey := store.appKeyIndexCacheKey("app_1")

	if !server.Exists(authKey) || !server.Exists(snapshotKey) || !server.Exists(indexKey) {
		t.Fatalf("expected prefixed cache keys to exist, got keys=%v", server.Keys())
	}
	if server.Exists(appAuthContextCachePrefix + "app_key_1") {
		t.Fatal("expected unprefixed auth context key to remain unused")
	}
	if ttl := server.TTL(authKey); ttl != 2500*time.Millisecond {
		t.Fatalf("expected auth key ttl 2500ms, got %s", ttl)
	}
	if ttl := server.TTL(snapshotKey); ttl != 2500*time.Millisecond {
		t.Fatalf("expected snapshot key ttl 2500ms, got %s", ttl)
	}
	if ttl := server.TTL(indexKey); ttl != 2500*time.Millisecond {
		t.Fatalf("expected index key ttl 2500ms, got %s", ttl)
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
