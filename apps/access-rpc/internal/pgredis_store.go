package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type accessStoreConfig struct {
	Backend                 string
	PostgresDSN             string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	SecretMasterKey         string
	UsageStatsFlushInterval time.Duration
	AppMetadataCacheTTL     time.Duration
	AppMetadataCachePrefix  string
	SeedAdminUsername       string
	SeedAdminPassword       string
}

type pgRedisStore struct {
	logger                  *xlog.Logger
	pg                      *pgxpool.Pool
	redis                   *redis.Client
	secretMasterKey         []byte
	usageStatsFlushInterval time.Duration
	appMetadataCacheTTL     time.Duration
	appMetadataCachePrefix  string
	stop                    context.CancelFunc
	flusherDone             chan struct{}
}

type appAuthContextCacheValue struct {
	TenantID   string
	AppID      string
	AppKey     string
	AppSecret  string
	Status     string
	DailyQuota int64
	QPSLimit   int
}

const (
	usageStatsBucketSize           = 5 * time.Minute
	usageStatsRedisTTL             = 48 * time.Hour
	usageStatsPendingSetKey        = "usage_stats:pending"
	usageStatsFlushLockKey         = "usage_stats:flush:lock"
	usageStatsFlushLockTTL         = 30 * time.Second
	defaultUsageStatsFlushInterval = time.Minute
	defaultAppMetadataCacheTTL     = time.Minute
	defaultAppMetadataCachePrefix  = ""
	appAuthContextCachePrefix      = "app_auth_ctx:"
	appSnapshotCachePrefix         = "app_snapshot:"
	appKeyIndexCachePrefix         = "app_key_by_id:"
)

var releaseUsageStatsFlushLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func loadAccessStoreConfig() accessStoreConfig {
	cfg := accessStoreConfig{
		Backend: strings.ToLower(strings.TrimSpace(firstNonEmpty(
			os.Getenv("ACCESS_RPC_STORE_BACKEND"),
			os.Getenv("COMMON_STORE_BACKEND"),
		))),
		PostgresDSN: strings.TrimSpace(firstNonEmpty(
			os.Getenv("ACCESS_RPC_POSTGRES_DSN"),
			os.Getenv("COMMON_POSTGRES_DSN"),
		)),
		RedisAddr: strings.TrimSpace(firstNonEmpty(
			os.Getenv("ACCESS_RPC_REDIS_ADDR"),
			os.Getenv("COMMON_REDIS_ADDR"),
		)),
		RedisPassword: firstNonEmpty(
			os.Getenv("ACCESS_RPC_REDIS_PASSWORD"),
			os.Getenv("COMMON_REDIS_PASSWORD"),
		),
		SecretMasterKey:         strings.TrimSpace(os.Getenv("COMMON_SECRET_MASTER_KEY")),
		UsageStatsFlushInterval: defaultUsageStatsFlushInterval,
		AppMetadataCacheTTL:     defaultAppMetadataCacheTTL,
		AppMetadataCachePrefix:  defaultAppMetadataCachePrefix,
		SeedAdminUsername:       strings.TrimSpace(os.Getenv("ACCESS_RPC_SEED_ADMIN_USERNAME")),
		SeedAdminPassword:       os.Getenv("ACCESS_RPC_SEED_ADMIN_PASSWORD"),
	}

	if redisDBValue := strings.TrimSpace(firstNonEmpty(
		os.Getenv("ACCESS_RPC_REDIS_DB"),
		os.Getenv("COMMON_REDIS_DB"),
	)); redisDBValue != "" {
		if parsed, err := strconv.Atoi(redisDBValue); err == nil {
			cfg.RedisDB = parsed
		}
	}
	if flushValue := strings.TrimSpace(os.Getenv("ACCESS_RPC_USAGE_STATS_FLUSH_INTERVAL_MS")); flushValue != "" {
		if parsed, err := strconv.Atoi(flushValue); err == nil && parsed > 0 {
			cfg.UsageStatsFlushInterval = time.Duration(parsed) * time.Millisecond
		}
	}
	if ttlValue := strings.TrimSpace(os.Getenv("ACCESS_RPC_APP_METADATA_CACHE_TTL_MS")); ttlValue != "" {
		if parsed, err := strconv.Atoi(ttlValue); err == nil && parsed > 0 {
			cfg.AppMetadataCacheTTL = time.Duration(parsed) * time.Millisecond
		}
	}
	if prefixValue := strings.TrimSpace(os.Getenv("ACCESS_RPC_APP_METADATA_CACHE_PREFIX")); prefixValue != "" {
		cfg.AppMetadataCachePrefix = prefixValue
	}

	if cfg.Backend == "" {
		if cfg.PostgresDSN != "" && cfg.RedisAddr != "" {
			cfg.Backend = "pgredis"
		} else {
			cfg.Backend = "memory"
		}
	}

	return cfg
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func newPGRedisStore(cfg accessStoreConfig, logger *xlog.Logger) (*pgRedisStore, error) {
	secretMasterKey, err := shared.ParseSecretMasterKey(cfg.SecretMasterKey)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	store := &pgRedisStore{
		logger:                  logger,
		pg:                      pool,
		redis:                   redisClient,
		secretMasterKey:         secretMasterKey,
		usageStatsFlushInterval: cfg.UsageStatsFlushInterval,
		appMetadataCacheTTL:     cfg.AppMetadataCacheTTL,
		appMetadataCachePrefix:  cfg.AppMetadataCachePrefix,
		flusherDone:             make(chan struct{}),
	}

	if err := store.ensureSchema(ctx); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}
	if err := store.seedAdmin(ctx, cfg.SeedAdminUsername, cfg.SeedAdminPassword); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}
	workerCtx, stop := context.WithCancel(context.Background())
	store.stop = stop
	go store.runUsageStatsFlusher(workerCtx, cfg.UsageStatsFlushInterval)

	return store, nil
}

func (s *pgRedisStore) Close(ctx context.Context) error {
	if s.stop != nil {
		s.stop()
	}
	if s.flusherDone != nil {
		select {
		case <-s.flusherDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if s.pg != nil {
		s.pg.Close()
	}
	if s.redis != nil {
		return s.redis.Close()
	}
	return nil
}

func (s *pgRedisStore) ensureSchema(ctx context.Context) error {
	if _, err := s.pg.Exec(ctx, accessInitSQL); err != nil {
		return fmt.Errorf("ensure access schema: %w", err)
	}
	return nil
}

func (s *pgRedisStore) seedAdmin(ctx context.Context, username, password string) error {
	if username == "" || password == "" {
		return nil
	}

	passwordHash, err := shared.HashPassword(password)
	if err != nil {
		return fmt.Errorf("hash admin password: %w", err)
	}

	_, err = s.pg.Exec(ctx, `
		INSERT INTO access.console_users (id, username, password_hash, role, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'platform_admin', 'active', NOW(), NOW())
		ON CONFLICT (username) DO UPDATE
		SET password_hash = EXCLUDED.password_hash,
		    role = EXCLUDED.role,
		    status = EXCLUDED.status,
		    updated_at = NOW()
	`, shared.NewID("user"), username, passwordHash)
	if err != nil {
		return fmt.Errorf("seed admin user: %w", err)
	}

	s.logger.Info("access-rpc connected to postgres/redis backend", map[string]any{
		"seed_admin_username": username,
	})

	return nil
}

func (s *pgRedisStore) authenticateConsoleUser(ctx context.Context, req authenticateConsoleUserRequest) (any, *serviceError) {
	var user consoleUser
	err := s.pg.QueryRow(ctx, `
		SELECT id, username, password_hash, role, status
		FROM access.console_users
		WHERE username = $1
	`, req.Username).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Role, &user.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, unauthorized("invalid username or password")
	}
	if err != nil {
		s.logger.Error("query console user failed", map[string]any{"error": err.Error()})
		return nil, internalError("query console user failed")
	}
	if !shared.VerifyPassword(user.PasswordHash, req.Password) || user.Status != "active" {
		return nil, unauthorized("invalid username or password")
	}

	return map[string]any{
		"user_id": user.ID,
		"role":    user.Role,
	}, nil
}

func (s *pgRedisStore) createTenant(ctx context.Context, req createTenantRequest) (any, *serviceError) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, invalidRequest("tenant name is required")
	}

	item := &tenant{
		ID:        shared.NewID("tenant"),
		Name:      name,
		Status:    "active",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	_, err := s.pg.Exec(ctx, `
		INSERT INTO access.tenants (id, name, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`, item.ID, item.Name, item.Status, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		s.logger.Error("create tenant failed", map[string]any{"error": err.Error()})
		return nil, internalError("create tenant failed")
	}

	return item, nil
}

func (s *pgRedisStore) listTenants(ctx context.Context) (any, *serviceError) {
	rows, err := s.pg.Query(ctx, `
		SELECT id, name, status, created_at, updated_at
		FROM access.tenants
		ORDER BY created_at ASC
	`)
	if err != nil {
		s.logger.Error("list tenants failed", map[string]any{"error": err.Error()})
		return nil, internalError("list tenants failed")
	}
	defer rows.Close()

	items := make([]tenant, 0)
	for rows.Next() {
		var item tenant
		if err := rows.Scan(&item.ID, &item.Name, &item.Status, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, internalError("decode tenants failed")
		}
		items = append(items, item)
	}

	return map[string]any{"items": items}, nil
}

func (s *pgRedisStore) createTenantApp(ctx context.Context, req createTenantAppRequest) (any, *serviceError) {
	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.Name) == "" {
		return nil, invalidRequest("tenant_id and name are required")
	}
	if req.DailyQuota < 0 || req.QPSLimit < 0 {
		return nil, invalidRequest("daily_quota and qps_limit must be non-negative")
	}

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		s.logger.Error("begin tenant app transaction failed", map[string]any{"error": err.Error()})
		return nil, internalError("create tenant app failed")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tenantExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM access.tenants WHERE id = $1)`, req.TenantID).Scan(&tenantExists); err != nil {
		return nil, internalError("validate tenant failed")
	}
	if !tenantExists {
		return nil, notFound("tenant not found")
	}

	now := time.Now().UTC()
	secretID := shared.NewID("secret")
	app := &tenantApp{
		ID:         shared.NewID("app"),
		TenantID:   req.TenantID,
		Name:       strings.TrimSpace(req.Name),
		AppKey:     shared.NewSecret("appkey"),
		AppSecret:  shared.NewSecret("secret"),
		Status:     "active",
		DailyQuota: req.DailyQuota,
		QPSLimit:   req.QPSLimit,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO access.secret_materials (id, app_id, secret_ciphertext, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'active', $4, $5)
	`, secretID, app.ID, mustEncryptAccessSecret(s.secretMasterKey, app.AppSecret), now, now); err != nil {
		return nil, internalError("create secret material failed")
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO access.tenant_apps (id, tenant_id, name, app_key, secret_material_id, daily_quota, qps_limit, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8, $9)
	`, app.ID, app.TenantID, app.Name, app.AppKey, secretID, app.DailyQuota, app.QPSLimit, now, now); err != nil {
		return nil, internalError("create tenant app failed")
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, internalError("commit tenant app failed")
	}

	if err := s.cacheTenantApp(ctx, *app); err != nil {
		s.logger.Error("cache tenant app snapshot failed", map[string]any{"error": err.Error(), "app_id": app.ID})
	}
	if err := s.cacheAppAuthContext(ctx, appAuthContextCacheValue{
		TenantID:   app.TenantID,
		AppID:      app.ID,
		AppKey:     app.AppKey,
		AppSecret:  app.AppSecret,
		Status:     app.Status,
		DailyQuota: app.DailyQuota,
		QPSLimit:   app.QPSLimit,
	}); err != nil {
		s.logger.Error("cache app auth context failed", map[string]any{"error": err.Error(), "app_id": app.ID})
	}

	return app, nil
}

func (s *pgRedisStore) listTenantApps(ctx context.Context, tenantID string) (any, *serviceError) {
	query := `
		SELECT a.id, a.tenant_id, a.name, a.app_key, a.status, a.daily_quota, a.qps_limit, a.created_at, a.updated_at
		FROM access.tenant_apps a
	`
	args := []any{}
	if tenantID != "" {
		query += ` WHERE a.tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY a.created_at ASC`

	rows, err := s.pg.Query(ctx, query, args...)
	if err != nil {
		s.logger.Error("list tenant apps failed", map[string]any{"error": err.Error()})
		return nil, internalError("list tenant apps failed")
	}
	defer rows.Close()

	items := make([]tenantApp, 0)
	for rows.Next() {
		var item tenantApp
		if err := rows.Scan(&item.ID, &item.TenantID, &item.Name, &item.AppKey, &item.Status, &item.DailyQuota, &item.QPSLimit, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, internalError("decode tenant apps failed")
		}
		items = append(items, item)
	}

	return map[string]any{"items": items}, nil
}

func (s *pgRedisStore) rotateAppSecret(ctx context.Context, req rotateAppSecretRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" {
		return nil, invalidRequest("app_id is required")
	}

	appSecret := shared.NewSecret("secret")
	encryptedSecret := mustEncryptAccessSecret(s.secretMasterKey, appSecret)
	tag, err := s.pg.Exec(ctx, `
		UPDATE access.secret_materials
		SET secret_ciphertext = $2, updated_at = NOW()
		WHERE app_id = $1
	`, req.AppID, encryptedSecret)
	if err != nil {
		return nil, internalError("rotate app secret failed")
	}
	if tag.RowsAffected() == 0 {
		return nil, notFound("app not found")
	}

	if err := s.invalidateTenantAppCache(ctx, req.AppID); err != nil {
		s.logger.Error("invalidate tenant app cache failed", map[string]any{"error": err.Error(), "app_id": req.AppID})
	}

	return map[string]any{
		"app_id":     req.AppID,
		"app_secret": appSecret,
		"updated_at": time.Now().UTC(),
	}, nil
}

func (s *pgRedisStore) updateTenantAppStatus(ctx context.Context, req updateTenantAppStatusRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.Status) == "" {
		return nil, invalidRequest("app_id and status are required")
	}

	status := strings.ToLower(strings.TrimSpace(req.Status))
	if status != "active" && status != "disabled" {
		return nil, invalidRequest("status must be active or disabled")
	}

	tag, err := s.pg.Exec(ctx, `
		UPDATE access.tenant_apps
		SET status = $2, updated_at = NOW()
		WHERE id = $1
	`, req.AppID, status)
	if err != nil {
		return nil, internalError("update app status failed")
	}
	if tag.RowsAffected() == 0 {
		return nil, notFound("app not found")
	}

	if err := s.invalidateTenantAppCache(ctx, req.AppID); err != nil {
		s.logger.Error("invalidate tenant app cache failed", map[string]any{"error": err.Error(), "app_id": req.AppID})
	}

	return s.getTenantAppSnapshot(ctx, req.AppID)
}

func (s *pgRedisStore) updateAppQuota(ctx context.Context, req updateAppQuotaRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" {
		return nil, invalidRequest("app_id is required")
	}
	if req.DailyQuota < 0 || req.QPSLimit < 0 {
		return nil, invalidRequest("daily_quota and qps_limit must be non-negative")
	}

	tag, err := s.pg.Exec(ctx, `
		UPDATE access.tenant_apps
		SET daily_quota = $2, qps_limit = $3, updated_at = NOW()
		WHERE id = $1
	`, req.AppID, req.DailyQuota, req.QPSLimit)
	if err != nil {
		return nil, internalError("update app quota failed")
	}
	if tag.RowsAffected() == 0 {
		return nil, notFound("app not found")
	}

	if err := s.invalidateTenantAppCache(ctx, req.AppID); err != nil {
		s.logger.Error("invalidate tenant app cache failed", map[string]any{"error": err.Error(), "app_id": req.AppID})
	}

	return s.getTenantAppSnapshot(ctx, req.AppID)
}

func (s *pgRedisStore) getAppAuthContext(ctx context.Context, req getAppAuthContextRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppKey) == "" {
		return nil, invalidRequest("app_key is required")
	}

	if cached, ok := s.loadCachedAppAuthContext(ctx, req.AppKey); ok {
		return cached.toMap(), nil
	}

	var item tenantApp
	err := s.pg.QueryRow(ctx, `
		SELECT a.id, a.tenant_id, a.name, a.app_key, a.status, a.daily_quota, a.qps_limit, a.created_at, a.updated_at, sm.secret_ciphertext
		FROM access.tenant_apps a
		JOIN access.secret_materials sm ON sm.id = a.secret_material_id
		WHERE a.app_key = $1
	`, req.AppKey).Scan(&item.ID, &item.TenantID, &item.Name, &item.AppKey, &item.Status, &item.DailyQuota, &item.QPSLimit, &item.CreatedAt, &item.UpdatedAt, &item.AppSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("app_key not found")
	}
	if err != nil {
		return nil, internalError("query app auth context failed")
	}
	if err := s.cacheTenantApp(ctx, tenantApp{
		ID:         item.ID,
		TenantID:   item.TenantID,
		Name:       item.Name,
		AppKey:     item.AppKey,
		Status:     item.Status,
		DailyQuota: item.DailyQuota,
		QPSLimit:   item.QPSLimit,
		CreatedAt:  item.CreatedAt,
		UpdatedAt:  item.UpdatedAt,
	}); err != nil {
		s.logger.Error("cache tenant app snapshot failed", map[string]any{"error": err.Error(), "app_id": item.ID})
	}
	item.AppSecret, err = shared.DecryptSecretValue(s.secretMasterKey, item.AppSecret)
	if err != nil {
		s.logger.Error("decode app secret failed", map[string]any{
			"error":   err.Error(),
			"app_key": req.AppKey,
		})
		return nil, internalError("decode app secret failed")
	}

	authContext := appAuthContextCacheValue{
		TenantID:   item.TenantID,
		AppID:      item.ID,
		AppKey:     item.AppKey,
		AppSecret:  item.AppSecret,
		Status:     item.Status,
		DailyQuota: item.DailyQuota,
		QPSLimit:   item.QPSLimit,
	}
	if err := s.cacheAppAuthContext(ctx, authContext); err != nil {
		s.logger.Error("cache app auth context failed", map[string]any{"error": err.Error(), "app_id": item.ID})
	}

	return authContext.toMap(), nil
}

func (s *pgRedisStore) checkReplay(ctx context.Context, req checkReplayRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.Nonce) == "" {
		return nil, invalidRequest("app_id and nonce are required")
	}
	if req.Timestamp == 0 {
		return nil, invalidRequest("timestamp is required")
	}

	now := time.Now().UTC()
	if absFloat64(float64(now.Unix()-req.Timestamp)) > replayWindow.Seconds() {
		return nil, forbidden("timestamp drift exceeds 60 seconds")
	}

	key := fmt.Sprintf("replay:%s:%s", req.AppID, req.Nonce)
	ok, err := s.redis.SetNX(ctx, key, "1", replayWindow).Result()
	if err != nil {
		return nil, internalError("check replay failed")
	}
	if !ok {
		return nil, conflict("nonce already used")
	}

	return map[string]any{
		"accepted":              true,
		"replay_window_seconds": int(replayWindow.Seconds()),
	}, nil
}

func (s *pgRedisStore) checkAndReserveQuota(ctx context.Context, req checkAndReserveQuotaRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.RequestID) == "" {
		return nil, invalidRequest("app_id and request_id are required")
	}

	item, svcErr := s.getTenantAppByID(ctx, req.AppID)
	if svcErr != nil {
		return nil, svcErr
	}
	if item.Status != "active" {
		return nil, forbidden("app is not active")
	}

	dateKey := time.Now().UTC().Format("2006-01-02")
	reservationKey := "reservation:" + req.RequestID
	if existing, err := s.redis.Get(ctx, reservationKey).Result(); err == nil && existing != "" {
		return map[string]any{
			"allowed":               true,
			"request_id":            req.RequestID,
			"policy_key":            req.PolicyKey,
			"remaining_daily_quota": remainingDailyQuota(item.DailyQuota, s.currentDailyUsage(ctx, item.ID, dateKey)),
			"qps_limit":             item.QPSLimit,
			"idempotent":            true,
		}, nil
	}

	secondKey := fmt.Sprintf("qps:%s:%d", item.ID, time.Now().UTC().Unix())
	qpsCount, err := s.redis.Incr(ctx, secondKey).Result()
	if err != nil {
		return nil, internalError("check qps failed")
	}
	if qpsCount == 1 {
		_ = s.redis.Expire(ctx, secondKey, 2*time.Second).Err()
	}
	if item.QPSLimit > 0 && qpsCount > int64(item.QPSLimit) {
		return nil, rateLimited("qps limit exceeded")
	}

	quotaKey := fmt.Sprintf("quota:%s:%s", item.ID, dateKey)
	dailyUsage, err := s.redis.Incr(ctx, quotaKey).Result()
	if err != nil {
		return nil, internalError("check daily quota failed")
	}
	if dailyUsage == 1 {
		_ = s.redis.Expire(ctx, quotaKey, 48*time.Hour).Err()
	}
	if item.DailyQuota > 0 && dailyUsage > item.DailyQuota {
		_, _ = s.redis.Decr(ctx, quotaKey).Result()
		return nil, rateLimited("daily quota exceeded")
	}

	_, err = s.redis.Set(ctx, reservationKey, item.ID+"|"+dateKey, 36*time.Hour).Result()
	if err != nil {
		return nil, internalError("store reservation failed")
	}

	return map[string]any{
		"allowed":               true,
		"request_id":            req.RequestID,
		"policy_key":            req.PolicyKey,
		"remaining_daily_quota": remainingDailyQuota(item.DailyQuota, dailyUsage),
		"qps_limit":             item.QPSLimit,
	}, nil
}

func (s *pgRedisStore) releaseQuotaOnFailure(ctx context.Context, req releaseQuotaOnFailureRequest) (any, *serviceError) {
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.RequestID) == "" {
		return nil, invalidRequest("app_id and request_id are required")
	}

	reservationKey := "reservation:" + req.RequestID
	existing, err := s.redis.Get(ctx, reservationKey).Result()
	if err == redis.Nil || existing == "" {
		return map[string]any{
			"released":   false,
			"request_id": req.RequestID,
		}, nil
	}
	if err != nil {
		return nil, internalError("load reservation failed")
	}

	parts := strings.Split(existing, "|")
	if len(parts) != 2 || parts[0] != req.AppID {
		return nil, conflict("request_id does not belong to app")
	}

	quotaKey := fmt.Sprintf("quota:%s:%s", req.AppID, parts[1])
	_, _ = s.redis.Decr(ctx, quotaKey).Result()
	_, _ = s.redis.Del(ctx, reservationKey).Result()

	return map[string]any{
		"released":   true,
		"request_id": req.RequestID,
	}, nil
}

func (s *pgRedisStore) recordUsageStat(ctx context.Context, req recordUsageStatRequest) (any, *serviceError) {
	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.PolicyKey) == "" {
		return nil, invalidRequest("tenant_id, app_id and policy_key are required")
	}

	bucketStart := currentUsageStatsBucketStart(time.Now().UTC())
	statsRedisKey := usageStatsRedisKey(bucketStart, req.TenantID, req.AppID, req.PolicyKey)
	successCount := int64(0)
	failureCount := int64(1)
	if req.Success {
		successCount = 1
		failureCount = 0
	}

	pipe := s.redis.TxPipeline()
	pipe.HSet(ctx, statsRedisKey,
		"bucket_start_unix", bucketStart.Unix(),
		"tenant_id", req.TenantID,
		"app_id", req.AppID,
		"policy_key", req.PolicyKey,
	)
	pipe.HIncrBy(ctx, statsRedisKey, "total_count", 1)
	pipe.HIncrBy(ctx, statsRedisKey, "success_count", successCount)
	pipe.HIncrBy(ctx, statsRedisKey, "failure_count", failureCount)
	pipe.HIncrBy(ctx, statsRedisKey, "duration_sum_ms", req.DurationMS)
	pipe.HIncrBy(ctx, statsRedisKey, "upstream_duration_sum_ms", req.UpstreamDurationMS)
	pipe.Expire(ctx, statsRedisKey, usageStatsRedisTTL)
	pipe.ZAdd(ctx, usageStatsPendingSetKey, redis.Z{
		Score:  float64(bucketStart.Unix()),
		Member: statsRedisKey,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, internalError("write usage stat cache failed")
	}

	_, _ = s.redis.Del(ctx, "reservation:"+req.RequestID).Result()

	return map[string]any{
		"bucket_start":             bucketStart,
		"tenant_id":                req.TenantID,
		"app_id":                   req.AppID,
		"policy_key":               req.PolicyKey,
		"total_count":              int64(1),
		"success_count":            successCount,
		"failure_count":            failureCount,
		"duration_sum_ms":          req.DurationMS,
		"upstream_duration_sum_ms": req.UpstreamDurationMS,
	}, nil
}

func (s *pgRedisStore) queryUsageStats(ctx context.Context, req queryUsageStatsRequest) (any, *serviceError) {
	if err := s.flushDueUsageStats(ctx, time.Now().UTC()); err != nil {
		s.logger.Error("flush usage stats before query failed", map[string]any{"error": err.Error()})
	}

	items, svcErr := s.loadPersistedUsageStats(ctx, req)
	if svcErr != nil {
		return nil, svcErr
	}

	pendingItems, pendingErr := s.loadPendingUsageStats(ctx, req, time.Now().UTC())
	if pendingErr != nil {
		return nil, pendingErr
	}

	merged := mergeUsageStats(items, pendingItems)
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].BucketStart.Equal(merged[j].BucketStart) {
			if merged[i].TenantID == merged[j].TenantID {
				if merged[i].AppID == merged[j].AppID {
					return merged[i].PolicyKey < merged[j].PolicyKey
				}
				return merged[i].AppID < merged[j].AppID
			}
			return merged[i].TenantID < merged[j].TenantID
		}
		return merged[i].BucketStart.Before(merged[j].BucketStart)
	})

	return map[string]any{"items": merged}, nil
}

func (s *pgRedisStore) loadPersistedUsageStats(ctx context.Context, req queryUsageStatsRequest) ([]usageStat, *serviceError) {
	query := `
		SELECT bucket_start, tenant_id, app_id, policy_key, total_count, success_count, failure_count, duration_sum_ms, upstream_duration_sum_ms
		FROM access.usage_stats
		WHERE 1=1
	`
	args := make([]any, 0)
	index := 1
	if req.TenantID != "" {
		query += fmt.Sprintf(" AND tenant_id = $%d", index)
		args = append(args, req.TenantID)
		index++
	}
	if req.AppID != "" {
		query += fmt.Sprintf(" AND app_id = $%d", index)
		args = append(args, req.AppID)
		index++
	}
	if req.PolicyKey != "" {
		query += fmt.Sprintf(" AND policy_key = $%d", index)
		args = append(args, req.PolicyKey)
		index++
	}
	if req.StartUnix > 0 {
		query += fmt.Sprintf(" AND bucket_start >= to_timestamp($%d)", index)
		args = append(args, req.StartUnix)
		index++
	}
	if req.EndUnix > 0 {
		query += fmt.Sprintf(" AND bucket_start <= to_timestamp($%d)", index)
		args = append(args, req.EndUnix)
		index++
	}
	query += " ORDER BY bucket_start ASC"

	rows, err := s.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, internalError("query usage stats failed")
	}
	defer rows.Close()

	items := make([]usageStat, 0)
	for rows.Next() {
		var item usageStat
		if err := rows.Scan(&item.BucketStart, &item.TenantID, &item.AppID, &item.PolicyKey, &item.TotalCount, &item.SuccessCount, &item.FailureCount, &item.DurationSumMS, &item.UpstreamDurationSumMS); err != nil {
			return nil, internalError("decode usage stats failed")
		}
		items = append(items, item)
	}

	return items, nil
}

func (s *pgRedisStore) checkIPBan(ctx context.Context, req checkIPBanRequest) (any, *serviceError) {
	if strings.TrimSpace(req.IP) == "" {
		return nil, invalidRequest("ip is required")
	}

	exists, err := s.redis.Exists(ctx, "ipban:"+req.IP).Result()
	if err != nil {
		return nil, internalError("check ip ban failed")
	}

	return map[string]any{
		"ip":      req.IP,
		"blocked": exists > 0,
	}, nil
}

func (s *pgRedisStore) getTenantAppByID(ctx context.Context, appID string) (*tenantApp, *serviceError) {
	if cached, ok := s.loadCachedTenantApp(ctx, appID); ok {
		return &cached, nil
	}

	var item tenantApp
	err := s.pg.QueryRow(ctx, `
		SELECT id, tenant_id, name, app_key, status, daily_quota, qps_limit, created_at, updated_at
		FROM access.tenant_apps
		WHERE id = $1
	`, appID).Scan(&item.ID, &item.TenantID, &item.Name, &item.AppKey, &item.Status, &item.DailyQuota, &item.QPSLimit, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("app not found")
	}
	if err != nil {
		return nil, internalError("query app failed")
	}
	if err := s.cacheTenantApp(ctx, item); err != nil {
		s.logger.Error("cache tenant app snapshot failed", map[string]any{"error": err.Error(), "app_id": item.ID})
	}
	return &item, nil
}

func (v appAuthContextCacheValue) toMap() map[string]any {
	return map[string]any{
		"tenant_id":   v.TenantID,
		"app_id":      v.AppID,
		"app_key":     v.AppKey,
		"app_secret":  v.AppSecret,
		"status":      v.Status,
		"daily_quota": v.DailyQuota,
		"qps_limit":   v.QPSLimit,
	}
}

func (s *pgRedisStore) loadCachedAppAuthContext(ctx context.Context, appKey string) (appAuthContextCacheValue, bool) {
	if s.redis == nil {
		return appAuthContextCacheValue{}, false
	}

	payload, err := s.redis.Get(ctx, s.appAuthContextCacheKey(appKey)).Bytes()
	if err != nil || len(payload) == 0 {
		return appAuthContextCacheValue{}, false
	}

	var value appAuthContextCacheValue
	if err := json.Unmarshal(payload, &value); err != nil {
		_ = s.redis.Del(ctx, s.appAuthContextCacheKey(appKey)).Err()
		return appAuthContextCacheValue{}, false
	}

	return value, true
}

func (s *pgRedisStore) cacheAppAuthContext(ctx context.Context, value appAuthContextCacheValue) error {
	if s.redis == nil {
		return nil
	}
	if strings.TrimSpace(value.AppKey) == "" || strings.TrimSpace(value.AppID) == "" {
		return nil
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}

	pipe := s.redis.TxPipeline()
	ttl := s.appMetadataCacheTTLOrDefault()
	pipe.Set(ctx, s.appAuthContextCacheKey(value.AppKey), payload, ttl)
	pipe.Set(ctx, s.appKeyIndexCacheKey(value.AppID), value.AppKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *pgRedisStore) loadCachedTenantApp(ctx context.Context, appID string) (tenantApp, bool) {
	if s.redis == nil {
		return tenantApp{}, false
	}

	payload, err := s.redis.Get(ctx, s.appSnapshotCacheKey(appID)).Bytes()
	if err != nil || len(payload) == 0 {
		return tenantApp{}, false
	}

	var item tenantApp
	if err := json.Unmarshal(payload, &item); err != nil {
		_ = s.redis.Del(ctx, s.appSnapshotCacheKey(appID)).Err()
		return tenantApp{}, false
	}

	return item, true
}

func (s *pgRedisStore) cacheTenantApp(ctx context.Context, item tenantApp) error {
	if s.redis == nil {
		return nil
	}
	if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.AppKey) == "" {
		return nil
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}

	pipe := s.redis.TxPipeline()
	ttl := s.appMetadataCacheTTLOrDefault()
	pipe.Set(ctx, s.appSnapshotCacheKey(item.ID), payload, ttl)
	pipe.Set(ctx, s.appKeyIndexCacheKey(item.ID), item.AppKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *pgRedisStore) invalidateTenantAppCache(ctx context.Context, appID string) error {
	if s.redis == nil {
		return nil
	}
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}

	keys := []string{
		s.appSnapshotCacheKey(appID),
		s.appKeyIndexCacheKey(appID),
	}

	appKey, err := s.redis.Get(ctx, s.appKeyIndexCacheKey(appID)).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if strings.TrimSpace(appKey) != "" {
		keys = append(keys, s.appAuthContextCacheKey(appKey))
	}

	return s.redis.Del(ctx, keys...).Err()
}

func (s *pgRedisStore) getTenantAppSnapshot(ctx context.Context, appID string) (any, *serviceError) {
	item, svcErr := s.getTenantAppByID(ctx, appID)
	if svcErr != nil {
		return nil, svcErr
	}
	return item, nil
}

func appAuthContextCacheKey(appKey string) string {
	return buildAppAuthContextCacheKey(defaultAppMetadataCachePrefix, appKey)
}

func appSnapshotCacheKey(appID string) string {
	return buildAppSnapshotCacheKey(defaultAppMetadataCachePrefix, appID)
}

func appKeyIndexCacheKey(appID string) string {
	return buildAppKeyIndexCacheKey(defaultAppMetadataCachePrefix, appID)
}

func (s *pgRedisStore) appMetadataCacheTTLOrDefault() time.Duration {
	if s.appMetadataCacheTTL > 0 {
		return s.appMetadataCacheTTL
	}
	return defaultAppMetadataCacheTTL
}

func (s *pgRedisStore) appAuthContextCacheKey(appKey string) string {
	return buildAppAuthContextCacheKey(s.appMetadataCachePrefix, appKey)
}

func (s *pgRedisStore) appSnapshotCacheKey(appID string) string {
	return buildAppSnapshotCacheKey(s.appMetadataCachePrefix, appID)
}

func (s *pgRedisStore) appKeyIndexCacheKey(appID string) string {
	return buildAppKeyIndexCacheKey(s.appMetadataCachePrefix, appID)
}

func buildAppAuthContextCacheKey(prefix, appKey string) string {
	return strings.TrimSpace(prefix) + appAuthContextCachePrefix + strings.TrimSpace(appKey)
}

func buildAppSnapshotCacheKey(prefix, appID string) string {
	return strings.TrimSpace(prefix) + appSnapshotCachePrefix + strings.TrimSpace(appID)
}

func buildAppKeyIndexCacheKey(prefix, appID string) string {
	return strings.TrimSpace(prefix) + appKeyIndexCachePrefix + strings.TrimSpace(appID)
}

func (s *pgRedisStore) currentDailyUsage(ctx context.Context, appID, dateKey string) int64 {
	value, err := s.redis.Get(ctx, fmt.Sprintf("quota:%s:%s", appID, dateKey)).Int64()
	if err != nil {
		return 0
	}
	return value
}

func (s *pgRedisStore) runUsageStatsFlusher(ctx context.Context, interval time.Duration) {
	defer close(s.flusherDone)
	if interval <= 0 {
		interval = defaultUsageStatsFlushInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.flushDueUsageStats(ctx, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("flush due usage stats failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

func (s *pgRedisStore) flushDueUsageStats(ctx context.Context, now time.Time) error {
	cutoffUnix := closedUsageStatsCutoff(now)
	if cutoffUnix <= 0 {
		return nil
	}

	lockToken := shared.NewSecret("usage_flush_lock")
	locked, err := s.redis.SetNX(ctx, usageStatsFlushLockKey, lockToken, usageStatsFlushLockTTL).Result()
	if err != nil {
		return fmt.Errorf("acquire usage stats flush lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer func() {
		_ = releaseUsageStatsFlushLock(ctx, s.redis, lockToken)
	}()

	members, err := s.redis.ZRangeByScore(ctx, usageStatsPendingSetKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(cutoffUnix, 10),
	}).Result()
	if err != nil {
		return fmt.Errorf("list pending usage stats: %w", err)
	}

	for _, member := range members {
		if err := s.flushUsageStatsMember(ctx, member); err != nil {
			s.logger.Error("flush usage stats member failed", map[string]any{
				"error":       err.Error(),
				"stats_key":   member,
				"cutoff_unix": cutoffUnix,
			})
		}
	}

	return nil
}

func releaseUsageStatsFlushLock(ctx context.Context, client *redis.Client, token string) error {
	if client == nil || strings.TrimSpace(token) == "" {
		return nil
	}
	return releaseUsageStatsFlushLockScript.Run(ctx, client, []string{usageStatsFlushLockKey}, token).Err()
}

func (s *pgRedisStore) flushUsageStatsMember(ctx context.Context, statsKey string) error {
	fields, err := s.redis.HGetAll(ctx, statsKey).Result()
	if err != nil {
		return fmt.Errorf("load usage stats bucket: %w", err)
	}
	if len(fields) == 0 {
		_, _ = s.redis.ZRem(ctx, usageStatsPendingSetKey, statsKey).Result()
		return nil
	}

	item, err := decodeUsageStatFields(fields)
	if err != nil {
		return fmt.Errorf("decode usage stats bucket: %w", err)
	}

	_, err = s.pg.Exec(ctx, `
		INSERT INTO access.usage_stats (
			bucket_start, tenant_id, app_id, policy_key,
			total_count, success_count, failure_count,
			duration_sum_ms, upstream_duration_sum_ms,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
		ON CONFLICT (bucket_start, tenant_id, app_id, policy_key)
		DO UPDATE SET
			total_count = access.usage_stats.total_count + EXCLUDED.total_count,
			success_count = access.usage_stats.success_count + EXCLUDED.success_count,
			failure_count = access.usage_stats.failure_count + EXCLUDED.failure_count,
			duration_sum_ms = access.usage_stats.duration_sum_ms + EXCLUDED.duration_sum_ms,
			upstream_duration_sum_ms = access.usage_stats.upstream_duration_sum_ms + EXCLUDED.upstream_duration_sum_ms,
			updated_at = NOW()
	`, item.BucketStart, item.TenantID, item.AppID, item.PolicyKey, item.TotalCount, item.SuccessCount, item.FailureCount, item.DurationSumMS, item.UpstreamDurationSumMS)
	if err != nil {
		return fmt.Errorf("persist usage stats bucket: %w", err)
	}

	pipe := s.redis.TxPipeline()
	pipe.Del(ctx, statsKey)
	pipe.ZRem(ctx, usageStatsPendingSetKey, statsKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cleanup flushed usage stats bucket: %w", err)
	}

	return nil
}

func (s *pgRedisStore) loadPendingUsageStats(ctx context.Context, req queryUsageStatsRequest, now time.Time) ([]usageStat, *serviceError) {
	currentBucketUnix := currentUsageStatsBucketStart(now).Unix()
	minUnix := currentBucketUnix
	if req.StartUnix > minUnix {
		minUnix = req.StartUnix
	}
	if req.EndUnix > 0 && req.EndUnix < minUnix {
		return []usageStat{}, nil
	}

	rangeBy := &redis.ZRangeBy{
		Min: strconv.FormatInt(minUnix, 10),
		Max: "+inf",
	}
	if req.EndUnix > 0 {
		rangeBy.Max = strconv.FormatInt(req.EndUnix, 10)
	}

	members, err := s.redis.ZRangeByScore(ctx, usageStatsPendingSetKey, rangeBy).Result()
	if err != nil {
		return nil, internalError("query pending usage stats failed")
	}

	items := make([]usageStat, 0, len(members))
	for _, member := range members {
		fields, err := s.redis.HGetAll(ctx, member).Result()
		if err != nil || len(fields) == 0 {
			continue
		}

		item, err := decodeUsageStatFields(fields)
		if err != nil {
			continue
		}
		if req.TenantID != "" && item.TenantID != req.TenantID {
			continue
		}
		if req.AppID != "" && item.AppID != req.AppID {
			continue
		}
		if req.PolicyKey != "" && item.PolicyKey != req.PolicyKey {
			continue
		}
		if req.StartUnix > 0 && item.BucketStart.Unix() < req.StartUnix {
			continue
		}
		if req.EndUnix > 0 && item.BucketStart.Unix() > req.EndUnix {
			continue
		}

		items = append(items, item)
	}

	return items, nil
}

func usageStatsRedisKey(bucketStart time.Time, tenantID, appID, policyKey string) string {
	return fmt.Sprintf("stats:%d:%s:%s:%s", bucketStart.Unix(), tenantID, appID, policyKey)
}

func mustEncryptAccessSecret(key []byte, plaintext string) string {
	ciphertext, err := shared.EncryptSecretValue(key, plaintext)
	if err != nil {
		panic(err)
	}
	return ciphertext
}

func currentUsageStatsBucketStart(now time.Time) time.Time {
	return now.UTC().Truncate(usageStatsBucketSize)
}

func closedUsageStatsCutoff(now time.Time) int64 {
	return currentUsageStatsBucketStart(now).Add(-usageStatsBucketSize).Unix()
}

func decodeUsageStatFields(fields map[string]string) (usageStat, error) {
	bucketUnix, err := strconv.ParseInt(fields["bucket_start_unix"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}
	totalCount, err := strconv.ParseInt(fields["total_count"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}
	successCount, err := strconv.ParseInt(fields["success_count"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}
	failureCount, err := strconv.ParseInt(fields["failure_count"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}
	durationSumMS, err := strconv.ParseInt(fields["duration_sum_ms"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}
	upstreamDurationSumMS, err := strconv.ParseInt(fields["upstream_duration_sum_ms"], 10, 64)
	if err != nil {
		return usageStat{}, err
	}

	return usageStat{
		BucketStart:           time.Unix(bucketUnix, 0).UTC(),
		TenantID:              fields["tenant_id"],
		AppID:                 fields["app_id"],
		PolicyKey:             fields["policy_key"],
		TotalCount:            totalCount,
		SuccessCount:          successCount,
		FailureCount:          failureCount,
		DurationSumMS:         durationSumMS,
		UpstreamDurationSumMS: upstreamDurationSumMS,
	}, nil
}

func mergeUsageStats(groups ...[]usageStat) []usageStat {
	itemsByKey := make(map[string]usageStat)
	for _, group := range groups {
		for _, item := range group {
			key := fmt.Sprintf("%d|%s|%s|%s", item.BucketStart.Unix(), item.TenantID, item.AppID, item.PolicyKey)
			existing, ok := itemsByKey[key]
			if !ok {
				itemsByKey[key] = item
				continue
			}

			existing.TotalCount += item.TotalCount
			existing.SuccessCount += item.SuccessCount
			existing.FailureCount += item.FailureCount
			existing.DurationSumMS += item.DurationSumMS
			existing.UpstreamDurationSumMS += item.UpstreamDurationSumMS
			itemsByKey[key] = existing
		}
	}

	merged := make([]usageStat, 0, len(itemsByKey))
	for _, item := range itemsByKey {
		merged = append(merged, item)
	}
	return merged
}

func internalError(message string) *serviceError {
	return &serviceError{statusCode: 500, code: 100500, message: message}
}

func absFloat64(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

const accessInitSQL = `
CREATE SCHEMA IF NOT EXISTS access;

CREATE TABLE IF NOT EXISTS access.tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.console_users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'platform_admin',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.secret_materials (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    secret_ciphertext TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.plan_templates (
    id TEXT PRIMARY KEY,
    plan_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    daily_quota BIGINT NOT NULL DEFAULT 0,
    qps_limit INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS access.tenant_apps (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES access.tenants(id),
    name TEXT NOT NULL,
    app_key TEXT NOT NULL UNIQUE,
    secret_material_id TEXT REFERENCES access.secret_materials(id),
    daily_quota BIGINT NOT NULL DEFAULT 0,
    qps_limit INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_apps_tenant_id ON access.tenant_apps (tenant_id);

CREATE TABLE IF NOT EXISTS access.usage_stats (
    bucket_start TIMESTAMPTZ NOT NULL,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    policy_key TEXT NOT NULL,
    total_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    duration_sum_ms BIGINT NOT NULL DEFAULT 0,
    upstream_duration_sum_ms BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_start, tenant_id, app_id, policy_key)
);

CREATE INDEX IF NOT EXISTS idx_usage_stats_tenant_app_policy
    ON access.usage_stats (tenant_id, app_id, policy_key, bucket_start DESC);
`
