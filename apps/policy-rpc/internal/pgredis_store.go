package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

const policyCacheTTL = 10 * time.Minute

type policyStoreConfig struct {
	Backend         string
	PostgresDSN     string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	SecretMasterKey string
	ProviderAPIKey  string
}

type pgRedisPolicyStore struct {
	logger          *xlog.Logger
	pg              *pgxpool.Pool
	redis           *redis.Client
	secretMasterKey []byte
}

type policyRowScanner interface {
	Scan(dest ...any) error
}

func loadPolicyStoreConfig() policyStoreConfig {
	cfg := policyStoreConfig{
		Backend: strings.ToLower(strings.TrimSpace(firstNonEmpty(
			os.Getenv("POLICY_RPC_STORE_BACKEND"),
			os.Getenv("COMMON_STORE_BACKEND"),
		))),
		PostgresDSN: strings.TrimSpace(firstNonEmpty(
			os.Getenv("POLICY_RPC_POSTGRES_DSN"),
			os.Getenv("COMMON_POSTGRES_DSN"),
		)),
		RedisAddr: strings.TrimSpace(firstNonEmpty(
			os.Getenv("POLICY_RPC_REDIS_ADDR"),
			os.Getenv("COMMON_REDIS_ADDR"),
		)),
		RedisPassword: firstNonEmpty(
			os.Getenv("POLICY_RPC_REDIS_PASSWORD"),
			os.Getenv("COMMON_REDIS_PASSWORD"),
		),
		SecretMasterKey: strings.TrimSpace(os.Getenv("COMMON_SECRET_MASTER_KEY")),
		ProviderAPIKey:  strings.TrimSpace(os.Getenv("POLICY_RPC_PROVIDER_API_KEY")),
	}

	if redisDBValue := strings.TrimSpace(firstNonEmpty(
		os.Getenv("POLICY_RPC_REDIS_DB"),
		os.Getenv("COMMON_REDIS_DB"),
	)); redisDBValue != "" {
		if parsed, err := strconv.Atoi(redisDBValue); err == nil {
			cfg.RedisDB = parsed
		}
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

func newPGRedisPolicyStore(cfg policyStoreConfig, logger *xlog.Logger) (*pgRedisPolicyStore, error) {
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

	store := &pgRedisPolicyStore{
		logger:          logger,
		pg:              pool,
		redis:           redisClient,
		secretMasterKey: secretMasterKey,
	}

	if err := store.ensureSchema(ctx); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}
	if err := store.seed(ctx, cfg.ProviderAPIKey); err != nil {
		pool.Close()
		_ = redisClient.Close()
		return nil, err
	}

	return store, nil
}

func (s *pgRedisPolicyStore) Close(context.Context) error {
	if s.pg != nil {
		s.pg.Close()
	}
	if s.redis != nil {
		return s.redis.Close()
	}
	return nil
}

func (s *pgRedisPolicyStore) ensureSchema(ctx context.Context) error {
	if _, err := s.pg.Exec(ctx, policyInitSQL); err != nil {
		return fmt.Errorf("ensure policy schema: %w", err)
	}
	return nil
}

func (s *pgRedisPolicyStore) seed(ctx context.Context, providerAPIKey string) error {
	if strings.TrimSpace(providerAPIKey) == "" {
		s.logger.Info("policy-rpc default provider credential bootstrap disabled", map[string]any{
			"reason": "missing POLICY_RPC_PROVIDER_API_KEY",
		})
		return nil
	}

	credential := providerCredential{
		ID:           "provider_credential_fapi_uk",
		ProviderName: "fapi.uk",
		DisplayName:  "Fapi.uk Default",
		APIKey:       providerAPIKey,
		Status:       "active",
	}

	_, err := s.pg.Exec(ctx, `
		INSERT INTO policy.provider_credentials (
			id, provider_name, credential_name, api_key_ciphertext, status, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
		ON CONFLICT (id) DO UPDATE
		SET provider_name = EXCLUDED.provider_name,
		    credential_name = EXCLUDED.credential_name,
		    api_key_ciphertext = EXCLUDED.api_key_ciphertext,
		    status = EXCLUDED.status,
		    updated_at = NOW()
	`, credential.ID, credential.ProviderName, credential.DisplayName, mustEncryptPolicySecret(s.secretMasterKey, credential.APIKey), credential.Status)
	if err != nil {
		return fmt.Errorf("seed provider credential: %w", err)
	}

	for _, item := range defaultPolicies(credential.ID) {
		_, err := s.pg.Exec(ctx, `
			INSERT INTO policy.policy_definitions (
				policy_key, display_name, public_method, public_path,
				upstream_method, upstream_path, allowed_params, required_params, denied_params,
				default_params, provider_credential_id, status, version,
				created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11, $12, $13, NOW(), NOW())
			ON CONFLICT (policy_key) DO UPDATE
			SET display_name = EXCLUDED.display_name,
			    public_method = EXCLUDED.public_method,
			    public_path = EXCLUDED.public_path,
			    upstream_method = EXCLUDED.upstream_method,
			    upstream_path = EXCLUDED.upstream_path,
			    allowed_params = EXCLUDED.allowed_params,
			    required_params = EXCLUDED.required_params,
			    denied_params = EXCLUDED.denied_params,
			    default_params = EXCLUDED.default_params,
			    provider_credential_id = EXCLUDED.provider_credential_id,
			    status = EXCLUDED.status,
			    updated_at = NOW()
		`, item.PolicyKey, item.DisplayName, item.PublicMethod, item.PublicPath, item.UpstreamMethod, item.UpstreamPath, mustJSONString(item.AllowedParams), mustJSONString(item.RequiredParams), mustJSONString(item.DeniedParams), mustJSONString(item.DefaultParams), item.ProviderCredentialID, item.Status, item.Version)
		if err != nil {
			return fmt.Errorf("seed policy %s: %w", item.PolicyKey, err)
		}
	}

	s.logger.Info("policy-rpc connected to postgres/redis backend", map[string]any{
		"default_policy_count": len(defaultPolicies(credential.ID)),
		"provider_name":        credential.ProviderName,
	})

	return nil
}

func (s *pgRedisPolicyStore) resolvePolicy(ctx context.Context, req resolvePolicyRequest) (any, *policyServiceError) {
	if strings.TrimSpace(req.Path) == "" {
		return nil, policyInvalidRequest("path is required")
	}

	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = "GET"
	}

	cacheKey := policyRouteCacheKey(method, req.Path)
	if cached, err := s.redis.Get(ctx, cacheKey).Result(); err == nil && cached != "" {
		var payload map[string]any
		if json.Unmarshal([]byte(cached), &payload) == nil {
			return payload, nil
		}
	}

	query := `
		SELECT
			p.policy_key,
			p.display_name,
			p.public_method,
			p.public_path,
			p.upstream_method,
			p.upstream_path,
			p.allowed_params,
			p.required_params,
			p.denied_params,
			p.default_params,
			p.provider_credential_id,
			p.status,
			p.version,
			c.provider_name,
			c.api_key_ciphertext
		FROM policy.policy_definitions p
		JOIN policy.provider_credentials c ON c.id = p.provider_credential_id
		WHERE p.public_method = $1
		  AND p.public_path = $2
		  AND p.status = 'published'
		  AND c.status = 'active'
	`

	row := s.pg.QueryRow(ctx, query, method, req.Path)
	policy, providerName, providerAPIKey, err := scanResolvedPolicyRowWithKey(row, s.secretMasterKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, policyNotFound("policy not found for path")
	}
	if err != nil {
		s.logger.Error("resolve policy failed", map[string]any{
			"error":  err.Error(),
			"path":   req.Path,
			"method": method,
		})
		return nil, policyInternalError("resolve policy failed")
	}

	payload := map[string]any{
		"policy_key":       policy.PolicyKey,
		"display_name":     policy.DisplayName,
		"public_method":    policy.PublicMethod,
		"public_path":      policy.PublicPath,
		"upstream_method":  policy.UpstreamMethod,
		"upstream_path":    policy.UpstreamPath,
		"allowed_params":   policy.AllowedParams,
		"required_params":  policy.RequiredParams,
		"denied_params":    policy.DeniedParams,
		"default_params":   policy.DefaultParams,
		"provider_name":    providerName,
		"provider_api_key": providerAPIKey,
		"version":          policy.Version,
		"status":           policy.Status,
	}

	if encoded, err := json.Marshal(payload); err == nil {
		_ = s.redis.Set(ctx, cacheKey, encoded, policyCacheTTL).Err()
	}

	return payload, nil
}

func (s *pgRedisPolicyStore) checkAppPolicyAccess(ctx context.Context, req checkAppPolicyAccessRequest) (any, *policyServiceError) {
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.PolicyKey) == "" {
		return nil, policyInvalidRequest("app_id and policy_key are required")
	}

	cacheKey := appPolicyCacheKey(req.AppID)
	if exists, err := s.redis.Exists(ctx, cacheKey).Result(); err == nil && exists > 0 {
		allowed, redisErr := s.redis.HExists(ctx, cacheKey, req.PolicyKey).Result()
		if redisErr == nil {
			return map[string]any{
				"app_id":     req.AppID,
				"policy_key": req.PolicyKey,
				"allowed":    allowed,
			}, nil
		}
	}

	rows, err := s.pg.Query(ctx, `
		SELECT policy_key
		FROM policy.app_policy_bindings
		WHERE app_id = $1 AND status = 'enabled'
	`, req.AppID)
	if err != nil {
		s.logger.Error("query app policy bindings failed", map[string]any{
			"error":  err.Error(),
			"app_id": req.AppID,
		})
		return nil, policyInternalError("check app policy access failed")
	}
	defer rows.Close()

	policyKeys := make([]string, 0)
	allowed := false
	for rows.Next() {
		var policyKey string
		if err := rows.Scan(&policyKey); err != nil {
			return nil, policyInternalError("decode app policy bindings failed")
		}
		policyKeys = append(policyKeys, policyKey)
		if policyKey == req.PolicyKey {
			allowed = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, policyInternalError("read app policy bindings failed")
	}

	s.refreshAppPolicyCache(ctx, req.AppID, policyKeys)

	return map[string]any{
		"app_id":     req.AppID,
		"policy_key": req.PolicyKey,
		"allowed":    allowed,
	}, nil
}

func (s *pgRedisPolicyStore) listPolicies(ctx context.Context) (any, *policyServiceError) {
	rows, err := s.pg.Query(ctx, `
		SELECT
			policy_key,
			display_name,
			public_method,
			public_path,
			upstream_method,
			upstream_path,
			allowed_params,
			required_params,
			denied_params,
			default_params,
			provider_credential_id,
			status,
			version
		FROM policy.policy_definitions
		ORDER BY public_path ASC, policy_key ASC
	`)
	if err != nil {
		s.logger.Error("list policies failed", map[string]any{"error": err.Error()})
		return nil, policyInternalError("list policies failed")
	}
	defer rows.Close()

	items := make([]policyDefinition, 0)
	for rows.Next() {
		item, scanErr := scanPolicyDefinition(rows)
		if scanErr != nil {
			return nil, policyInternalError("decode policies failed")
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, policyInternalError("read policies failed")
	}

	return map[string]any{"items": items}, nil
}

func (s *pgRedisPolicyStore) publishPolicyConfig(ctx context.Context, req publishPolicyConfigRequest) (any, *policyServiceError) {
	if strings.TrimSpace(req.PolicyKey) == "" || strings.TrimSpace(req.DisplayName) == "" {
		return nil, policyInvalidRequest("policy_key and display_name are required")
	}
	if strings.TrimSpace(req.PublicPath) == "" || strings.TrimSpace(req.UpstreamPath) == "" {
		return nil, policyInvalidRequest("public_path and upstream_path are required")
	}
	if strings.TrimSpace(req.ProviderCredentialID) == "" {
		return nil, policyInvalidRequest("provider_credential_id is required")
	}

	publicMethod := strings.ToUpper(firstNonEmpty(req.PublicMethod, "GET"))
	upstreamMethod := strings.ToUpper(firstNonEmpty(req.UpstreamMethod, "GET"))

	var credentialExists bool
	if err := s.pg.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM policy.provider_credentials
			WHERE id = $1 AND status = 'active'
		)
	`, req.ProviderCredentialID).Scan(&credentialExists); err != nil {
		s.logger.Error("validate provider credential failed", map[string]any{
			"error":                  err.Error(),
			"provider_credential_id": req.ProviderCredentialID,
		})
		return nil, policyInternalError("validate provider credential failed")
	}
	if !credentialExists {
		return nil, policyNotFound("provider credential not found")
	}

	var oldMethod, oldPath string
	oldRouteErr := s.pg.QueryRow(ctx, `
		SELECT public_method, public_path
		FROM policy.policy_definitions
		WHERE policy_key = $1
	`, req.PolicyKey).Scan(&oldMethod, &oldPath)
	if oldRouteErr != nil && !errors.Is(oldRouteErr, pgx.ErrNoRows) {
		return nil, policyInternalError("load existing policy failed")
	}

	row := s.pg.QueryRow(ctx, `
		INSERT INTO policy.policy_definitions (
			policy_key, display_name, public_method, public_path,
			upstream_method, upstream_path, allowed_params, required_params, denied_params,
			default_params, provider_credential_id, status, version,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11, 'published', 1, NOW(), NOW())
		ON CONFLICT (policy_key) DO UPDATE
		SET display_name = EXCLUDED.display_name,
		    public_method = EXCLUDED.public_method,
		    public_path = EXCLUDED.public_path,
		    upstream_method = EXCLUDED.upstream_method,
		    upstream_path = EXCLUDED.upstream_path,
		    allowed_params = EXCLUDED.allowed_params,
		    required_params = EXCLUDED.required_params,
		    denied_params = EXCLUDED.denied_params,
		    default_params = EXCLUDED.default_params,
		    provider_credential_id = EXCLUDED.provider_credential_id,
		    status = EXCLUDED.status,
		    version = policy.policy_definitions.version + 1,
		    updated_at = NOW()
		RETURNING
			policy_key,
			display_name,
			public_method,
			public_path,
			upstream_method,
			upstream_path,
			allowed_params,
			required_params,
			denied_params,
			default_params,
			provider_credential_id,
			status,
			version
	`, req.PolicyKey, req.DisplayName, publicMethod, req.PublicPath, upstreamMethod, req.UpstreamPath, mustJSONString(req.AllowedParams), mustJSONString(req.RequiredParams), mustJSONString(req.DeniedParams), mustJSONString(req.DefaultParams), req.ProviderCredentialID)

	item, err := scanPolicyDefinition(row)
	if err != nil {
		s.logger.Error("publish policy config failed", map[string]any{
			"error":      err.Error(),
			"policy_key": req.PolicyKey,
		})
		return nil, policyInternalError("publish policy config failed")
	}

	s.invalidateRouteCache(ctx, publicMethod, req.PublicPath)
	if oldPath != "" {
		s.invalidateRouteCache(ctx, oldMethod, oldPath)
	}

	return item, nil
}

func (s *pgRedisPolicyStore) bindAppPolicies(ctx context.Context, req bindAppPoliciesRequest) (any, *policyServiceError) {
	if strings.TrimSpace(req.AppID) == "" {
		return nil, policyInvalidRequest("app_id is required")
	}

	uniqueKeys := uniquePolicyKeys(req.PolicyKeys)
	tx, err := s.pg.Begin(ctx)
	if err != nil {
		s.logger.Error("begin bind app policies transaction failed", map[string]any{
			"error":  err.Error(),
			"app_id": req.AppID,
		})
		return nil, policyInternalError("bind app policies failed")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, policyKey := range uniqueKeys {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM policy.policy_definitions
				WHERE policy_key = $1 AND status = 'published'
			)
		`, policyKey).Scan(&exists); err != nil {
			return nil, policyInternalError("validate policy binding failed")
		}
		if !exists {
			return nil, policyNotFound("policy not found: " + policyKey)
		}
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM policy.app_policy_bindings
		WHERE app_id = $1
	`, req.AppID); err != nil {
		return nil, policyInternalError("clear app policy bindings failed")
	}

	for _, policyKey := range uniqueKeys {
		if _, err := tx.Exec(ctx, `
			INSERT INTO policy.app_policy_bindings (
				app_id, policy_key, status, created_at, updated_at
			)
			VALUES ($1, $2, 'enabled', NOW(), NOW())
		`, req.AppID, policyKey); err != nil {
			return nil, policyInternalError("write app policy bindings failed")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, policyInternalError("commit app policy bindings failed")
	}

	s.refreshAppPolicyCache(ctx, req.AppID, uniqueKeys)

	return map[string]any{
		"app_id":      req.AppID,
		"policy_keys": uniqueKeys,
	}, nil
}

func (s *pgRedisPolicyStore) refreshAppPolicyCache(ctx context.Context, appID string, policyKeys []string) {
	if strings.TrimSpace(appID) == "" {
		return
	}

	cacheKey := appPolicyCacheKey(appID)
	fields := make([]any, 0, len(policyKeys)*2+2)
	fields = append(fields, "_loaded", "1")
	for _, policyKey := range uniquePolicyKeys(policyKeys) {
		fields = append(fields, policyKey, "1")
	}

	pipe := s.redis.TxPipeline()
	pipe.Del(ctx, cacheKey)
	pipe.HSet(ctx, cacheKey, fields...)
	pipe.Expire(ctx, cacheKey, policyCacheTTL)
	_, _ = pipe.Exec(ctx)
}

func (s *pgRedisPolicyStore) invalidateRouteCache(ctx context.Context, method, path string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	_ = s.redis.Del(ctx, policyRouteCacheKey(method, path)).Err()
}

func scanResolvedPolicyRow(row policyRowScanner) (*policyDefinition, string, string, error) {
	keyProvider, ok := row.(interface{ SecretMasterKey() []byte })
	if !ok {
		return nil, "", "", shared.ErrSecretMasterKeyRequired
	}
	return scanResolvedPolicyRowWithKey(row, keyProvider.SecretMasterKey())
}

func scanResolvedPolicyRowWithKey(row policyRowScanner, secretMasterKey []byte) (*policyDefinition, string, string, error) {
	var (
		item              policyDefinition
		allowedParamsRaw  []byte
		requiredParamsRaw []byte
		deniedParamsRaw   []byte
		defaultParamsRaw  []byte
		providerName      string
		providerAPIKey    string
	)

	err := row.Scan(
		&item.PolicyKey,
		&item.DisplayName,
		&item.PublicMethod,
		&item.PublicPath,
		&item.UpstreamMethod,
		&item.UpstreamPath,
		&allowedParamsRaw,
		&requiredParamsRaw,
		&deniedParamsRaw,
		&defaultParamsRaw,
		&item.ProviderCredentialID,
		&item.Status,
		&item.Version,
		&providerName,
		&providerAPIKey,
	)
	if err != nil {
		return nil, "", "", err
	}

	item.AllowedParams = decodeJSONStringSlice(allowedParamsRaw)
	item.RequiredParams = decodeJSONStringSlice(requiredParamsRaw)
	item.DeniedParams = decodeJSONStringSlice(deniedParamsRaw)
	item.DefaultParams = decodeJSONStringMap(defaultParamsRaw)
	providerAPIKey, err = shared.DecryptSecretValue(secretMasterKey, providerAPIKey)
	if err != nil {
		return nil, "", "", err
	}
	return &item, providerName, providerAPIKey, nil
}

func mustEncryptPolicySecret(key []byte, plaintext string) string {
	ciphertext, err := shared.EncryptSecretValue(key, plaintext)
	if err != nil {
		panic(err)
	}
	return ciphertext
}

func scanPolicyDefinition(row policyRowScanner) (*policyDefinition, error) {
	var (
		item              policyDefinition
		allowedParamsRaw  []byte
		requiredParamsRaw []byte
		deniedParamsRaw   []byte
		defaultParamsRaw  []byte
	)

	err := row.Scan(
		&item.PolicyKey,
		&item.DisplayName,
		&item.PublicMethod,
		&item.PublicPath,
		&item.UpstreamMethod,
		&item.UpstreamPath,
		&allowedParamsRaw,
		&requiredParamsRaw,
		&deniedParamsRaw,
		&defaultParamsRaw,
		&item.ProviderCredentialID,
		&item.Status,
		&item.Version,
	)
	if err != nil {
		return nil, err
	}

	item.AllowedParams = decodeJSONStringSlice(allowedParamsRaw)
	item.RequiredParams = decodeJSONStringSlice(requiredParamsRaw)
	item.DeniedParams = decodeJSONStringSlice(deniedParamsRaw)
	item.DefaultParams = decodeJSONStringMap(defaultParamsRaw)
	return &item, nil
}

func decodeJSONStringSlice(raw []byte) []string {
	if len(raw) == 0 {
		return []string{}
	}

	var result []string
	if err := json.Unmarshal(raw, &result); err != nil {
		return []string{}
	}
	if result == nil {
		return []string{}
	}
	return result
}

func decodeJSONStringMap(raw []byte) map[string]string {
	if len(raw) == 0 {
		return map[string]string{}
	}

	result := map[string]string{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return map[string]string{}
	}
	return result
}

func policyRouteCacheKey(method, path string) string {
	return "policy:v2:route:" + strings.ToUpper(strings.TrimSpace(method)) + ":" + path
}

func appPolicyCacheKey(appID string) string {
	return "policy:app:" + strings.TrimSpace(appID)
}

func uniquePolicyKeys(policyKeys []string) []string {
	seen := make(map[string]struct{}, len(policyKeys))
	result := make([]string, 0, len(policyKeys))
	for _, policyKey := range policyKeys {
		key := strings.TrimSpace(policyKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, key)
	}
	return result
}

func policyInternalError(message string) *policyServiceError {
	return &policyServiceError{statusCode: 500, code: 100500, message: message}
}

const policyInitSQL = `
CREATE SCHEMA IF NOT EXISTS policy;

CREATE TABLE IF NOT EXISTS policy.provider_credentials (
    id TEXT PRIMARY KEY,
    provider_name TEXT NOT NULL,
    credential_name TEXT NOT NULL,
    api_key_ciphertext TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS policy.policy_definitions (
    policy_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    public_method TEXT NOT NULL,
    public_path TEXT NOT NULL,
    upstream_method TEXT NOT NULL,
    upstream_path TEXT NOT NULL,
    allowed_params JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_params JSONB NOT NULL DEFAULT '[]'::jsonb,
    denied_params JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_credential_id TEXT REFERENCES policy.provider_credentials(id),
    status TEXT NOT NULL DEFAULT 'draft',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE policy.policy_definitions
    ADD COLUMN IF NOT EXISTS public_method TEXT;

ALTER TABLE policy.policy_definitions
    ADD COLUMN IF NOT EXISTS public_path TEXT;

ALTER TABLE policy.policy_definitions
    ADD COLUMN IF NOT EXISTS required_params JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS policy.app_policy_bindings (
    app_id TEXT NOT NULL,
    policy_key TEXT NOT NULL REFERENCES policy.policy_definitions(policy_key),
    status TEXT NOT NULL DEFAULT 'enabled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (app_id, policy_key)
);

CREATE INDEX IF NOT EXISTS idx_policy_definitions_public_route
    ON policy.policy_definitions (public_method, public_path);

CREATE INDEX IF NOT EXISTS idx_policy_bindings_policy_key
    ON policy.app_policy_bindings (policy_key);
`
