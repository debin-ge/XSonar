package internal

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

const replayWindow = 60 * time.Second

type serviceError struct {
	statusCode int
	code       int
	message    string
}

type consoleUser struct {
	ID           string
	Username     string
	PasswordHash string
	Role         string
	Status       string
}

type tenant struct {
	ID        string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type tenantApp struct {
	ID         string    `json:"app_id"`
	TenantID   string    `json:"tenant_id"`
	Name       string    `json:"name"`
	AppKey     string    `json:"app_key"`
	AppSecret  string    `json:"app_secret,omitempty"`
	Status     string    `json:"status"`
	DailyQuota int64     `json:"daily_quota"`
	QPSLimit   int       `json:"qps_limit"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type usageStat struct {
	BucketStart           time.Time `json:"bucket_start"`
	TenantID              string    `json:"tenant_id"`
	AppID                 string    `json:"app_id"`
	PolicyKey             string    `json:"policy_key"`
	TotalCount            int64     `json:"total_count"`
	SuccessCount          int64     `json:"success_count"`
	FailureCount          int64     `json:"failure_count"`
	DurationSumMS         int64     `json:"duration_sum_ms"`
	UpstreamDurationSumMS int64     `json:"upstream_duration_sum_ms"`
}

type reservation struct {
	AppID   string
	DateKey string
}

type service struct {
	logger       *xlog.Logger
	pgStore      *pgRedisStore
	mu           sync.RWMutex
	tenants      map[string]*tenant
	apps         map[string]*tenantApp
	appByKey     map[string]string
	consoleUsers map[string]*consoleUser
	nonceSeen    map[string]map[string]time.Time
	dailyUsage   map[string]map[string]int64
	secondUsage  map[string]map[int64]int
	reservations map[string]reservation
	usageStats   map[string]*usageStat
	ipBans       map[string]bool
}

type authenticateConsoleUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type createTenantRequest struct {
	Name string `json:"name"`
}

type createTenantAppRequest struct {
	TenantID   string `json:"tenant_id"`
	Name       string `json:"name"`
	DailyQuota int64  `json:"daily_quota"`
	QPSLimit   int    `json:"qps_limit"`
}

type rotateAppSecretRequest struct {
	AppID string `json:"app_id"`
}

type updateTenantAppStatusRequest struct {
	AppID  string `json:"app_id"`
	Status string `json:"status"`
}

type updateAppQuotaRequest struct {
	AppID      string `json:"app_id"`
	DailyQuota int64  `json:"daily_quota"`
	QPSLimit   int    `json:"qps_limit"`
}

type getAppAuthContextRequest struct {
	AppKey string `json:"app_key"`
}

type getAppAuthContextByIDRequest struct {
	AppID string `json:"app_id"`
}

type checkReplayRequest struct {
	AppID     string `json:"app_id"`
	Nonce     string `json:"nonce"`
	Timestamp int64  `json:"timestamp"`
}

type checkAndReserveQuotaRequest struct {
	AppID     string `json:"app_id"`
	PolicyKey string `json:"policy_key"`
	RequestID string `json:"request_id"`
}

type releaseQuotaOnFailureRequest struct {
	AppID     string `json:"app_id"`
	RequestID string `json:"request_id"`
}

type recordUsageStatRequest struct {
	TenantID           string `json:"tenant_id"`
	AppID              string `json:"app_id"`
	PolicyKey          string `json:"policy_key"`
	RequestID          string `json:"request_id"`
	Success            bool   `json:"success"`
	DurationMS         int64  `json:"duration_ms"`
	UpstreamDurationMS int64  `json:"upstream_duration_ms"`
	StatusCode         int    `json:"status_code"`
	ResultCode         string `json:"result_code"`
}

type queryUsageStatsRequest struct {
	TenantID  string `json:"tenant_id"`
	AppID     string `json:"app_id"`
	PolicyKey string `json:"policy_key"`
	StartUnix int64  `json:"start_unix"`
	EndUnix   int64  `json:"end_unix"`
}

type checkIPBanRequest struct {
	IP string `json:"ip"`
}

func newService(logger *xlog.Logger) *service {
	svc := &service{
		logger:       logger,
		tenants:      make(map[string]*tenant),
		apps:         make(map[string]*tenantApp),
		appByKey:     make(map[string]string),
		consoleUsers: make(map[string]*consoleUser),
		nonceSeen:    make(map[string]map[string]time.Time),
		dailyUsage:   make(map[string]map[string]int64),
		secondUsage:  make(map[string]map[int64]int),
		reservations: make(map[string]reservation),
		usageStats:   make(map[string]*usageStat),
		ipBans:       make(map[string]bool),
	}

	cfg := loadAccessStoreConfig()
	if cfg.Backend == "pgredis" {
		store, err := newPGRedisStore(cfg, logger)
		if err != nil {
			if errors.Is(err, shared.ErrSecretMasterKeyRequired) || errors.Is(err, shared.ErrInvalidSecretMasterKey) {
				panic(err)
			}
			logger.Error("access-rpc persistent backend unavailable, falling back to memory", map[string]any{
				"error": err.Error(),
			})
		} else {
			svc.pgStore = store
		}
	}

	if svc.pgStore == nil {
		svc.seed(cfg.SeedAdminUsername, cfg.SeedAdminPassword)
	}
	return svc
}

func (s *service) Shutdown(ctx context.Context) error {
	if s.pgStore == nil {
		return nil
	}
	return s.pgStore.Close(ctx)
}

func (s *service) seed(username, password string) {
	username = strings.TrimSpace(username)
	if username == "" || strings.TrimSpace(password) == "" {
		s.logger.Info("access-rpc bootstrap admin disabled", map[string]any{
			"reason": "missing ACCESS_RPC_SEED_ADMIN_USERNAME or ACCESS_RPC_SEED_ADMIN_PASSWORD",
		})
		return
	}

	passwordHash, err := shared.HashPassword(password)
	if err != nil {
		s.logger.Error("access-rpc bootstrap admin disabled", map[string]any{
			"reason": "hash ACCESS_RPC_SEED_ADMIN_PASSWORD failed",
			"error":  err.Error(),
		})
		return
	}
	user := &consoleUser{
		ID:           shared.NewID("user"),
		Username:     username,
		PasswordHash: passwordHash,
		Role:         "platform_admin",
		Status:       "active",
	}
	s.consoleUsers[user.Username] = user

	s.logger.Info("access-rpc seeded default admin user", map[string]any{
		"username": user.Username,
		"role":     user.Role,
	})
}

func (s *service) handleAuthenticateConsoleUser(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req authenticateConsoleUserRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.authenticateConsoleUser(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req createTenantRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.createTenant(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleListTenants(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	data, svcErr := s.listTenants(r.Context())
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleCreateTenantApp(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req createTenantAppRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.createTenantApp(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleListTenantApps(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	data, svcErr := s.listTenantApps(r.Context(), r.URL.Query().Get("tenant_id"))
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleRotateAppSecret(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req rotateAppSecretRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.rotateAppSecret(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleUpdateTenantAppStatus(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req updateTenantAppStatusRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.updateTenantAppStatus(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleUpdateAppQuota(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req updateAppQuotaRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.updateAppQuota(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleGetAppAuthContext(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req getAppAuthContextRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.getAppAuthContext(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleCheckReplay(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req checkReplayRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.checkReplay(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleCheckAndReserveQuota(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req checkAndReserveQuotaRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	if req.RequestID == "" {
		req.RequestID = requestID
	}

	data, svcErr := s.checkAndReserveQuota(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleReleaseQuotaOnFailure(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req releaseQuotaOnFailureRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.releaseQuotaOnFailure(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleRecordUsageStat(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req recordUsageStatRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.recordUsageStat(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleQueryUsageStats(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req queryUsageStatsRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.queryUsageStats(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) handleCheckIPBan(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req checkIPBanRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.checkIPBan(r.Context(), req)
	writeServiceResult(w, requestID, data, svcErr)
}

func (s *service) authenticateConsoleUser(ctx context.Context, req authenticateConsoleUserRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.authenticateConsoleUser(ctx, req)
	}
	if strings.TrimSpace(req.Username) == "" || strings.TrimSpace(req.Password) == "" {
		return nil, invalidRequest("username and password are required")
	}

	s.mu.RLock()
	user, ok := s.consoleUsers[req.Username]
	s.mu.RUnlock()
	if !ok || !shared.VerifyPassword(user.PasswordHash, req.Password) || user.Status != "active" {
		return nil, unauthorized("invalid username or password")
	}

	return map[string]any{
		"user_id": user.ID,
		"role":    user.Role,
	}, nil
}

func (s *service) createTenant(ctx context.Context, req createTenantRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.createTenant(ctx, req)
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, invalidRequest("tenant name is required")
	}

	now := time.Now().UTC()
	item := &tenant{
		ID:        shared.NewID("tenant"),
		Name:      name,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[item.ID] = item

	s.logger.Info("tenant created", map[string]any{
		"tenant_id": item.ID,
		"name":      item.Name,
	})

	return item, nil
}

func (s *service) listTenants(ctx context.Context) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.listTenants(ctx)
	}
	s.mu.RLock()
	items := make([]tenant, 0, len(s.tenants))
	for _, item := range s.tenants {
		items = append(items, *item)
	}
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})

	return map[string]any{"items": items}, nil
}

func (s *service) createTenantApp(ctx context.Context, req createTenantAppRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.createTenantApp(ctx, req)
	}
	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.Name) == "" {
		return nil, invalidRequest("tenant_id and name are required")
	}
	if req.DailyQuota < 0 || req.QPSLimit < 0 {
		return nil, invalidRequest("daily_quota and qps_limit must be non-negative")
	}

	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tenants[req.TenantID]; !ok {
		return nil, notFound("tenant not found")
	}

	item := &tenantApp{
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

	s.apps[item.ID] = item
	s.appByKey[item.AppKey] = item.ID

	s.logger.Info("tenant app created", map[string]any{
		"tenant_id": item.TenantID,
		"app_id":    item.ID,
	})

	return item, nil
}

func (s *service) listTenantApps(ctx context.Context, tenantID string) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.listTenantApps(ctx, tenantID)
	}
	s.mu.RLock()
	items := make([]tenantApp, 0, len(s.apps))
	for _, item := range s.apps {
		if tenantID != "" && item.TenantID != tenantID {
			continue
		}
		items = append(items, *item)
	}
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})

	return map[string]any{"items": items}, nil
}

func (s *service) rotateAppSecret(ctx context.Context, req rotateAppSecretRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.rotateAppSecret(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" {
		return nil, invalidRequest("app_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.apps[req.AppID]
	if !ok {
		return nil, notFound("app not found")
	}

	item.AppSecret = shared.NewSecret("secret")
	item.UpdatedAt = time.Now().UTC()

	return map[string]any{
		"app_id":     item.ID,
		"app_secret": item.AppSecret,
		"updated_at": item.UpdatedAt,
	}, nil
}

func (s *service) updateTenantAppStatus(ctx context.Context, req updateTenantAppStatusRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.updateTenantAppStatus(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.Status) == "" {
		return nil, invalidRequest("app_id and status are required")
	}

	status := strings.ToLower(strings.TrimSpace(req.Status))
	if status != "active" && status != "disabled" {
		return nil, invalidRequest("status must be active or disabled")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.apps[req.AppID]
	if !ok {
		return nil, notFound("app not found")
	}

	item.Status = status
	item.UpdatedAt = time.Now().UTC()

	return item, nil
}

func (s *service) updateAppQuota(ctx context.Context, req updateAppQuotaRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.updateAppQuota(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" {
		return nil, invalidRequest("app_id is required")
	}
	if req.DailyQuota < 0 || req.QPSLimit < 0 {
		return nil, invalidRequest("daily_quota and qps_limit must be non-negative")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.apps[req.AppID]
	if !ok {
		return nil, notFound("app not found")
	}

	item.DailyQuota = req.DailyQuota
	item.QPSLimit = req.QPSLimit
	item.UpdatedAt = time.Now().UTC()

	return item, nil
}

func (s *service) getAppAuthContext(ctx context.Context, req getAppAuthContextRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.getAppAuthContext(ctx, req)
	}
	if strings.TrimSpace(req.AppKey) == "" {
		return nil, invalidRequest("app_key is required")
	}

	s.mu.RLock()
	appID, ok := s.appByKey[req.AppKey]
	if !ok {
		s.mu.RUnlock()
		return nil, notFound("app_key not found")
	}

	item := *s.apps[appID]
	s.mu.RUnlock()

	return map[string]any{
		"tenant_id":   item.TenantID,
		"app_id":      item.ID,
		"app_key":     item.AppKey,
		"app_secret":  item.AppSecret,
		"status":      item.Status,
		"daily_quota": item.DailyQuota,
		"qps_limit":   item.QPSLimit,
	}, nil
}

func (s *service) getAppAuthContextByID(ctx context.Context, req getAppAuthContextByIDRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.getAppAuthContextByID(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" {
		return nil, invalidRequest("app_id is required")
	}

	s.mu.RLock()
	item, ok := s.apps[req.AppID]
	if !ok {
		s.mu.RUnlock()
		return nil, notFound("app not found")
	}
	app := *item
	s.mu.RUnlock()

	return map[string]any{
		"tenant_id":   app.TenantID,
		"app_id":      app.ID,
		"app_key":     app.AppKey,
		"app_secret":  app.AppSecret,
		"status":      app.Status,
		"daily_quota": app.DailyQuota,
		"qps_limit":   app.QPSLimit,
	}, nil
}

func (s *service) checkReplay(ctx context.Context, req checkReplayRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.checkReplay(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.Nonce) == "" {
		return nil, invalidRequest("app_id and nonce are required")
	}
	if req.Timestamp == 0 {
		return nil, invalidRequest("timestamp is required")
	}

	now := time.Now().UTC()
	if math.Abs(float64(now.Unix()-req.Timestamp)) > replayWindow.Seconds() {
		return nil, forbidden("timestamp drift exceeds 60 seconds")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nonceMap, ok := s.nonceSeen[req.AppID]
	if !ok {
		nonceMap = make(map[string]time.Time)
		s.nonceSeen[req.AppID] = nonceMap
	}

	for nonce, expiresAt := range nonceMap {
		if now.After(expiresAt) {
			delete(nonceMap, nonce)
		}
	}

	if expiresAt, exists := nonceMap[req.Nonce]; exists && now.Before(expiresAt) {
		return nil, conflict("nonce already used")
	}

	nonceMap[req.Nonce] = now.Add(replayWindow)

	return map[string]any{
		"accepted":              true,
		"replay_window_seconds": int(replayWindow.Seconds()),
	}, nil
}

func (s *service) checkAndReserveQuota(ctx context.Context, req checkAndReserveQuotaRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.checkAndReserveQuota(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.RequestID) == "" {
		return nil, invalidRequest("app_id and request_id are required")
	}

	now := time.Now().UTC()
	secondKey := now.Unix()
	dateKey := now.Format("2006-01-02")

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.apps[req.AppID]
	if !ok {
		return nil, notFound("app not found")
	}

	if reservation, exists := s.reservations[req.RequestID]; exists {
		return map[string]any{
			"allowed":               true,
			"request_id":            req.RequestID,
			"policy_key":            req.PolicyKey,
			"remaining_daily_quota": remainingDailyQuota(item.DailyQuota, s.dailyUsage[item.ID][reservation.DateKey]),
			"qps_limit":             item.QPSLimit,
			"idempotent":            true,
		}, nil
	}

	secondUsage := s.secondUsage[item.ID]
	if secondUsage == nil {
		secondUsage = make(map[int64]int)
		s.secondUsage[item.ID] = secondUsage
	}
	for key := range secondUsage {
		if key < secondKey-2 {
			delete(secondUsage, key)
		}
	}

	if item.QPSLimit > 0 && secondUsage[secondKey] >= item.QPSLimit {
		return nil, rateLimited("qps limit exceeded")
	}

	dailyUsage := s.dailyUsage[item.ID]
	if dailyUsage == nil {
		dailyUsage = make(map[string]int64)
		s.dailyUsage[item.ID] = dailyUsage
	}

	if item.DailyQuota > 0 && dailyUsage[dateKey] >= item.DailyQuota {
		return nil, rateLimited("daily quota exceeded")
	}

	secondUsage[secondKey]++
	dailyUsage[dateKey]++
	s.reservations[req.RequestID] = reservation{
		AppID:   item.ID,
		DateKey: dateKey,
	}

	return map[string]any{
		"allowed":               true,
		"request_id":            req.RequestID,
		"policy_key":            req.PolicyKey,
		"remaining_daily_quota": remainingDailyQuota(item.DailyQuota, dailyUsage[dateKey]),
		"qps_limit":             item.QPSLimit,
	}, nil
}

func (s *service) releaseQuotaOnFailure(ctx context.Context, req releaseQuotaOnFailureRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.releaseQuotaOnFailure(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.RequestID) == "" {
		return nil, invalidRequest("app_id and request_id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	reservation, ok := s.reservations[req.RequestID]
	if !ok {
		return map[string]any{
			"released":   false,
			"request_id": req.RequestID,
		}, nil
	}

	if reservation.AppID != req.AppID {
		return nil, conflict("request_id does not belong to app")
	}

	if dailyUsage := s.dailyUsage[reservation.AppID]; dailyUsage != nil && dailyUsage[reservation.DateKey] > 0 {
		dailyUsage[reservation.DateKey]--
	}
	delete(s.reservations, req.RequestID)

	return map[string]any{
		"released":   true,
		"request_id": req.RequestID,
	}, nil
}

func (s *service) recordUsageStat(ctx context.Context, req recordUsageStatRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.recordUsageStat(ctx, req)
	}
	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.PolicyKey) == "" {
		return nil, invalidRequest("tenant_id, app_id and policy_key are required")
	}

	bucketStart := time.Now().UTC().Truncate(5 * time.Minute)
	statKey := statsKey(bucketStart, req.TenantID, req.AppID, req.PolicyKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	stat, ok := s.usageStats[statKey]
	if !ok {
		stat = &usageStat{
			BucketStart: bucketStart,
			TenantID:    req.TenantID,
			AppID:       req.AppID,
			PolicyKey:   req.PolicyKey,
		}
		s.usageStats[statKey] = stat
	}

	stat.TotalCount++
	if req.Success {
		stat.SuccessCount++
	} else {
		stat.FailureCount++
	}
	stat.DurationSumMS += req.DurationMS
	stat.UpstreamDurationSumMS += req.UpstreamDurationMS

	delete(s.reservations, req.RequestID)

	return map[string]any{
		"bucket_start":             stat.BucketStart,
		"tenant_id":                stat.TenantID,
		"app_id":                   stat.AppID,
		"policy_key":               stat.PolicyKey,
		"total_count":              stat.TotalCount,
		"success_count":            stat.SuccessCount,
		"failure_count":            stat.FailureCount,
		"duration_sum_ms":          stat.DurationSumMS,
		"upstream_duration_sum_ms": stat.UpstreamDurationSumMS,
	}, nil
}

func (s *service) queryUsageStats(ctx context.Context, req queryUsageStatsRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.queryUsageStats(ctx, req)
	}
	s.mu.RLock()
	items := make([]usageStat, 0, len(s.usageStats))
	for _, item := range s.usageStats {
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
		items = append(items, *item)
	}
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].BucketStart.Before(items[j].BucketStart)
	})

	return map[string]any{"items": items}, nil
}

func (s *service) checkIPBan(ctx context.Context, req checkIPBanRequest) (any, *serviceError) {
	if s.pgStore != nil {
		return s.pgStore.checkIPBan(ctx, req)
	}
	if strings.TrimSpace(req.IP) == "" {
		return nil, invalidRequest("ip is required")
	}

	s.mu.RLock()
	blocked := s.ipBans[req.IP]
	s.mu.RUnlock()

	return map[string]any{
		"ip":      req.IP,
		"blocked": blocked,
	}, nil
}

func writeServiceResult(w http.ResponseWriter, requestID string, data any, svcErr *serviceError) {
	if svcErr != nil {
		shared.WriteError(w, svcErr.statusCode, svcErr.code, svcErr.message, requestID)
		return
	}

	shared.WriteOK(w, data, requestID)
}

func statsKey(bucketStart time.Time, tenantID, appID, policyKey string) string {
	return strings.Join([]string{
		strconv.FormatInt(bucketStart.Unix(), 10),
		tenantID,
		appID,
		policyKey,
	}, ":")
}

func remainingDailyQuota(limit, used int64) int64 {
	if limit <= 0 {
		return -1
	}
	return limit - used
}

func invalidRequest(message string) *serviceError {
	return &serviceError{statusCode: http.StatusBadRequest, code: model.CodeInvalidRequest, message: message}
}

func unauthorized(message string) *serviceError {
	return &serviceError{statusCode: http.StatusUnauthorized, code: model.CodeUnauthorized, message: message}
}

func forbidden(message string) *serviceError {
	return &serviceError{statusCode: http.StatusForbidden, code: model.CodeForbidden, message: message}
}

func notFound(message string) *serviceError {
	return &serviceError{statusCode: http.StatusNotFound, code: model.CodeNotFound, message: message}
}

func conflict(message string) *serviceError {
	return &serviceError{statusCode: http.StatusConflict, code: model.CodeConflict, message: message}
}

func rateLimited(message string) *serviceError {
	return &serviceError{statusCode: http.StatusTooManyRequests, code: model.CodeRateLimited, message: message}
}

func marshalJSON(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(payload)
}
