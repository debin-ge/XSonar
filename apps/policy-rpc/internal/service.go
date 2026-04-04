package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"

	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type policyServiceError struct {
	statusCode int
	code       int
	message    string
}

type providerCredential struct {
	ID           string `json:"id"`
	ProviderName string `json:"provider_name"`
	DisplayName  string `json:"display_name"`
	APIKey       string `json:"api_key"`
	Status       string `json:"status"`
}

type policyDefinition struct {
	PolicyKey            string            `json:"policy_key"`
	DisplayName          string            `json:"display_name"`
	PublicMethod         string            `json:"public_method"`
	PublicPath           string            `json:"public_path"`
	UpstreamMethod       string            `json:"upstream_method"`
	UpstreamPath         string            `json:"upstream_path"`
	AllowedParams        []string          `json:"allowed_params"`
	RequiredParams       []string          `json:"required_params"`
	DeniedParams         []string          `json:"denied_params"`
	DefaultParams        map[string]string `json:"default_params"`
	ProviderCredentialID string            `json:"provider_credential_id"`
	Status               string            `json:"status"`
	Version              int               `json:"version"`
}

type policyService struct {
	logger          *xlog.Logger
	pgStore         *pgRedisPolicyStore
	mu              sync.RWMutex
	credentials     map[string]*providerCredential
	policies        map[string]*policyDefinition
	pathIndex       map[string]string
	appPolicyAccess map[string]map[string]struct{}
}

type resolvePolicyRequest struct {
	Path   string `json:"path"`
	Method string `json:"method"`
}

type checkAppPolicyAccessRequest struct {
	AppID     string `json:"app_id"`
	PolicyKey string `json:"policy_key"`
}

type publishPolicyConfigRequest struct {
	PolicyKey            string            `json:"policy_key"`
	DisplayName          string            `json:"display_name"`
	PublicMethod         string            `json:"public_method"`
	PublicPath           string            `json:"public_path"`
	UpstreamMethod       string            `json:"upstream_method"`
	UpstreamPath         string            `json:"upstream_path"`
	AllowedParams        []string          `json:"allowed_params"`
	RequiredParams       []string          `json:"required_params"`
	DeniedParams         []string          `json:"denied_params"`
	DefaultParams        map[string]string `json:"default_params"`
	ProviderCredentialID string            `json:"provider_credential_id"`
}

type bindAppPoliciesRequest struct {
	AppID      string   `json:"app_id"`
	PolicyKeys []string `json:"policy_keys"`
}

func newPolicyService(logger *xlog.Logger) *policyService {
	svc := &policyService{
		logger:          logger,
		credentials:     make(map[string]*providerCredential),
		policies:        make(map[string]*policyDefinition),
		pathIndex:       make(map[string]string),
		appPolicyAccess: make(map[string]map[string]struct{}),
	}

	cfg := loadPolicyStoreConfig()
	if cfg.Backend == "pgredis" {
		store, err := newPGRedisPolicyStore(cfg, logger)
		if err != nil {
			if errors.Is(err, shared.ErrSecretMasterKeyRequired) || errors.Is(err, shared.ErrInvalidSecretMasterKey) {
				panic(err)
			}
			logger.Error("policy-rpc persistent backend unavailable, falling back to memory", map[string]any{
				"error": err.Error(),
			})
		} else {
			svc.pgStore = store
		}
	}

	if svc.pgStore == nil {
		svc.seed(cfg.ProviderAPIKey)
	}
	return svc
}

func (s *policyService) Shutdown(ctx context.Context) error {
	if s.pgStore == nil {
		return nil
	}
	return s.pgStore.Close(ctx)
}

func (s *policyService) seed(providerAPIKey string) {
	if strings.TrimSpace(providerAPIKey) == "" {
		s.logger.Info("policy-rpc default provider credential bootstrap disabled", map[string]any{
			"reason": "missing POLICY_RPC_PROVIDER_API_KEY",
		})
		return
	}

	credential := &providerCredential{
		ID:           "provider_credential_fapi_uk",
		ProviderName: "fapi.uk",
		DisplayName:  "Fapi.uk Default",
		APIKey:       providerAPIKey,
		Status:       "active",
	}
	s.credentials[credential.ID] = credential

	for _, item := range defaultPolicies(credential.ID) {
		policy := item
		s.policies[policy.PolicyKey] = &policy
		s.pathIndex[pathKey(policy.PublicMethod, policy.PublicPath)] = policy.PolicyKey
	}

	s.logger.Info("policy-rpc seeded default policies", map[string]any{
		"count": len(s.policies),
	})
}

func (s *policyService) handleResolvePolicy(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req resolvePolicyRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.resolvePolicy(r.Context(), req)
	writePolicyResult(w, requestID, data, svcErr)
}

func (s *policyService) handleCheckAppPolicyAccess(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req checkAppPolicyAccessRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.checkAppPolicyAccess(r.Context(), req)
	writePolicyResult(w, requestID, data, svcErr)
}

func (s *policyService) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	data, svcErr := s.listPolicies(r.Context())
	writePolicyResult(w, requestID, data, svcErr)
}

func (s *policyService) handlePublishPolicyConfig(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req publishPolicyConfigRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.publishPolicyConfig(r.Context(), req)
	writePolicyResult(w, requestID, data, svcErr)
}

func (s *policyService) handleBindAppPolicies(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req bindAppPoliciesRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.bindAppPolicies(r.Context(), req)
	writePolicyResult(w, requestID, data, svcErr)
}

func (s *policyService) resolvePolicy(ctx context.Context, req resolvePolicyRequest) (any, *policyServiceError) {
	if s.pgStore != nil {
		return s.pgStore.resolvePolicy(ctx, req)
	}
	if strings.TrimSpace(req.Path) == "" {
		return nil, policyInvalidRequest("path is required")
	}

	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = http.MethodGet
	}

	s.mu.RLock()
	policyKey, ok := s.pathIndex[pathKey(method, req.Path)]
	if !ok {
		s.mu.RUnlock()
		return nil, policyNotFound("policy not found for path")
	}

	policy := *s.policies[policyKey]
	credential := s.credentials[policy.ProviderCredentialID]
	s.mu.RUnlock()

	return map[string]any{
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
		"provider_name":    credential.ProviderName,
		"provider_api_key": credential.APIKey,
		"version":          policy.Version,
		"status":           policy.Status,
	}, nil
}

func (s *policyService) checkAppPolicyAccess(ctx context.Context, req checkAppPolicyAccessRequest) (any, *policyServiceError) {
	if s.pgStore != nil {
		return s.pgStore.checkAppPolicyAccess(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" || strings.TrimSpace(req.PolicyKey) == "" {
		return nil, policyInvalidRequest("app_id and policy_key are required")
	}

	s.mu.RLock()
	bindings := s.appPolicyAccess[req.AppID]
	_, allowed := bindings[req.PolicyKey]
	s.mu.RUnlock()

	return map[string]any{
		"app_id":     req.AppID,
		"policy_key": req.PolicyKey,
		"allowed":    allowed,
	}, nil
}

func (s *policyService) listPolicies(ctx context.Context) (any, *policyServiceError) {
	if s.pgStore != nil {
		return s.pgStore.listPolicies(ctx)
	}
	s.mu.RLock()
	items := make([]policyDefinition, 0, len(s.policies))
	for _, item := range s.policies {
		items = append(items, *item)
	}
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].PublicPath < items[j].PublicPath
	})

	return map[string]any{"items": items}, nil
}

func (s *policyService) publishPolicyConfig(ctx context.Context, req publishPolicyConfigRequest) (any, *policyServiceError) {
	if s.pgStore != nil {
		return s.pgStore.publishPolicyConfig(ctx, req)
	}
	if strings.TrimSpace(req.PolicyKey) == "" || strings.TrimSpace(req.DisplayName) == "" {
		return nil, policyInvalidRequest("policy_key and display_name are required")
	}
	if strings.TrimSpace(req.PublicPath) == "" || strings.TrimSpace(req.UpstreamPath) == "" {
		return nil, policyInvalidRequest("public_path and upstream_path are required")
	}
	if strings.TrimSpace(req.ProviderCredentialID) == "" {
		return nil, policyInvalidRequest("provider_credential_id is required")
	}

	publicMethod := strings.ToUpper(firstNonEmpty(req.PublicMethod, http.MethodGet))
	upstreamMethod := strings.ToUpper(firstNonEmpty(req.UpstreamMethod, http.MethodGet))

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.credentials[req.ProviderCredentialID]; !ok {
		return nil, policyNotFound("provider credential not found")
	}

	current, exists := s.policies[req.PolicyKey]
	version := 1
	if exists {
		delete(s.pathIndex, pathKey(current.PublicMethod, current.PublicPath))
		version = current.Version + 1
	}

	item := &policyDefinition{
		PolicyKey:            req.PolicyKey,
		DisplayName:          req.DisplayName,
		PublicMethod:         publicMethod,
		PublicPath:           req.PublicPath,
		UpstreamMethod:       upstreamMethod,
		UpstreamPath:         req.UpstreamPath,
		AllowedParams:        append([]string(nil), req.AllowedParams...),
		RequiredParams:       append([]string(nil), req.RequiredParams...),
		DeniedParams:         append([]string(nil), req.DeniedParams...),
		DefaultParams:        cloneStringMap(req.DefaultParams),
		ProviderCredentialID: req.ProviderCredentialID,
		Status:               "published",
		Version:              version,
	}

	s.policies[item.PolicyKey] = item
	s.pathIndex[pathKey(item.PublicMethod, item.PublicPath)] = item.PolicyKey

	return item, nil
}

func (s *policyService) bindAppPolicies(ctx context.Context, req bindAppPoliciesRequest) (any, *policyServiceError) {
	if s.pgStore != nil {
		return s.pgStore.bindAppPolicies(ctx, req)
	}
	if strings.TrimSpace(req.AppID) == "" {
		return nil, policyInvalidRequest("app_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	access := make(map[string]struct{}, len(req.PolicyKeys))
	for _, policyKey := range req.PolicyKeys {
		if _, ok := s.policies[policyKey]; !ok {
			return nil, policyNotFound("policy not found: " + policyKey)
		}
		access[policyKey] = struct{}{}
	}

	s.appPolicyAccess[req.AppID] = access

	return map[string]any{
		"app_id":      req.AppID,
		"policy_keys": req.PolicyKeys,
	}, nil
}

func writePolicyResult(w http.ResponseWriter, requestID string, data any, svcErr *policyServiceError) {
	if svcErr != nil {
		shared.WriteError(w, svcErr.statusCode, svcErr.code, svcErr.message, requestID)
		return
	}

	shared.WriteOK(w, data, requestID)
}

func policyInvalidRequest(message string) *policyServiceError {
	return &policyServiceError{statusCode: http.StatusBadRequest, code: model.CodeInvalidRequest, message: message}
}

func policyNotFound(message string) *policyServiceError {
	return &policyServiceError{statusCode: http.StatusNotFound, code: model.CodeNotFound, message: message}
}

func pathKey(method, path string) string {
	return strings.ToUpper(method) + " " + path
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func defaultPolicies(credentialID string) []policyDefinition {
	return []policyDefinition{
		newSeedPolicy("users_by_ids_v1", "Users By IDs", http.MethodGet, "/v1/users/by-ids", http.MethodGet, "/base/apitools/usersByIdRestIds", []string{"userIds"}, []string{"userIds"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("users_by_id_v1", "User By ID", http.MethodGet, "/v1/users/by-id", http.MethodGet, "/base/apitools/uerByIdRestIdV2", []string{"userId", "cursor"}, []string{"userId"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("users_by_username_v1", "User By Username", http.MethodGet, "/v1/users/by-username", http.MethodGet, "/base/apitools/userByScreenNameV2", []string{"screenName"}, []string{"screenName"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("tweets_by_ids_v1", "Tweets By IDs", http.MethodGet, "/v1/tweets/by-ids", http.MethodGet, "/base/apitools/tweetResultsByRestIds", []string{"tweetIds"}, []string{"tweetIds"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("tweets_timeline_v1", "Tweets Timeline", http.MethodGet, "/v1/tweets/timeline", http.MethodGet, "/base/apitools/userTimeline", []string{"userId", "cursor"}, []string{"userId"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("tweets_replies_v1", "Tweets Replies", http.MethodGet, "/v1/tweets/replies", http.MethodGet, "/base/apitools/userTweetReply", []string{"userId", "cursor"}, []string{"userId"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("tweets_detail_v1", "Tweet Detail", http.MethodGet, "/v1/tweets/detail", http.MethodGet, "/base/apitools/tweetTimeline", []string{"tweetId", "cursor"}, []string{"tweetId"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("search_tweets_v1", "Search Tweets", http.MethodGet, "/v1/search/tweets", http.MethodGet, "/base/apitools/search", []string{"cursor", "words", "phrase", "any", "none", "tag", "from", "to", "mentioning", "replies", "likes", "retweets", "since", "until", "product", "count"}, []string{"words"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("search_trending_v1", "Search Trending", http.MethodGet, "/v1/search/trending", http.MethodGet, "/base/apitools/trending", []string{}, nil, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("search_trends_v1", "Search Trends", http.MethodGet, "/v1/search/trends", http.MethodGet, "/base/apitools/trends", []string{"id"}, []string{"id"}, []string{"proxyUrl", "auth_token"}, credentialID),
		newSeedPolicy("tweets_brief_v1", "Tweets Brief", http.MethodGet, "/v1/tweets/brief", http.MethodGet, "/base/apitools/tweetSimple", []string{"tweetId", "cursor"}, []string{"tweetId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("tweets_quotes_v1", "Tweets Quotes", http.MethodGet, "/v1/tweets/quotes", http.MethodGet, "/base/apitools/quotesV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("tweets_retweeters_v1", "Tweets Retweeters", http.MethodGet, "/v1/tweets/retweeters", http.MethodGet, "/base/apitools/retweetersV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("tweets_favoriters_v1", "Tweets Favoriters", http.MethodGet, "/v1/tweets/favoriters", http.MethodGet, "/base/apitools/favoritersV2", []string{"tweetId", "cursor", "authToken"}, []string{"tweetId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_mentions_timeline_v1", "Users Mentions Timeline", http.MethodGet, "/v1/users/mentions-timeline", http.MethodGet, "/base/apitools/mentionsTimeline", []string{"authToken", "csrfToken", "sinceId", "maxId", "includeEntities", "trimUser"}, []string{"authToken"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("search_box_v1", "Search Box", http.MethodGet, "/v1/search/box", http.MethodGet, "/base/apitools/searchBox", []string{"words", "searchType"}, []string{"words"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("search_explore_v1", "Search Explore", http.MethodGet, "/v1/search/explore", http.MethodGet, "/base/apitools/explore", []string{}, nil, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("search_news_v1", "Search News", http.MethodGet, "/v1/search/news", http.MethodGet, "/base/apitools/news", []string{}, nil, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("search_sports_v1", "Search Sports", http.MethodGet, "/v1/search/sports", http.MethodGet, "/base/apitools/sports", []string{}, nil, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("search_entertainment_v1", "Search Entertainment", http.MethodGet, "/v1/search/entertainment", http.MethodGet, "/base/apitools/entertainment", []string{}, nil, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_username_changes_v1", "Users Username Changes", http.MethodGet, "/v1/users/username-changes", http.MethodGet, "/base/apitools/usernameChanges", []string{"screenName"}, []string{"screenName"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_likes_v1", "Users Likes", http.MethodGet, "/v1/users/likes", http.MethodGet, "/base/apitools/userLikeV2", []string{"userId", "cursor", "authToken"}, []string{"userId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_highlights_v1", "Users Highlights", http.MethodGet, "/v1/users/highlights", http.MethodGet, "/base/apitools/highlightsV2", []string{"userId", "cursor", "authToken"}, []string{"userId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_articles_tweets_v1", "Users Articles Tweets", http.MethodGet, "/v1/users/articles-tweets", http.MethodGet, "/base/apitools/UserArticlesTweets", []string{"userId", "cursor", "authToken"}, []string{"userId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_account_analytics_v1", "Users Account Analytics", http.MethodGet, "/v1/users/account-analytics", http.MethodGet, "/base/apitools/accountAnalytics", []string{"restId", "authToken", "csrfToken"}, []string{"restId", "authToken"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_followers_v1", "Users Followers", http.MethodGet, "/v1/users/followers", http.MethodGet, "/base/apitools/followersListV2", []string{"userId", "cursor"}, []string{"userId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("users_followings_v1", "Users Followings", http.MethodGet, "/v1/users/followings", http.MethodGet, "/base/apitools/followingsListV2", []string{"userId", "cursor"}, []string{"userId"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("lists_v1", "Lists", http.MethodGet, "/v1/lists", http.MethodGet, "/base/apitools/listByUserIdOrScreenName", []string{"userId", "screenName"}, nil, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
		newSeedPolicy("communities_v1", "Communities", http.MethodGet, "/v1/communities", http.MethodGet, "/base/apitools/getCommunitiesByScreenName", []string{"screenName"}, []string{"screenName"}, []string{"proxyUrl", "auth_token", "ct0"}, credentialID),
	}
}

func newSeedPolicy(policyKey, displayName, publicMethod, publicPath, upstreamMethod, upstreamPath string, allowedParams, requiredParams, deniedParams []string, credentialID string) policyDefinition {
	return policyDefinition{
		PolicyKey:            policyKey,
		DisplayName:          displayName,
		PublicMethod:         publicMethod,
		PublicPath:           publicPath,
		UpstreamMethod:       upstreamMethod,
		UpstreamPath:         upstreamPath,
		AllowedParams:        append([]string(nil), allowedParams...),
		RequiredParams:       append([]string(nil), requiredParams...),
		DeniedParams:         append([]string(nil), deniedParams...),
		DefaultParams:        map[string]string{},
		ProviderCredentialID: credentialID,
		Status:               "published",
		Version:              1,
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return map[string]string{}
	}

	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func mustJSONString(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(payload)
}
