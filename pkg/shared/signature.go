package shared

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func ComputeSignature(secret, method, path string, query url.Values, timestamp, nonce string) string {
	canonical := CanonicalSignaturePayload(method, path, query, timestamp, nonce)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func SignaturesEqual(expected, provided string) bool {
	normalizedExpected := strings.ToLower(strings.TrimSpace(expected))
	normalizedProvided := strings.ToLower(strings.TrimSpace(provided))
	if len(normalizedExpected) != len(normalizedProvided) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(normalizedExpected), []byte(normalizedProvided)) == 1
}

func CanonicalSignaturePayload(method, path string, query url.Values, timestamp, nonce string) string {
	return strings.Join([]string{
		strings.ToUpper(method),
		path,
		CanonicalBusinessQuery(query),
		timestamp,
		nonce,
	}, "\n")
}

func CanonicalBusinessQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	filtered := make([]string, 0)
	keys := make([]string, 0, len(query))
	for key := range query {
		if isAuthField(key) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			filtered = append(filtered, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}

	return strings.Join(filtered, "&")
}

func ValidateTimestamp(timestamp string, maxSkew time.Duration) error {
	if strings.TrimSpace(timestamp) == "" {
		return fmt.Errorf("timestamp is required")
	}

	parsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("timestamp must be unix seconds")
	}

	if absDuration(time.Since(time.Unix(parsed, 0))) > maxSkew {
		return fmt.Errorf("timestamp drift exceeds %d seconds", int(maxSkew.Seconds()))
	}

	return nil
}

func ParseUnixTimestamp(timestamp string) (int64, error) {
	return strconv.ParseInt(timestamp, 10, 64)
}

func isAuthField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "appkey", "app_key", "appsecret", "app_secret", "timestamp", "nonce", "signature":
		return true
	default:
		return false
	}
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
