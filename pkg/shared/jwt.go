package shared

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type JWTClaims struct {
	Subject   string `json:"sub"`
	Role      string `json:"role"`
	Issuer    string `json:"iss"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp,omitempty"`
}

func SignJWT(secret, issuer, subject, role string, ttl time.Duration, now time.Time) (string, error) {
	if strings.TrimSpace(secret) == "" {
		return "", errors.New("jwt secret is required")
	}
	if strings.TrimSpace(subject) == "" {
		return "", errors.New("jwt subject is required")
	}
	if ttl < 0 {
		return "", errors.New("jwt ttl must be non-negative")
	}

	headerPayload, err := encodeJWTPart(map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	})
	if err != nil {
		return "", err
	}

	claims := JWTClaims{
		Subject:  subject,
		Role:     role,
		Issuer:   issuer,
		IssuedAt: now.UTC().Unix(),
	}
	if ttl > 0 {
		claims.ExpiresAt = now.UTC().Add(ttl).Unix()
	}

	claimsPayload, err := encodeJWTPart(claims)
	if err != nil {
		return "", err
	}

	unsigned := headerPayload + "." + claimsPayload
	signature := signJWT(secret, unsigned)
	return unsigned + "." + signature, nil
}

func ParseAndValidateJWT(secret, token string, now time.Time) (*JWTClaims, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, errors.New("jwt secret is required")
	}

	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid jwt format")
	}

	unsigned := parts[0] + "." + parts[1]
	expected := signJWT(secret, unsigned)
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return nil, errors.New("invalid jwt signature")
	}

	var claims JWTClaims
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return nil, err
	}
	if claims.ExpiresAt > 0 && claims.ExpiresAt <= now.UTC().Unix() {
		return nil, errors.New("jwt expired")
	}
	return &claims, nil
}

func ExtractBearerToken(headerValue string) string {
	headerValue = strings.TrimSpace(headerValue)
	if !strings.HasPrefix(strings.ToLower(headerValue), "bearer ") {
		return ""
	}
	return strings.TrimSpace(headerValue[7:])
}

func encodeJWTPart(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeJWTPart(part string, out any) error {
	payload, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, out)
}

func signJWT(secret, unsigned string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(unsigned))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
