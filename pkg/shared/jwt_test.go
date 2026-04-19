package shared

import (
	"testing"
	"time"
)

func TestSignJWTAllowsInfiniteTTLWhenZero(t *testing.T) {
	now := time.Date(2026, time.April, 15, 10, 0, 0, 0, time.UTC)

	token, err := SignJWT("secret-1", "issuer-1", "subject-1", "gateway_app", 0, now)
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}

	claims, err := ParseAndValidateJWT("secret-1", token, now.Add(365*24*time.Hour))
	if err != nil {
		t.Fatalf("parse jwt: %v", err)
	}
	if claims.Subject != "subject-1" || claims.Issuer != "issuer-1" || claims.Role != "gateway_app" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	if claims.ExpiresAt != 0 {
		t.Fatalf("expected no exp claim, got %+v", claims)
	}
}
