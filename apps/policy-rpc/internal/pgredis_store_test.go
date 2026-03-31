package internal

import (
	"testing"

	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

func TestPolicyServicePanicsWhenPGRedisMasterKeyMissing(t *testing.T) {
	t.Setenv("COMMON_STORE_BACKEND", "pgredis")
	t.Setenv("COMMON_SECRET_MASTER_KEY", "")
	t.Setenv("COMMON_POSTGRES_DSN", "://")
	t.Setenv("COMMON_REDIS_ADDR", "127.0.0.1:1")

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected missing COMMON_SECRET_MASTER_KEY to abort pgredis startup")
		}
	}()

	_ = newPolicyService(xlog.NewStdout("policy-rpc-test"))
}

func TestScanResolvedPolicyRowDecryptsProviderAPIKey(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	ciphertext, err := shared.EncryptSecretValue(key, "provider-key-1")
	if err != nil {
		t.Fatalf("EncryptSecretValue returned error: %v", err)
	}

	policy, providerName, providerAPIKey, err := scanResolvedPolicyRow(fakeResolvedPolicyRow{
		providerAPIKey: ciphertext,
		masterKey:      key,
	})
	if err != nil {
		t.Fatalf("scanResolvedPolicyRow returned error: %v", err)
	}
	if providerName != "fapi.uk" {
		t.Fatalf("unexpected provider name: %q", providerName)
	}
	if providerAPIKey != "provider-key-1" {
		t.Fatalf("expected decrypted provider api key, got %q", providerAPIKey)
	}
	if len(policy.RequiredParams) != 1 || policy.RequiredParams[0] != "userIds" {
		t.Fatalf("unexpected required params: %#v", policy.RequiredParams)
	}
}

func TestScanResolvedPolicyRowRejectsPlaintextProviderAPIKey(t *testing.T) {
	if _, _, _, err := scanResolvedPolicyRow(fakeResolvedPolicyRow{
		providerAPIKey: "provider-key-1",
		masterKey:      []byte("0123456789abcdef0123456789abcdef"),
	}); err == nil {
		t.Fatal("expected plaintext provider api key to be rejected")
	}
}

type fakeResolvedPolicyRow struct {
	providerAPIKey string
	masterKey      []byte
}

func (r fakeResolvedPolicyRow) SecretMasterKey() []byte {
	return r.masterKey
}

func (r fakeResolvedPolicyRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = "users_by_ids_v1"
	*(dest[1].(*string)) = "Users By IDs"
	*(dest[2].(*string)) = "GET"
	*(dest[3].(*string)) = "/v1/users/by-ids"
	*(dest[4].(*string)) = "GET"
	*(dest[5].(*string)) = "/base/apitools/usersByIdRestIds"
	*(dest[6].(*[]byte)) = []byte(`["userIds"]`)
	*(dest[7].(*[]byte)) = []byte(`["userIds"]`)
	*(dest[8].(*[]byte)) = []byte(`["proxyUrl","auth_token"]`)
	*(dest[9].(*[]byte)) = []byte(`{"resFormat":"json"}`)
	*(dest[10].(*string)) = "provider_credential_fapi_uk"
	*(dest[11].(*string)) = "published"
	*(dest[12].(*int)) = 1
	*(dest[13].(*string)) = "fapi.uk"
	*(dest[14].(*string)) = r.providerAPIKey
	return nil
}
