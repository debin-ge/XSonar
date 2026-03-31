package shared

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParseSecretMasterKeyAcceptsRaw32ByteValue(t *testing.T) {
	raw := "0123456789abcdef0123456789abcdef"

	key, err := ParseSecretMasterKey(raw)
	if err != nil {
		t.Fatalf("ParseSecretMasterKey returned error: %v", err)
	}
	if string(key) != raw {
		t.Fatalf("unexpected parsed key: %q", string(key))
	}
}

func TestParseSecretMasterKeyAcceptsBase64Value(t *testing.T) {
	raw := []byte("0123456789abcdef0123456789abcdef")
	encoded := base64.StdEncoding.EncodeToString(raw)

	key, err := ParseSecretMasterKey(encoded)
	if err != nil {
		t.Fatalf("ParseSecretMasterKey returned error: %v", err)
	}
	if string(key) != string(raw) {
		t.Fatalf("unexpected parsed key: %q", string(key))
	}
}

func TestEncryptSecretValueRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")

	first, err := EncryptSecretValue(key, "tenant-secret")
	if err != nil {
		t.Fatalf("EncryptSecretValue returned error: %v", err)
	}
	second, err := EncryptSecretValue(key, "tenant-secret")
	if err != nil {
		t.Fatalf("EncryptSecretValue returned error: %v", err)
	}
	if first == second {
		t.Fatal("expected different ciphertexts for different nonces")
	}
	if !strings.HasPrefix(first, "enc:v1:gcm:") {
		t.Fatalf("expected encrypted prefix, got %q", first)
	}

	plaintext, err := DecryptSecretValue(key, first)
	if err != nil {
		t.Fatalf("DecryptSecretValue returned error: %v", err)
	}
	if plaintext != "tenant-secret" {
		t.Fatalf("unexpected plaintext: %q", plaintext)
	}
}

func TestDecryptSecretValueRejectsPlaintext(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")

	if _, err := DecryptSecretValue(key, "tenant-secret"); err == nil {
		t.Fatal("expected plaintext secret to be rejected")
	}
}
