package shared

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

// #nosec G101 -- versioned ciphertext prefix, not a credential.
const secretCipherPrefix = "enc:v1:gcm:"

var (
	ErrSecretMasterKeyRequired = errors.New("COMMON_SECRET_MASTER_KEY is required")
	ErrInvalidSecretMasterKey  = errors.New("COMMON_SECRET_MASTER_KEY must be 32 bytes or base64-encoded 32 bytes")
	ErrInvalidSecretCiphertext = errors.New("secret ciphertext must use enc:v1:gcm format")
)

func ParseSecretMasterKey(value string) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, ErrSecretMasterKeyRequired
	}
	if len(trimmed) == 32 {
		return []byte(trimmed), nil
	}

	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		decoded, err := encoding.DecodeString(trimmed)
		if err != nil {
			continue
		}
		if len(decoded) == 32 {
			return decoded, nil
		}
	}

	return nil, ErrInvalidSecretMasterKey
}

func EncryptSecretValue(key []byte, plaintext string) (string, error) {
	aead, err := newSecretAEAD(key)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	sealed := aead.Seal(nil, nonce, []byte(plaintext), nil)
	payload := append(append(make([]byte, 0, len(nonce)+len(sealed)), nonce...), sealed...)
	return secretCipherPrefix + base64.StdEncoding.EncodeToString(payload), nil
}

func DecryptSecretValue(key []byte, ciphertext string) (string, error) {
	aead, err := newSecretAEAD(key)
	if err != nil {
		return "", err
	}

	encoded := strings.TrimSpace(ciphertext)
	if !strings.HasPrefix(encoded, secretCipherPrefix) {
		return "", ErrInvalidSecretCiphertext
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, secretCipherPrefix))
	if err != nil {
		return "", fmt.Errorf("decode ciphertext: %w", err)
	}
	if len(payload) < aead.NonceSize() {
		return "", ErrInvalidSecretCiphertext
	}

	nonce := payload[:aead.NonceSize()]
	sealed := payload[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt ciphertext: %w", err)
	}
	return string(plaintext), nil
}

func newSecretAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, ErrInvalidSecretMasterKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
