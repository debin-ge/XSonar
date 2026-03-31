package shared

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func NewID(prefix string) string {
	return prefix + "_" + randomHex(8)
}

func NewSecret(prefix string) string {
	return prefix + "_" + randomHex(16)
}

func randomHex(byteLength int) string {
	buffer := make([]byte, byteLength)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}
