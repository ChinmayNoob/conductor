// Package security holds Conductor's authentication and transport security:
// API keys for the HTTP API, and a shared token plus optional (m)TLS for gRPC
// between components.
package security

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// apiKeyPrefix makes Conductor keys recognizable, e.g. to secret scanners.
const apiKeyPrefix = "cnd_"

// GenerateAPIKey returns a new random API key with 256 bits of entropy.
func GenerateAPIKey() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // never fails; see crypto/rand docs
	return apiKeyPrefix + hex.EncodeToString(b)
}

// HashAPIKey returns the hash stored for a key. Generated keys are random, so
// a fast hash is enough; there is nothing to brute-force.
func HashAPIKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// KeyPrefix returns the start of a key, which is safe to show in listings.
func KeyPrefix(key string) string {
	const n = 12
	if len(key) <= n {
		return key
	}
	return key[:n]
}
