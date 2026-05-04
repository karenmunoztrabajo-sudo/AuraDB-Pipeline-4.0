package service

import (
	"strings"
	"testing"
)

func TestSanitizeSensitiveTextRedactsSecrets(t *testing.T) {
	input := strings.Join([]string{
		"OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyz1234567890",
		"Authorization: Bearer abcdefghijklmnopqrstuvwxyz1234567890",
		"password=supersecret",
		"admin@example.com:secretpass",
		"curl -H \"Authorization: Bearer abcdefghijklmnopqrstuvwxyz1234567890\" https://api.example.test",
		"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
		"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890",
	}, "\n")

	sanitized, count := SanitizeSensitiveTextWithCount(input)

	if count == 0 {
		t.Fatal("expected redactions")
	}
	if strings.Contains(sanitized, "sk-abcdefghijklmnopqrstuvwxyz") ||
		strings.Contains(sanitized, "Bearer abcdefghijklmnopqrstuvwxyz") ||
		strings.Contains(sanitized, "supersecret") ||
		strings.Contains(sanitized, "admin@example.com:secretpass") ||
		strings.Contains(sanitized, "eyJhbGciOiJIUzI1Ni") {
		t.Fatalf("sensitive value leaked:\n%s", sanitized)
	}
	if !strings.Contains(sanitized, sensitiveRedactionToken) {
		t.Fatalf("expected redaction token in:\n%s", sanitized)
	}
}
