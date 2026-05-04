package service

import (
	"regexp"
	"strings"
)

const sensitiveRedactionToken = "[REDACTADO]"

var sensitiveTextPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?im)^.*\bcurl\b.*\bAuthorization\b.*$`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`),
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`),
	regexp.MustCompile(`(?i)\bAuthorization\s*:\s*Bearer\s+[A-Za-z0-9._~+/=-]{12,}`),
	regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{20,}`),
	regexp.MustCompile(`(?i)\b(password|passwd|pwd)\s*[:=]\s*["']?[^"'\s;,\r\n]+`),
	regexp.MustCompile(`(?i)\b(api[_-]?key|apikey|client[_-]?secret|access[_-]?key|private[_-]?key|secret)\s*[:=]\s*["']?[^"'\s;,\r\n]+`),
	regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\s*[:|,;]\s*[^ \t\r\n]+`),
	regexp.MustCompile(`\b[A-Za-z0-9._~+/=-]{41,}\b`),
}

func SanitizeSensitiveText(text string) string {
	return sanitizeSensitiveText(text)
}

func SanitizeSensitiveTextWithCount(text string) (string, int) {
	return sanitizeSensitiveTextWithCount(text)
}

func sanitizeSensitiveText(text string) string {
	sanitized, _ := sanitizeSensitiveTextWithCount(text)
	return sanitized
}

func sanitizeSensitiveTextWithCount(text string) (string, int) {
	if strings.TrimSpace(text) == "" {
		return text, 0
	}

	total := 0
	sanitized := text
	for _, pattern := range sensitiveTextPatterns {
		matches := pattern.FindAllStringIndex(sanitized, -1)
		if len(matches) == 0 {
			continue
		}
		total += len(matches)
		sanitized = pattern.ReplaceAllString(sanitized, sensitiveRedactionToken)
	}
	return sanitized, total
}
