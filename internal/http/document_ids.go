package http

import (
	"net/http"
	"strings"
)

func parseDocumentIDs(r *http.Request) []string {
	raw := strings.TrimSpace(r.URL.Query().Get("document_ids"))
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	seen := make(map[string]bool, len(parts))
	documentIDs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || seen[part] {
			continue
		}
		seen[part] = true
		documentIDs = append(documentIDs, part)
	}
	if len(documentIDs) == 0 {
		return nil
	}
	return documentIDs
}

func firstDocumentID(documentIDs []string, fallback string) string {
	if len(documentIDs) > 0 && strings.TrimSpace(documentIDs[0]) != "" {
		return strings.TrimSpace(documentIDs[0])
	}
	return strings.TrimSpace(fallback)
}

func intersectDocumentIDs(left []string, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}

	allowed := make(map[string]bool, len(right))
	for _, item := range right {
		item = strings.TrimSpace(item)
		if item != "" {
			allowed[item] = true
		}
	}

	seen := make(map[string]bool, len(left))
	result := make([]string, 0, len(left))
	for _, item := range left {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] || !allowed[item] {
			continue
		}
		seen[item] = true
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
