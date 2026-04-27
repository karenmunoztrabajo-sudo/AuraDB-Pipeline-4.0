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
