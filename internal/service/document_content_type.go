package service

import (
	"regexp"
	"strings"

	"auradb-pipeline/internal/repository/postgres"
)

type DocumentContentType string

const (
	DocumentContentTypeTechnical DocumentContentType = "technical"
	DocumentContentTypeNarrative DocumentContentType = "narrative"
	DocumentContentTypeTable     DocumentContentType = "table"
)

var credentialHintPattern = regexp.MustCompile(`(?i)\b(api[_-]?key|apikey|secret|password|passwd|pwd|token|bearer|client[_-]?secret|access[_-]?key|private[_-]?key)\b\s*[:=]?\s*["']?[A-Za-z0-9._~+/=-]{8,}`)

func DetectDocumentContentTypeFromChunks(chunks []postgres.SearchResult) DocumentContentType {
	return DetectDocumentContentType(joinChunkContents(chunks, 12000))
}

func DetectDocumentContentType(text string) DocumentContentType {
	normalized := NormalizeSearchText(text)
	if normalized == "" {
		return DocumentContentTypeNarrative
	}
	if containsWholeNormalizedTerm(normalized, "docker") ||
		containsWholeNormalizedTerm(normalized, "curl") ||
		containsWholeNormalizedTerm(normalized, "git") ||
		containsWholeNormalizedTerm(normalized, "powershell") ||
		containsWholeNormalizedTerm(normalized, "token") ||
		strings.Contains(normalized, "authorization") ||
		strings.Contains(normalized, "api key") ||
		strings.Contains(normalized, "apikey") ||
		strings.Contains(normalized, "dockerfile") {
		return DocumentContentTypeTechnical
	}
	if containsWholeNormalizedTerm(normalized, "columnas") ||
		containsWholeNormalizedTerm(normalized, "filas") {
		return DocumentContentTypeTable
	}
	if containsLongParagraph(text) {
		return DocumentContentTypeNarrative
	}
	return DocumentContentTypeNarrative
}

func ShouldUseTechnicalDocumentInterpretation(question string, queryType string) bool {
	normalized := NormalizeSearchText(question)
	if queryType == "summary" || queryType == "structure" {
		return true
	}
	return containsAny(normalized,
		"que contiene",
		"que hay",
		"contenido del documento",
		"proposito",
		"objetivo",
		"para que sirve",
		"instrucciones",
		"acciones",
	)
}

func BuildTechnicalDocumentAnswer(question string, chunks []postgres.SearchResult) string {
	_ = question
	text := joinChunkContents(chunks, 20000)
	if strings.TrimSpace(text) == "" {
		return ""
	}
	sanitizedText, redactedCount := sanitizeSensitiveTextWithCount(text)
	text = sanitizedText

	actions := inferredTechnicalActions(text)
	credentialsFound := redactedCount > 0 || documentHasCredentialHints(text)

	var builder strings.Builder
	builder.WriteString("El documento contiene instrucciones técnicas para:\n")
	for _, action := range actions {
		builder.WriteString("- ")
		builder.WriteString(action)
		builder.WriteString("\n")
	}

	if credentialsFound {
		builder.WriteString("\nTambién contiene credenciales o tokens sensibles, por lo que fueron ocultados por seguridad.")
	} else {
		builder.WriteString("\nNo se detectaron credenciales o tokens sensibles evidentes en el contenido analizado.")
	}

	return strings.TrimSpace(builder.String())
}

func joinChunkContents(chunks []postgres.SearchResult, maxRunes int) string {
	var builder strings.Builder
	for _, chunk := range chunks {
		content := strings.TrimSpace(chunk.Content)
		if content == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(content)
		if maxRunes > 0 && len([]rune(builder.String())) >= maxRunes {
			break
		}
	}
	text := builder.String()
	if maxRunes <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes])
	}
	return text
}

func containsWholeNormalizedTerm(text string, term string) bool {
	for _, word := range strings.Fields(text) {
		if word == term {
			return true
		}
	}
	return false
}

func containsLongParagraph(text string) bool {
	for _, paragraph := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n\n") {
		if len(strings.Fields(paragraph)) >= 45 {
			return true
		}
	}
	return false
}

func identifiedTechnicalCommands(text string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	commands := make([]string, 0, limit)
	seen := make(map[string]bool)
	for _, line := range lines {
		line = cleanTechnicalCommandLine(line)
		if line == "" || !looksLikeTechnicalCommand(line) {
			continue
		}
		key := NormalizeSearchText(line)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		commands = append(commands, line)
		if len(commands) == limit {
			break
		}
	}
	return commands
}

func cleanTechnicalCommandLine(line string) string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "-")
	line = strings.TrimPrefix(line, "*")
	line = strings.TrimSpace(line)
	line = strings.Trim(line, "`")
	line = strings.TrimSpace(line)
	if len([]rune(line)) > 220 {
		line = string([]rune(line)[:220]) + "..."
	}
	return line
}

func looksLikeTechnicalCommand(line string) bool {
	normalized := NormalizeSearchText(line)
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "docker ") ||
		strings.HasPrefix(normalized, "curl ") ||
		strings.HasPrefix(normalized, "git ") ||
		strings.HasPrefix(normalized, "powershell ") {
		return true
	}
	return strings.Contains(normalized, " curl ") ||
		strings.Contains(normalized, " docker ") ||
		strings.Contains(normalized, " git ") ||
		strings.Contains(normalized, " powershell ")
}

func inferredTechnicalActions(text string) []string {
	normalized := NormalizeSearchText(text)
	candidates := []struct {
		terms  []string
		action string
	}{
		{terms: []string{"tenant", "tenant id"}, action: "obtener el ID del tenant"},
		{terms: []string{"token", "bearer", "jwt"}, action: "generar un token de autenticación"},
		{terms: []string{"subir", "upload", "documento", "documents upload"}, action: "subir documentos mediante la API"},
		{terms: []string{"docker", "powershell", "curl", "git"}, action: "usar comandos Docker, PowerShell, curl y Git"},
	}

	actions := make([]string, 0, len(candidates))
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		matched := false
		for _, term := range candidate.terms {
			if strings.Contains(normalized, NormalizeSearchText(term)) {
				matched = true
				break
			}
		}
		if matched && !seen[candidate.action] {
			seen[candidate.action] = true
			actions = append(actions, candidate.action)
		}
	}
	if len(actions) == 0 {
		actions = append(actions, "seguir pasos técnicos descritos en el documento")
	}
	return actions
}

func documentHasCredentialHints(text string) bool {
	if credentialHintPattern.MatchString(text) {
		return true
	}
	normalized := NormalizeSearchText(text)
	return strings.Contains(text, sensitiveRedactionToken) ||
		strings.Contains(normalized, "bearer token") ||
		strings.Contains(normalized, "authorization bearer") ||
		strings.Contains(normalized, "client secret") ||
		strings.Contains(normalized, "api key")
}
