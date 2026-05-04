package service

import (
	"strings"
	"testing"

	"auradb-pipeline/internal/repository/postgres"
)

func TestDetectDocumentContentTypeTechnical(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "PowerShell\n$token = 'abc123'\ncurl -H \"Authorization: Bearer $token\" https://api.example.test/documents/upload\ndocker compose up"},
	}

	if got := DetectDocumentContentTypeFromChunks(chunks); got != DocumentContentTypeTechnical {
		t.Fatalf("expected technical, got %s", got)
	}
}

func TestDetectDocumentContentTypeTable(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "Columnas: Cliente, Valor\nTotal de filas de datos: 2\nFila 1: Cliente=Ana | Valor=100"},
	}

	if got := DetectDocumentContentTypeFromChunks(chunks); got != DocumentContentTypeTable {
		t.Fatalf("expected table, got %s", got)
	}
}

func TestBuildTechnicalDocumentAnswerUsesStructuredSections(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "Obtener tenant id\nGenerar token\nTOKEN=secret-value-12345\ncurl -H \"Authorization: Bearer $TOKEN\" https://api.example.test/documents/upload"},
	}

	answer := BuildTechnicalDocumentAnswer("Resume este documento", chunks)

	for _, expected := range []string{
		"El documento contiene instrucciones técnicas para:",
		"ocultados por seguridad",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("expected answer to contain %q:\n%s", expected, answer)
		}
	}
}
