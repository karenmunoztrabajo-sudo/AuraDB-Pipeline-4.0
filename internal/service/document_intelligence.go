package service

import (
	"strings"
)

type DocumentTask string

const (
	DocumentTaskSummary          DocumentTask = "summary"
	DocumentTaskQuestion         DocumentTask = "question"
	DocumentTaskDates            DocumentTask = "extract_dates"
	DocumentTaskParts            DocumentTask = "extract_parts"
	DocumentTaskEconomicValues   DocumentTask = "extract_economic_values"
	DocumentTaskObligations      DocumentTask = "identify_obligations"
	DocumentTaskContradictions   DocumentTask = "detect_contradictions"
	DocumentTaskLegalAnalysis    DocumentTask = "legal_analysis"
	DocumentTaskCompareDocuments DocumentTask = "compare_documents"
)

type AIAnswerProvider interface {
	Answer(question string, context string, task DocumentTask) (string, error)
}

func DocumentTaskForQuery(query string) DocumentTask {
	normalized := NormalizeSearchText(query)
	switch {
	case containsAnyDocumentTaskTerm(normalized, "fecha", "fechas", "vencimiento", "plazo", "dia", "mes", "ano"):
		return DocumentTaskDates
	case containsAnyDocumentTaskTerm(normalized, "parte", "partes", "contratante", "demandante", "demandado", "firmante"):
		return DocumentTaskParts
	case containsAnyDocumentTaskTerm(normalized, "valor", "monto", "precio", "dinero", "pago", "cuota", "economico", "economicos", "usd", "cop"):
		return DocumentTaskEconomicValues
	case containsAnyDocumentTaskTerm(normalized, "obligacion", "obligaciones", "debe", "debera", "responsabilidad", "responsabilidades"):
		return DocumentTaskObligations
	case containsAnyDocumentTaskTerm(normalized, "contradiccion", "contradicciones", "inconsistencia", "inconsistencias", "conflicto"):
		return DocumentTaskContradictions
	case containsAnyDocumentTaskTerm(normalized, "juridico", "juridica", "legal", "analisis juridico", "analisis legal"):
		return DocumentTaskLegalAnalysis
	case containsAnyDocumentTaskTerm(normalized, "comparar", "compara", "comparacion", "diferencias", "otro documento"):
		return DocumentTaskCompareDocuments
	case QueryTypeForQuery(query) == "summary":
		return DocumentTaskSummary
	default:
		return DocumentTaskQuestion
	}
}

func containsAnyDocumentTaskTerm(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, term) {
			return true
		}
	}
	return false
}
