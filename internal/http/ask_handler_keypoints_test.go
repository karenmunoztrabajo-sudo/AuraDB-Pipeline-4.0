package http

import (
	"strings"
	"testing"

	"auradb-pipeline/internal/repository/postgres"
)

func TestRewriteKeyPointsAnswerBuildsGroupedPoints(t *testing.T) {
	chunks := []string{
		"Introducción a AuraDB Pipeline\nAuraDB Pipeline permite cargar documentos al sistema para procesarlos en el backend. El sistema extrae texto del documento y prepara ese contenido para responder preguntas. La carga del documento inicia el flujo de procesamiento.",
		"Procesamiento documental\nEl backend divide el texto extraído en fragmentos para facilitar búsquedas y respuestas. Los fragmentos permiten consultar información específica sin revisar todo el archivo.",
		"Consulta inteligente\nEl usuario puede pedir resúmenes, puntos clave y respuestas sobre la información procesada. La búsqueda recupera fragmentos útiles para construir una respuesta clara.",
		"Procesamiento documental\nEl backend divide el texto extraído en fragmentos para facilitar búsquedas y respuestas.",
	}

	answer := rewriteKeyPointsAnswer(chunks)

	if !strings.HasPrefix(answer, "Puntos clave del documento\n\n") {
		t.Fatalf("unexpected title:\n%s", answer)
	}
	if strings.Contains(strings.ToLower(answer), strings.Join([]string{"se", "relaciona", "con"}, " ")) {
		t.Fatalf("answer contains forbidden phrase:\n%s", answer)
	}
	if strings.Count(answer, "\n\n") > 5 {
		t.Fatalf("answer should contain at most five points:\n%s", answer)
	}
	for _, forbidden := range []string{
		"Introducción a AuraDB Pipeline",
		"Procesamiento documental\n",
		"Consulta inteligente\n",
		"AuraDB Pipeline permite cargar documentos al sistema para procesarlos en el backend",
	} {
		if strings.Contains(answer, forbidden) {
			t.Fatalf("answer copied a title or full source phrase %q:\n%s", forbidden, answer)
		}
	}
	if !strings.Contains(answer, "\nEl documento") {
		t.Fatalf("point explanation missing paragraph after title:\n%s", answer)
	}
}

func TestRewriteKeyPointsAnswerReturnsEmptyWithoutUsefulContent(t *testing.T) {
	answer := rewriteKeyPointsAnswer([]string{
		"Resumen",
		"1. Introducción",
		"Hoja: Datos",
		"Columnas: A | B | C",
	})

	if answer != "" {
		t.Fatalf("expected empty answer for headings and list lines, got:\n%s", answer)
	}
}

func TestAppendSourcesToAnswerAddsSingleExcelFileSource(t *testing.T) {
	answer := appendSourcesToAnswer("El archivo tiene las columnas: Cliente, Valor y Fecha.", []askSource{
		{DocumentName: "PruebaExcel.xlsx"},
	})

	expected := "El archivo tiene las columnas: Cliente, Valor y Fecha.\n\nFuente: PruebaExcel.xlsx"
	if answer != expected {
		t.Fatalf("unexpected sourced answer:\n%s", answer)
	}
}

func TestAppendSourcesToAnswerAddsExcelSheetWhenKnown(t *testing.T) {
	answer := appendSourcesToAnswer("El archivo contiene 3 registros.", []askSource{
		{DocumentName: "PruebaExcel.xlsx", SheetName: "Hoja1"},
	})

	expected := "El archivo contiene 3 registros.\n\nFuente: PruebaExcel.xlsx — Hoja: Hoja1"
	if answer != expected {
		t.Fatalf("unexpected sourced answer:\n%s", answer)
	}
}

func TestAppendSourcesToAnswerAddsMultipleSources(t *testing.T) {
	answer := appendSourcesToAnswer("Respuesta unificada.", []askSource{
		{DocumentName: "archivo1.pdf"},
		{DocumentName: "archivo2.xlsx"},
	})

	expected := "Respuesta unificada.\n\nFuente: archivo1.pdf; archivo2.xlsx"
	if answer != expected {
		t.Fatalf("unexpected sourced answer:\n%s", answer)
	}
}

func TestCleanDisplayFormattingFixesFileExtensionsAndCase(t *testing.T) {
	answer := cleanUserVisibleAnswer("El dOCUMENTO Aura Db Pipeline. docx usa PruebaExcel. xlsx")

	expected := "El Documento AuraDB Pipeline.docx usa PruebaExcel.xlsx"
	if answer != expected {
		t.Fatalf("unexpected cleaned answer: %q", answer)
	}
}

func TestCleanOverviewDescriptionRemovesNoise(t *testing.T) {
	answer := cleanOverviewDescription("DOCUMENTO BASE\nnombre del proyecto AuraDB Pipeline 2")

	expected := "información base del proyecto AuraDB Pipeline"
	if answer != expected {
		t.Fatalf("unexpected overview: %q", answer)
	}
}

func TestPolishFinalPresentationRemovesRawExcelAndRewritesMixedAnswer(t *testing.T) {
	answer := polishFinalPresentation(`Documento tipo: Excel
Hoja: Hoja1
Columnas: Cliente, Valor y Fecha
Fila 1: Cliente = Ana, Valor = 10
El documento Documento Base AuraDB Pipeline.docx contiene nombre del proyecto AuraDB Pipeline 2.`, []askSource{
		{
			DocumentName: "Documento Base AuraDB Pipeline.docx",
			Excerpt:      "AuraDB Pipeline permite subir archivos y consultarlos mediante preguntas.",
		},
		{
			DocumentName: "PruebaExcel.xlsx",
			SheetName:    "Hoja1",
			Excerpt:      "Documento tipo: Excel\nHoja: Hoja1\nColumnas: Cliente, Valor y Fecha\nTotal de filas de datos: 2\nFila 1: Cliente = Ana, Valor = 10",
		},
	})

	expected := "El documento Documento Base AuraDB Pipeline.docx contiene información base del proyecto AuraDB Pipeline como un sistema de análisis documental inteligente que permite subir archivos y consultarlos mediante preguntas.\n\n" +
		"El archivo PruebaExcel.xlsx contiene 2 registros con las columnas Cliente, Valor y Fecha.\n\n" +
		"En conjunto, los documentos muestran tanto la lógica del sistema como ejemplos de datos que pueden ser procesados por la plataforma."
	if answer != expected {
		t.Fatalf("unexpected polished answer:\n%s", answer)
	}
}

func TestAnswerLooksLikeRawDocumentTextDetectsUppercaseTitle(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "COMPENDIO HISTÓRICO DEL TRANSPORTE\nEl transporte evolucionó con las civilizaciones y transformó la economía."},
	}

	if !answerLooksLikeRawDocumentText("COMPENDIO HISTÓRICO DEL TRANSPORTE", chunks) {
		t.Fatal("expected raw text detection for uppercase copied title")
	}
}

func TestBuildInterpretedFallbackForTransportDocument(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "COMPENDIO HISTÓRICO DEL TRANSPORTE\nLa movilidad humana inicia con tracción humana y animal. La rueda permitió infraestructura vial y avances tecnológicos."},
	}

	answer := buildInterpretedFallback(chunks, nil)

	if strings.Contains(answer, "COMPENDIO HISTÓRICO") {
		t.Fatalf("fallback copied raw title:\n%s", answer)
	}
	if !strings.Contains(answer, "evolución de los sistemas de transporte") {
		t.Fatalf("fallback did not interpret transport content:\n%s", answer)
	}
}

func TestIsRawChunkAnswerSimpleHeuristic(t *testing.T) {
	answer := "COMPENDIO HISTÓRICO DEL TRANSPORTE\n\nLos Albores de la Movilidad humana muestran datos crudos del documento."
	if !isRawChunkAnswer(answer) {
		t.Fatal("expected raw chunk answer detection")
	}
}

func TestSanitizeAnswerPreservingSourceLineDoesNotRedactFilename(t *testing.T) {
	answer := "Token sk-abcdefghijklmnopqrstuvwxyz1234567890\n\nFuente: Historia_Evolucion_Transporte_Academico.pdf"
	sanitized, redacted := sanitizeAnswerPreservingSourceLine(answer)

	if redacted == 0 {
		t.Fatal("expected body redaction")
	}
	if !strings.Contains(sanitized, "Fuente: Historia_Evolucion_Transporte_Academico.pdf") {
		t.Fatalf("source filename was not preserved:\n%s", sanitized)
	}
}
