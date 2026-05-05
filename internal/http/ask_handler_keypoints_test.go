package http

import (
	"strconv"
	"strings"
	"testing"
	"time"

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
	answer := appendSourcesToAnswer("El documento archivo1.pdf describe la base. El archivo archivo2.xlsx contiene datos.", []askSource{
		{DocumentName: "archivo1.pdf"},
		{DocumentName: "archivo2.xlsx"},
	})

	expected := "El documento archivo1.pdf [A] contiene información narrativa del documento consultado.\n\n" +
		"El archivo archivo2.xlsx [B] contiene información estructurada tipo datos.\n\n" +
		"En conjunto, los documentos cargados reúnen información narrativa y datos estructurados consultables.\n\n" +
		"Fuentes:\n[A] archivo1.pdf\n[B] archivo2.xlsx"
	if answer != expected {
		t.Fatalf("unexpected sourced answer:\n%s", answer)
	}
}

func TestAppendCitedSourcesToAnswerLabelsThreeDocumentsAndExcelSheet(t *testing.T) {
	answer := appendCitedSourcesToAnswer(
		"El documento A.pdf explica. El documento B.pdf analiza. El archivo Datos.xlsx contiene.",
		[]string{"A.pdf", "B.pdf", "Datos.xlsx — Hoja: Hoja1"},
	)

	for _, expected := range []string{
		"A.pdf [A]",
		"B.pdf [B]",
		"Datos.xlsx [C]",
		"Fuentes:",
		"[A] A.pdf",
		"[B] B.pdf",
		"Datos.xlsx — Hoja: Hoja1 [C]",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("citation answer missing %q:\n%s", expected, answer)
		}
	}
}

func TestCleanDisplayFormattingFixesFileExtensionsAndCase(t *testing.T) {
	answer := cleanUserVisibleAnswer("El dOCUMENTO Aura Db Pipeline. docx usa PruebaExcel. xlsx")

	expected := "El Documento AuraDB Pipeline.docx usa PruebaExcel.xlsx"
	if answer != expected {
		t.Fatalf("unexpected cleaned answer: %q", answer)
	}
}

func TestCleanDisplayFormattingRestoresCommonSpanishAccents(t *testing.T) {
	answer := cleanUserVisibleAnswer("La evolucion historica usa tecnologias para la organizacion de un sistema movil.")

	expected := "La evolución histórica usa tecnologías para la organización de un sistema móvil."
	if answer != expected {
		t.Fatalf("unexpected accented answer: %q", answer)
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

func TestBuildDetailedExplanationFallbackUsesNarrativeStructure(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "La lógica algorítmica permite descomponer problemas complejos en pasos finitos y ordenados antes de escribir código."},
		{Content: "Los arreglos almacenan elementos del mismo tipo y facilitan el acceso aleatorio mediante índices."},
		{Content: "La eficiencia de un algoritmo se mide por su complejidad temporal y espacial."},
	}

	answer := buildDetailedExplanationFallback("explica el documento", chunks, []askSource{{DocumentName: "logica.pdf"}})

	for _, expected := range []string{"Introducción", "Desarrollo", "Conclusión", "Fuente: logica.pdf"} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("detailed fallback missing %q:\n%s", expected, answer)
		}
	}
	for _, forbidden := range forbiddenDetailedExpressions {
		if strings.Contains(strings.ToLower(answer), strings.ToLower(forbidden)) {
			t.Fatalf("detailed fallback contains forbidden expression %q:\n%s", forbidden, answer)
		}
	}
}

func TestBuildDetailedExplanationFallbackUsesSpreadsheetStructure(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "Documento tipo: Excel\nHoja: Ventas\nColumnas: Cliente | Valor | Fecha\nTotal de filas de datos: 3\nFila 1: Cliente = Ana | Valor = 10 | Fecha = 2026-01-01"},
	}

	answer := buildDetailedExplanationFallback("analiza este archivo", chunks, []askSource{{DocumentName: "ventas.xlsx", SheetName: "Ventas"}})

	for _, expected := range []string{"Introducción", "Desarrollo", "Columnas: Cliente | Valor | Fecha", "Número de registros: 3", "Hallazgos principales", "Conclusión", "Fuente: ventas.xlsx"} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("spreadsheet detailed fallback missing %q:\n%s", expected, answer)
		}
	}
}

func TestBuildDetailedExplanationFallbackUsesTechnicalStructureAndRedacts(t *testing.T) {
	chunks := []postgres.SearchResult{
		{Content: "Use curl para llamar el endpoint de upload con Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456. Incluya tenant_id y el documento."},
	}

	answer := buildDetailedExplanationFallback("detalla el documento", chunks, []askSource{{DocumentName: "api.txt"}})

	for _, expected := range []string{"Introducción", "Desarrollo", "Componentes principales", "Flujo técnico", "Riesgos o datos sensibles si existen", "Conclusión", "Fuente: api.txt"} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("technical detailed fallback missing %q:\n%s", expected, answer)
		}
	}
	if strings.Contains(answer, "abcdefghijklmnopqrstuvwxyz123456") {
		t.Fatalf("technical detailed fallback leaked token:\n%s", answer)
	}
}

func TestIsCrossDocumentAnalysisQuestionDetectsRelationshipIntent(t *testing.T) {
	for _, question := range []string{
		"¿Qué relación hay entre estos documentos?",
		"cómo se relacionan estos archivos",
		"qué conexión hay entre ambos",
		"qué tienen en común",
	} {
		if !isCrossDocumentAnalysisQuestion(question) {
			t.Fatalf("expected cross-document intent for %q", question)
		}
	}
}

func TestBuildCrossDocumentAnalysisAnswerUsesRequiredFormatAndSources(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "doc-a",
			Content:    "AuraDB Pipeline es una plataforma de análisis documental que permite subir archivos y consultarlos mediante preguntas.",
		},
		{
			DocumentID: "doc-b",
			Content:    "Documento tipo: Excel\nHoja: Ventas\nColumnas: Cliente | Valor | Fecha\nTotal de filas de datos: 3\nFila 1: Cliente = Ana | Valor = 10 | Fecha = 2026-01-01",
		},
	}

	answer := handler.buildCrossDocumentAnalysisAnswer(nil, "", chunks)
	answer = appendSourcesToAnswer(answer, []askSource{
		{DocumentID: "doc-a", DocumentName: "sistema.pdf"},
		{DocumentID: "doc-b", DocumentName: "ventas.xlsx", SheetName: "Ventas"},
	})

	for _, expected := range []string{
		"El documento doc-a aborda",
		"El archivo doc-b contiene",
		"La relación entre ambos radica en",
		"En conjunto, estos documentos muestran",
		"Fuentes:",
		"[A] sistema.pdf",
		"ventas.xlsx — Hoja: Ventas [B]",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("cross-document answer missing %q:\n%s", expected, answer)
		}
	}
	if strings.Contains(answer, "Documento tipo: Excel") || strings.Contains(answer, "Fila 1:") {
		t.Fatalf("cross-document answer copied raw source text:\n%s", answer)
	}
}

func TestCleanDisplayFormattingFixesCrossDocumentVerbPhrases(t *testing.T) {
	answer := cleanDisplayFormatting("El documento X describe analiza algo. El documento Y describe contiene datos. El documento Z describe explica pasos.")

	expected := "El documento X analiza algo. El documento Y contiene datos. El documento Z explica pasos."
	if answer != expected {
		t.Fatalf("unexpected cleaned cross-document phrasing: %q", answer)
	}
}

func TestBuildCrossDocumentAnalysisAnswerNarrativeAndTableUsesSpecificRelation(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "Historia_Evolucion_Transporte_Academico.pdf",
			Content:    "COMPENDIO HISTÓRICO DEL TRANSPORTE\nLa movilidad humana inicia con tracción humana y animal. La rueda permitió infraestructura vial y avances tecnológicos.",
		},
		{
			DocumentID: "PruebaExcel.xlsx",
			Content:    "Documento tipo: Excel\nHoja: Hoja1\nColumnas: Cliente | Valor | Fecha\nTotal de filas de datos: 2\nFila 1: Cliente = Ana | Valor = 10 | Fecha = 2026-01-01",
		},
	}

	answer := handler.buildCrossDocumentAnalysisAnswer(nil, "", chunks)
	answer = appendSourcesToAnswer(answer, []askSource{
		{DocumentID: "Historia_Evolucion_Transporte_Academico.pdf", DocumentName: "Historia_Evolucion_Transporte_Academico.pdf"},
		{DocumentID: "PruebaExcel.xlsx", DocumentName: "PruebaExcel.xlsx", SheetName: "Hoja1"},
	})

	for _, expected := range []string{
		"El documento Historia_Evolucion_Transporte_Academico.pdf [A] aborda la evolución histórica de los sistemas de transporte, desde sus primeras formas hasta la movilidad contemporánea.",
		"El archivo PruebaExcel.xlsx [B] contiene 2 registros con las columnas Cliente, Valor y Fecha, orientados a consulta y validación.",
		"La relación entre ambos radica en que el primero aporta contexto conceptual, mientras que el segundo representa información estructurada que puede consultarse y analizarse.",
		"En conjunto, estos documentos muestran cómo el sistema puede trabajar tanto con contenido narrativo como con datos tabulares.",
		"El documento Historia_Evolucion_Transporte_Academico.pdf [A] aborda",
		"El archivo PruebaExcel.xlsx [B] contiene",
		"Fuentes:",
		"[A] Historia_Evolucion_Transporte_Academico.pdf",
		"PruebaExcel.xlsx — Hoja: Hoja1 [B]",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("cross-document narrative/table answer missing %q:\n%s", expected, answer)
		}
	}
}

func TestConversationMemoryKeepsLastFiveQuestionsAndDocuments(t *testing.T) {
	handler := &AskHandler{}

	for i := 1; i <= 6; i++ {
		handler.rememberConversationTurn("tenant-1", "user-1", "pregunta "+strconv.Itoa(i), "respuesta "+strconv.Itoa(i), []string{"doc-a", "doc-b"}, time.Unix(int64(i), 0))
	}

	memory := handler.memoryByUser[conversationMemoryKey("tenant-1", "user-1")]
	if len(memory.Turns) != 5 {
		t.Fatalf("expected last 5 turns, got %d: %#v", len(memory.Turns), memory.Turns)
	}
	if memory.Turns[0].Question != "pregunta 6" || memory.Turns[4].Question != "pregunta 2" {
		t.Fatalf("unexpected turn order: %#v", memory.Turns)
	}

	documentIDs := handler.conversationDocumentIDs("tenant-1", "user-1")
	expected := []string{"doc-a", "doc-b"}
	if strings.Join(documentIDs, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected remembered document IDs: %#v", documentIDs)
	}
}

func TestConversationMemorySanitizesSecrets(t *testing.T) {
	handler := &AskHandler{}
	handler.rememberConversationTurn(
		"tenant-1",
		"user-1",
		"usa token sk-abcdefghijklmnopqrstuvwxyz1234567890",
		"respuesta con Bearer abcdefghijklmnopqrstuvwxyz123456",
		[]string{"doc-a"},
		time.Now(),
	)

	memory := handler.memoryByUser[conversationMemoryKey("tenant-1", "user-1")]
	if len(memory.Turns) != 1 {
		t.Fatalf("expected one turn, got %#v", memory.Turns)
	}
	if strings.Contains(memory.Turns[0].Question, "sk-abcdefghijklmnopqrstuvwxyz1234567890") ||
		strings.Contains(memory.Turns[0].Answer, "abcdefghijklmnopqrstuvwxyz123456") {
		t.Fatalf("memory stored secret values: %#v", memory.Turns[0])
	}
}

func TestIsConversationFollowUpQuestion(t *testing.T) {
	for _, question := range []string{
		"¿y cuál es el más importante?",
		"de esos, resume el primero",
		"entre ellos cuál conviene",
		"comparalos",
		"explica mejor",
		"amplía eso",
	} {
		if !isConversationFollowUpQuestion(question) {
			t.Fatalf("expected follow-up detection for %q", question)
		}
	}
	if isConversationFollowUpQuestion("qué información hay en los documentos") {
		t.Fatal("did not expect general question to use conversation memory")
	}
}

func TestIsComparativeFollowUpQuestion(t *testing.T) {
	for _, question := range []string{
		"¿cuál es más importante?",
		"cuál pesa más",
		"cuál es mejor",
		"compara estos",
		"diferencias entre ellos",
	} {
		if !isComparativeFollowUpQuestion(question) {
			t.Fatalf("expected comparative follow-up detection for %q", question)
		}
	}
}

func TestConversationDocumentIDsFromResponseUsesSourcesAndChunks(t *testing.T) {
	documentIDs := conversationDocumentIDsFromResponse([]postgres.SearchResult{
		{DocumentID: "doc-b"},
		{DocumentID: "doc-c"},
	}, []askSource{
		{DocumentID: "doc-a"},
		{DocumentID: "doc-b"},
	})

	expected := []string{"doc-a", "doc-b", "doc-c"}
	if strings.Join(documentIDs, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected document IDs from response: %#v", documentIDs)
	}
}

func TestBuildMultiDocumentFollowupComparisonAnswerUsesAllDocuments(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "Historia.pdf",
			Content:    "COMPENDIO HISTÓRICO DEL TRANSPORTE\nLa movilidad humana inicia con tracción humana y animal. La rueda permitió infraestructura vial y avances tecnológicos.",
		},
		{
			DocumentID: "Datos.xlsx",
			Content:    "Documento tipo: Excel\nHoja: Hoja1\nColumnas: Cliente | Valor | Fecha\nTotal de filas de datos: 2\nFila 1: Cliente = Ana | Valor = 10",
		},
	}

	answer := handler.buildMultiDocumentFollowupComparisonAnswer(nil, "", chunks)

	for _, expected := range []string{
		"El documento Historia.pdf",
		"El archivo Datos.xlsx",
		"Comparación",
		"Conclusión",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("follow-up comparison missing %q:\n%s", expected, answer)
		}
	}
}

func TestSelectMultiDocumentCoverageChunksKeepsMinimumPerDocument(t *testing.T) {
	chunks := []postgres.SearchResult{
		{ChunkID: "a3", DocumentID: "doc-a", ChunkIndex: 3, Score: 0.9},
		{ChunkID: "a1", DocumentID: "doc-a", ChunkIndex: 1, Score: 0.8},
		{ChunkID: "a2", DocumentID: "doc-a", ChunkIndex: 2, Score: 0.7},
		{ChunkID: "b1", DocumentID: "doc-b", ChunkIndex: 1, Score: 0.1},
		{ChunkID: "b2", DocumentID: "doc-b", ChunkIndex: 2, Score: 0.2},
	}

	selected := selectMultiDocumentCoverageChunks(chunks, 2, 20)
	counts := map[string]int{}
	for _, chunk := range selected {
		counts[chunk.DocumentID]++
	}

	if counts["doc-a"] < 2 || counts["doc-b"] < 2 {
		t.Fatalf("expected at least 2 chunks per document, got %#v from %#v", counts, selected)
	}
}

func TestEnsureMultiDocumentCoverageAddsMissingDocumentFromDirectChunks(t *testing.T) {
	ranked := []postgres.SearchResult{
		{ChunkID: "a1", DocumentID: "doc-a", ChunkIndex: 1, Score: 0.9},
		{ChunkID: "a2", DocumentID: "doc-a", ChunkIndex: 2, Score: 0.8},
	}
	all := []postgres.SearchResult{
		{ChunkID: "a1", DocumentID: "doc-a", ChunkIndex: 1, Score: 0.9},
		{ChunkID: "a2", DocumentID: "doc-a", ChunkIndex: 2, Score: 0.8},
		{ChunkID: "b1", DocumentID: "doc-b", ChunkIndex: 1, Score: 0.1},
		{ChunkID: "b2", DocumentID: "doc-b", ChunkIndex: 2, Score: 0.2},
	}

	selected := ensureMultiDocumentCoverage(ranked, all, 2, 20)
	counts := map[string]int{}
	for _, chunk := range selected {
		counts[chunk.DocumentID]++
	}

	if counts["doc-b"] < 2 {
		t.Fatalf("expected missing document coverage, got %#v from %#v", counts, selected)
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

func TestSanitizeAnswerPreservingSourceNamesDoesNotRedactOriginalName(t *testing.T) {
	filename := "sk-abcdefghijklmnopqrstuvwxyz1234567890.pdf"
	answer := "El documento " + filename + " describe una guía operativa.\n\nFuente: " + filename

	sanitized, redacted := sanitizeAnswerPreservingSourceLineAndSourceNames(answer, []askSource{
		{DocumentName: filename, DocumentID: "doc-1"},
	})

	if redacted != 0 {
		t.Fatalf("source filename should not be counted as redacted, got %d in:\n%s", redacted, sanitized)
	}
	if strings.Contains(sanitized, "[REDACTADO]") {
		t.Fatalf("source filename was redacted:\n%s", sanitized)
	}
	if strings.Count(sanitized, filename) != 2 {
		t.Fatalf("source filename was not preserved in body and source line:\n%s", sanitized)
	}
}

func TestCrossDocumentNarrativeDescriptionAvoidsUppercasePDFTitle(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "transporte.pdf",
			Content:    "COMPENDIO HISTÓRICO DEL TRANSPORTE\nLa movilidad humana inicia con tracción humana y animal. La rueda permitió infraestructura vial y avances tecnológicos.",
		},
		{
			DocumentID: "datos.xlsx",
			Content:    "Documento tipo: Excel\nHoja: Datos\nColumnas: Medio | Valor\nTotal de filas de datos: 2\nFila 1: Medio = Tren | Valor = 10",
		},
	}

	answer := handler.buildCrossDocumentAnalysisAnswer(nil, "", chunks)

	if strings.Contains(answer, "COMPENDIO HISTÓRICO") || strings.Contains(answer, "describe Compendio") {
		t.Fatalf("cross-document answer copied uppercase title:\n%s", answer)
	}
	if !strings.Contains(answer, "aborda la evolución") {
		t.Fatalf("cross-document answer did not use natural narrative description:\n%s", answer)
	}
}

func TestCrossDocumentNarrativeDescriptionForProximaCentauri(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "Informe_Proxima_Centauri.pdf",
			Content:    "INFORME PRÓXIMA CENTAURI\nLa astronomía moderna estudia exoplanetas cercanos y las características de Próxima Centauri.",
		},
		{
			DocumentID: "Historia_Evolucion_Transporte_Academico.pdf",
			Content:    "COMPENDIO HISTÓRICO DEL TRANSPORTE\nLa movilidad humana inicia con tracción humana y animal. La rueda permitió infraestructura vial y avances tecnológicos.",
		},
	}

	answer := handler.buildCrossDocumentAnalysisAnswer(nil, "", chunks)

	if strings.Contains(answer, "INFORME PRÓXIMA CENTAURI") {
		t.Fatalf("cross-document answer copied uppercase title:\n%s", answer)
	}
	expected := "El documento Informe_Proxima_Centauri.pdf aborda la exploración de exoplanetas y el caso de Próxima Centauri."
	if !strings.Contains(answer, expected) {
		t.Fatalf("cross-document answer missing natural Proxima Centauri description %q:\n%s", expected, answer)
	}
}

func TestCrossDocumentAnalysisForDistinctAcademicTopics(t *testing.T) {
	handler := &AskHandler{}
	chunks := []postgres.SearchResult{
		{
			DocumentID: "Informe_Proxima_Centauri.pdf",
			Content:    "INFORME PRÓXIMA CENTAURI\nLa astronomía moderna estudia exoplanetas cercanos y el caso de Próxima Centauri.",
		},
		{
			DocumentID: "Logica_y_Programacion_Estudio.pdf",
			Content:    "FUNDAMENTOS DE LÓGICA DE PROGRAMACIÓN\nLa lógica de programación permite diseñar algoritmos y usar estructuras de datos como arreglos.",
		},
	}

	answer := handler.buildCrossDocumentAnalysisAnswer(nil, "", chunks)
	answer = appendSourcesToAnswer(answer, []askSource{
		{DocumentID: "Informe_Proxima_Centauri.pdf", DocumentName: "Informe_Proxima_Centauri.pdf"},
		{DocumentID: "Logica_y_Programacion_Estudio.pdf", DocumentName: "Logica_y_Programacion_Estudio.pdf"},
	})

	for _, expected := range []string{
		"El documento Informe_Proxima_Centauri.pdf [A] aborda la exploración de exoplanetas y el caso de Próxima Centauri.",
		"El documento Logica_y_Programacion_Estudio.pdf [B] aborda fundamentos de lógica de programación y estructuras de datos.",
		"La relación entre ambos no está en el contenido temático directo, sino en que ambos presentan conocimiento técnico-académico organizado: uno desde la astronomía y otro desde la informática.",
		"En conjunto, muestran cómo AuraDB puede procesar documentos de áreas distintas y responder sobre cada uno manteniendo sus fuentes.",
		"Fuentes:",
		"[A] Informe_Proxima_Centauri.pdf",
		"[B] Logica_y_Programacion_Estudio.pdf",
	} {
		if !strings.Contains(answer, expected) {
			t.Fatalf("distinct academic topics answer missing %q:\n%s", expected, answer)
		}
	}
	if strings.Contains(answer, "lectura documental") || strings.Contains(answer, "instrucciones y sistema") {
		t.Fatalf("answer contains generic relation wording:\n%s", answer)
	}
}
