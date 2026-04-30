package http

import (
	"strings"
	"testing"
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
