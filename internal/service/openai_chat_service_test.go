package service

import (
	"strings"
	"testing"
)

func TestDetailedExplanationPromptDefinesProfessionalContentFormats(t *testing.T) {
	prompt := detailedExplanationSystemPrompt + "\n" + detailedExplanationUserRules

	for _, expected := range []string{
		"lenguaje natural",
		"profesional",
		"Explica directamente",
		"Introducción",
		"Desarrollo",
		"Conclusión",
		"herramientas",
		"orden lógico",
		"datos sensibles",
		"columnas",
		"registros",
		"patrones",
		"Fuente",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("detailed explanation prompt missing %q", expected)
		}
	}

	for _, forbiddenInstruction := range []string{
		"chunks recuperados",
		"contenido procesado",
		"material suficiente",
		"consulta planteada",
		"ejes recuperados",
		"sistema",
		"modelo",
		"respuesta generada",
	} {
		if !strings.Contains(strings.ToLower(prompt), strings.ToLower(forbiddenInstruction)) {
			t.Fatalf("detailed explanation prompt does not explicitly prohibit %q", forbiddenInstruction)
		}
	}
}
