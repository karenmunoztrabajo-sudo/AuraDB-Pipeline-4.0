package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"auradb-pipeline/internal/config"
)

type OllamaChatService struct {
	baseURL string
	model   string
	client  *http.Client
}

func NewOllamaChatService(cfg config.Config) *OllamaChatService {
	return &OllamaChatService{
		baseURL: strings.TrimRight(cfg.OllamaBaseURL, "/"),
		model:   cfg.OllamaChatModel,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
}

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Options  ollamaChatOptions   `json:"options,omitempty"`
}

type ollamaChatOptions struct {
	NumPredict int `json:"num_predict,omitempty"`
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatResponse struct {
	Message ollamaChatMessage `json:"message"`
	Error   string            `json:"error,omitempty"`
}

type answerPromptConfig struct {
	SystemPrompt string
	UserRules    string
	NumPredict   int
}

type sectionSummaryPromptConfig struct {
	SystemPrompt string
	UserRules    string
	NumPredict   int
}

func (s *OllamaChatService) Answer(ctx context.Context, question string, contextText string, queryType string) (string, error) {
	if s.baseURL == "" {
		return "", errors.New("OLLAMA_BASE_URL no configurada")
	}
	if s.model == "" {
		return "", errors.New("OLLAMA_CHAT_MODEL no configurado")
	}
	if question == "" {
		return "", errors.New("pregunta vacía")
	}

	promptConfig := promptConfigForQuestion(question, queryType)
	return s.answerWithPrompt(ctx, question, contextText, promptConfig.SystemPrompt, promptConfig.UserRules, promptConfig.NumPredict)
}

func (s *OllamaChatService) SummarizeSection(ctx context.Context, sectionTitle string, contextText string) (string, error) {
	config := summarySectionPromptConfig()
	question := strings.TrimSpace(sectionTitle)
	if question == "" {
		question = "Seccion sin titulo explicito"
	}
	return s.answerWithPrompt(ctx, question, contextText, config.SystemPrompt, config.UserRules, config.NumPredict)
}

func (s *OllamaChatService) AnswerExpandedSummary(ctx context.Context, question string, contextText string) (string, error) {
	config := promptConfigForQueryType("summary")
	return s.answerWithPrompt(ctx, question, contextText, config.SystemPrompt, config.UserRules, config.NumPredict)
}

func (s *OllamaChatService) answerWithPrompt(ctx context.Context, question string, contextText string, systemPrompt string, userRules string, numPredict int) (string, error) {
	if s.baseURL == "" {
		return "", errors.New("OLLAMA_BASE_URL no configurada")
	}
	if s.model == "" {
		return "", errors.New("OLLAMA_CHAT_MODEL no configurado")
	}
	if question == "" {
		return "", errors.New("pregunta vacía")
	}

	contextText = trimContextForQuery(question, contextText)

	body, err := json.Marshal(ollamaChatRequest{
		Model: s.model,
		Messages: []ollamaChatMessage{
			{
				Role:    "system",
				Content: systemPrompt,
			},
			{
				Role: "user",
				Content: fmt.Sprintf(
					"Contexto recuperado:\n%s\n\n%s\n\nPregunta:\n%s",
					contextText,
					userRules,
					question,
				),
			},
		},
		Stream:  false,
		Options: ollamaChatOptions{NumPredict: numPredict},
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/api/chat", bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		if QueryTypeForQuery(question) == "summary" {
			log.Printf("ollama_summary_error=%q", err.Error())
		}
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		err := fmt.Errorf("ollama chat error: status=%d body=%s", resp.StatusCode, raw.String())
		if QueryTypeForQuery(question) == "summary" {
			log.Printf("ollama_summary_error=%q", err.Error())
		}
		return "", err
	}

	var parsed ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		if QueryTypeForQuery(question) == "summary" {
			log.Printf("ollama_summary_error=%q", err.Error())
		}
		return "", err
	}
	if parsed.Error != "" {
		if QueryTypeForQuery(question) == "summary" {
			log.Printf("ollama_summary_error=%q", parsed.Error)
		}
		return "", errors.New(parsed.Error)
	}
	if parsed.Message.Content == "" {
		err := errors.New("respuesta vacía")
		if QueryTypeForQuery(question) == "summary" {
			log.Printf("ollama_summary_error=%q", err.Error())
		}
		return "", err
	}

	return parsed.Message.Content, nil
}

func trimContextForQuery(question string, contextText string) string {
	maxContextRunes := 18000
	if QueryTypeForQuery(question) == "summary" {
		maxContextRunes = 32000
	}

	runes := []rune(strings.TrimSpace(contextText))
	if len(runes) <= maxContextRunes {
		return string(runes)
	}
	return strings.TrimSpace(string(runes[:maxContextRunes]))
}

func promptConfigForQueryType(queryType string) answerPromptConfig {
	switch queryType {
	case "summary":
		return answerPromptConfig{
			SystemPrompt: "Actúa como analista documental experto. Elabora un resumen amplio, profesional y estructurado usando únicamente el contexto recuperado. No produzcas una lista mínima: desarrolla ideas, relaciones y conclusiones con profundidad.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Estructura la respuesta con: Título, Resumen ejecutivo, Desarrollo por secciones, Ideas principales explicadas y Conclusión.\n- El resumen ejecutivo debe tener varios párrafos sustantivos.\n- En el desarrollo por secciones, explica qué aborda cada parte y por qué es relevante.\n- Las ideas principales deben estar explicadas, no solo enumeradas.\n- No uses referencias internas, identificadores ni etiquetas técnicas.\n- No inventes información ni agregues conocimiento externo.\n- Si el contexto es limitado, desarrolla lo disponible sin mencionar limitaciones técnicas.",
			NumPredict:   1800,
		}
	case "section":
		return answerPromptConfig{
			SystemPrompt: "Responde como analista documental sobre la sección solicitada usando únicamente el contexto. Desarrolla una explicación profesional, con contexto, alcance e implicaciones dentro del documento.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No mezcles contenido ajeno a la sección si no está respaldado por el contexto.\n- Responde en párrafos completos o en apartados breves con explicación.\n- Evita listas de frases sueltas.\n- No incluyas referencias internas ni etiquetas técnicas.\n- No inventes información ni agregues conocimiento externo.",
			NumPredict:   900,
		}
	case "structure":
		return answerPromptConfig{
			SystemPrompt: "Construye un índice comentado del documento usando solo el contexto. Identifica secciones, temas, hojas, columnas o bloques y explica brevemente qué contiene cada uno.",
			UserRules:    "Reglas obligatorias:\n- Extrae estructura o temas visibles en el contexto.\n- Presenta un índice comentado, no una lista mínima.\n- Cada elemento debe incluir una explicación de su contenido o función.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.",
			NumPredict:   850,
		}
	default:
		return answerPromptConfig{
			SystemPrompt: "Responde como analista documental experto usando solo el contexto recuperado. Da una respuesta argumentada, clara y profesional, con párrafos completos y explicación suficiente.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Responde en párrafos completos, no en frases sueltas.\n- Explica el contexto, la respuesta y sus matices dentro del documento.\n- No inventes información ni agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Si la información es parcial, responde con lo que el documento permite afirmar.",
			NumPredict:   900,
		}
	}
}

func promptConfigForQuestion(question string, queryType string) answerPromptConfig {
	if queryType != "question" {
		if queryType == "summary" && IsKeyPointsQuery(question) {
			return keyPointsPromptConfig()
		}
		return promptConfigForQueryType(queryType)
	}

	task := DocumentTaskForQuery(question)
	switch task {
	case DocumentTaskDates:
		return answerPromptConfig{
			SystemPrompt: "Extrae fechas del documento usando solo el contexto. Incluye fecha, evento asociado y parte/seccion si aparece. No inventes fechas.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Responde en lista breve.\n- Incluye la fecha y qué representa.\n- Si no hay fechas claras, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   260,
		}
	case DocumentTaskParts:
		return answerPromptConfig{
			SystemPrompt: "Identifica partes, firmantes, actores o entidades mencionadas usando solo el contexto. No inventes nombres ni roles.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Responde en lista breve.\n- Incluye nombre y rol si aparece.\n- Si no hay partes claras, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   260,
		}
	case DocumentTaskEconomicValues:
		return answerPromptConfig{
			SystemPrompt: "Extrae valores económicos del documento usando solo el contexto. Incluye monto, moneda, concepto y condiciones si aparecen.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No calcules ni infieras montos no escritos.\n- Responde en lista breve.\n- Si no hay valores claros, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   280,
		}
	case DocumentTaskObligations:
		return answerPromptConfig{
			SystemPrompt: "Identifica obligaciones, deberes, responsabilidades o condiciones vinculantes usando solo el contexto. No inventes obligaciones.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Indica obligado, obligación y plazo/condición si aparecen.\n- Responde en lista breve.\n- Si no hay obligaciones claras, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   360,
		}
	case DocumentTaskContradictions:
		return answerPromptConfig{
			SystemPrompt: "Detecta posibles contradicciones o inconsistencias internas usando solo el contexto. Señala únicamente conflictos respaldados por fragmentos recuperados.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No inventes contradicciones.\n- Explica brevemente los puntos en tensión.\n- Si no hay contradicciones claras, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   380,
		}
	case DocumentTaskLegalAnalysis:
		return answerPromptConfig{
			SystemPrompt: "Genera un análisis jurídico preliminar usando solo el contexto. Identifica partes, obligaciones, riesgos, plazos y puntos que requieren revisión. No des asesoría definitiva.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No agregues normas externas si no están en el contexto.\n- Presenta hallazgos y riesgos en viñetas.\n- Si el contexto no alcanza, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   520,
		}
	case DocumentTaskCompareDocuments:
		return answerPromptConfig{
			SystemPrompt: "Compara documentos usando solo el contexto recuperado. Identifica coincidencias, diferencias, cambios de obligaciones, fechas, partes y valores.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No inventes documentos ni diferencias.\n- Responde en secciones breves: Coincidencias, Diferencias, Riesgos.\n- Si el contexto no permite comparar, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   520,
		}
	default:
		return promptConfigForQueryType(queryType)
	}
}

func IsKeyPointsQuery(query string) bool {
	normalized := NormalizeSearchText(query)
	return strings.Contains(normalized, "puntos clave") ||
		strings.Contains(normalized, "ideas clave") ||
		strings.Contains(normalized, "aspectos clave") ||
		strings.Contains(normalized, "lo mas importante") ||
		strings.Contains(normalized, "principales puntos")
}

func keyPointsPromptConfig() answerPromptConfig {
	return answerPromptConfig{
		SystemPrompt: "Extrae y desarrolla los puntos clave del documento como analista documental experto. Cada punto debe estar explicado con suficiente contexto, no como frase aislada.",
		UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Presenta el título: Puntos clave del documento.\n- Incluye entre 5 y 8 puntos clave.\n- Cada punto debe tener una explicación de 3 a 5 líneas, con contexto y relevancia.\n- Evita puntos de una sola frase.\n- No incluyas referencias internas ni etiquetas técnicas.\n- No inventes información ni agregues conocimiento externo.",
		NumPredict:   1300,
	}
}

func summarySectionPromptConfig() sectionSummaryPromptConfig {
	return sectionSummaryPromptConfig{
		SystemPrompt: "Analiza la seccion usando solo el contexto recuperado. Identifica su idea principal, puntos importantes y aporte dentro del documento con una explicación desarrollada.",
		UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Escribe 2 o 3 párrafos explicativos.\n- Identifica el aporte principal de la sección dentro del documento.\n- No inventes información.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.",
		NumPredict:   550,
	}
}

func ModelTokensLimitForQueryType(queryType string) int {
	return promptConfigForQueryType(queryType).NumPredict
}
