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

	contextText = sanitizeSensitiveText(trimContextForQuery(question, contextText))
	userRules = ensureInterpretationRule(userRules)

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

	return sanitizeSensitiveText(parsed.Message.Content), nil
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
	interpretationRule := "\n- No copies el texto del documento.\n- Interpreta y explica el contenido con tus propias palabras."
	switch queryType {
	case "summary":
		return answerPromptConfig{
			SystemPrompt: "Resume el documento usando únicamente el contexto recuperado. Reescribe y sintetiza información visible del documento sin agregar comentarios genéricos.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- La respuesta solo puede contener texto reescrito del documento o síntesis directa de ese texto.\n- No agregues explicaciones de relevancia, función, importancia, aporte o interpretación si no están explícitas en el contexto.\n- No repitas estructuras ni concatenes frases fijas.\n- No uses referencias internas, identificadores ni etiquetas técnicas.\n- No inventes información ni agregues conocimiento externo." + interpretationRule,
			NumPredict:   1800,
		}
	case "section":
		return answerPromptConfig{
			SystemPrompt: "Responde sobre la sección solicitada usando únicamente el contexto. Sintetiza de forma directa el texto recuperado.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No mezcles contenido ajeno a la sección si no está respaldado por el contexto.\n- La respuesta solo puede contener texto reescrito del documento o síntesis directa de ese texto.\n- No agregues explicaciones genéricas ni interpretación documental.\n- No incluyas referencias internas ni etiquetas técnicas.\n- No inventes información ni agregues conocimiento externo." + interpretationRule,
			NumPredict:   900,
		}
	case "structure":
		return answerPromptConfig{
			SystemPrompt: "Extrae la estructura visible del documento usando solo el contexto. Enumera secciones, temas, hojas, columnas o bloques cuando aparezcan.",
			UserRules:    "Reglas obligatorias:\n- Extrae estructura o temas visibles en el contexto.\n- Describe cada elemento solo con información presente en el documento.\n- No agregues comentarios sobre función, importancia o utilidad si no están escritos en el contexto.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas." + interpretationRule,
			NumPredict:   850,
		}
	default:
		return answerPromptConfig{
			SystemPrompt: "Responde usando solo el contexto recuperado. La respuesta debe ser una extracción reescrita o una síntesis directa del documento.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Responde con información directa del documento.\n- No agregues explicación genérica, interpretación documental ni frases fijas.\n- No inventes información ni agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Si la información es parcial, responde solo con lo que el documento afirma." + interpretationRule,
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
		SystemPrompt: "Extrae puntos clave reales del documento usando solo el contexto recuperado. Analiza los chunks, agrupa ideas y redacta una sintesis propia sin copiar frases completas.",
		UserRules:    "Reglas obligatorias:\n- Usa exactamente el titulo: Puntos clave del documento.\n- Incluye maximo 5 puntos.\n- Cada punto debe tener un titulo corto de 2 a 5 palabras y una explicacion clara de 2 a 3 lineas.\n- Agrupa ideas repetidas; no encadenes chunks ni repitas la misma informacion.\n- No uses titulos del documento como puntos.\n- No copies frases completas ni listas del documento.\n- No uses frases de enlace genericas entre puntos.\n- No incluyas referencias internas, identificadores ni etiquetas tecnicas.\n- No inventes informacion ni agregues conocimiento externo.\n\nFormato obligatorio:\nPuntos clave del documento\n\n1. Titulo\nExplicacion en parrafo\n\n2. Titulo\nExplicacion en parrafo",
		NumPredict:   1300,
	}
}

func summarySectionPromptConfig() sectionSummaryPromptConfig {
	return sectionSummaryPromptConfig{
		SystemPrompt: "Sintetiza la seccion usando solo el contexto recuperado.",
		UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Escribe solo información reescrita o sintetizada directamente del texto.\n- No agregues comentarios de aporte, relevancia, función o interpretación si no están en el contexto.\n- No inventes información.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.",
		NumPredict:   550,
	}
}

func ModelTokensLimitForQueryType(queryType string) int {
	return promptConfigForQueryType(queryType).NumPredict
}
