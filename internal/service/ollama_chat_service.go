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

	promptConfig := promptConfigForQueryType(queryType)
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
	if QueryTypeForQuery(question) != "summary" {
		return contextText
	}

	const maxSummaryContextRunes = 12000
	runes := []rune(strings.TrimSpace(contextText))
	if len(runes) <= maxSummaryContextRunes {
		return string(runes)
	}
	return strings.TrimSpace(string(runes[:maxSummaryContextRunes]))
}

func promptConfigForQueryType(queryType string) answerPromptConfig {
	switch queryType {
	case "summary":
		return answerPromptConfig{
			SystemPrompt: "Explica el documento completo utilizando el contexto. Recorre sus partes principales y desarrolla cada una. No hagas un resumen corto. Genera una explicación amplia, clara y estructurada. Usa varios párrafos si es necesario. No inventes información.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Recorre las partes principales del documento y desarrolla cada una.\n- No hagas un resumen corto.\n- Genera una explicación amplia, clara y estructurada.\n- Usa varios párrafos si es necesario.\n- No inventes información.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Si la información no está clara en el contexto, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   900,
		}
	case "section":
		return answerPromptConfig{
			SystemPrompt: "Responde solo sobre la sección o parte pedida usando únicamente el contexto recuperado. Mantén modo grounded: no inventes, no agregues conocimiento externo y no mezcles contenido ajeno a esa parte. La respuesta debe tener una extensión intermedia y desarrollar más de una idea cuando el contexto lo permita. Si la información no está clara en el contexto, responde exactamente: No encontré esa información en el documento.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No mezcles otras partes del documento si no están en el contexto.\n- No inventes.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Responde con 1 o 2 párrafos breves o con 3 a 6 viñetas útiles si conviene.\n- Si la información no está clara en el contexto, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   420,
		}
	case "structure":
		return answerPromptConfig{
			SystemPrompt: "Extrae títulos, secciones, hojas, columnas o temas del documento usando solo el contexto. Mantén modo grounded: no inventes y no agregues conocimiento externo. Responde en formato lista breve. Si no hay títulos literales, extrae los temas más claros y fieles al contenido. No incluyas referencias internas, identificadores, ni formatos como [chunk_id: ...] o [document_id: ...] en la respuesta final.",
			UserRules:    "Reglas obligatorias:\n- Extrae estructura o temas visibles en el contexto.\n- Responde solo en formato lista breve.\n- No expliques cada punto.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Devuelve hasta 8 elementos si el contexto lo permite.",
			NumPredict:   220,
		}
	default:
		return answerPromptConfig{
			SystemPrompt: "Responde solo con información del contexto recuperado. Mantén modo grounded total: no inventes, no uses conocimiento externo y no expliques de más. Si la respuesta no está clara en el contexto, responde exactamente: No encontré esa información en el documento. La respuesta debe ser breve y directa.",
			UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- No inventes.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas técnicas.\n- Responde breve, clara y directa.\n- Máximo 3 o 4 líneas.\n- Si la respuesta no está clara en el contexto, responde exactamente: No encontré esa información en el documento.",
			NumPredict:   180,
		}
	}
}

func summarySectionPromptConfig() sectionSummaryPromptConfig {
	return sectionSummaryPromptConfig{
		SystemPrompt: "Resume brevemente la seccion usando solo el contexto recuperado. Identifica la idea principal, los puntos importantes y el rol de esta seccion dentro del documento. No inventes informacion.",
		UserRules:    "Reglas obligatorias:\n- Usa solo el contexto recuperado.\n- Resume esta seccion en un parrafo breve.\n- Identifica su aporte principal dentro del documento.\n- No inventes informacion.\n- No agregues conocimiento externo.\n- No incluyas referencias internas ni etiquetas tecnicas.\n- Si la informacion no esta clara en el contexto, responde exactamente: No encontré esa información en el documento.",
		NumPredict:   220,
	}
}

func ModelTokensLimitForQueryType(queryType string) int {
	return promptConfigForQueryType(queryType).NumPredict
}
