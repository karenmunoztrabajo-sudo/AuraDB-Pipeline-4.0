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

type OpenAIChatService struct {
	apiKey string
	model  string
	client *http.Client
}

func NewOpenAIChatService(cfg config.Config) (*OpenAIChatService, error) {
	if strings.TrimSpace(cfg.OpenAIModel) == "" {
		return nil, errors.New("OPENAI_MODEL no configurado")
	}

	return &OpenAIChatService{
		apiKey: strings.TrimSpace(cfg.OpenAIAPIKey),
		model:  strings.TrimSpace(cfg.OpenAIModel),
		client: &http.Client{Timeout: 90 * time.Second},
	}, nil
}

type openAIResponseRequest struct {
	Model           string `json:"model"`
	Instructions    string `json:"instructions"`
	Input           string `json:"input"`
	MaxOutputTokens int    `json:"max_output_tokens,omitempty"`
	Store           bool   `json:"store"`
}

type openAIResponse struct {
	OutputText string             `json:"output_text"`
	Output     []openAIOutputItem `json:"output"`
	Error      *openAIError       `json:"error"`
}

type openAIOutputItem struct {
	Type    string                `json:"type"`
	Role    string                `json:"role"`
	Content []openAIOutputContent `json:"content"`
}

type openAIOutputContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type openAIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (s *OpenAIChatService) Answer(ctx context.Context, question string, contextText string, queryType string) (string, error) {
	if strings.TrimSpace(question) == "" {
		return "", errors.New("pregunta vacía")
	}

	promptConfig := promptConfigForQuestion(question, queryType)
	return s.answerWithPrompt(ctx, question, contextText, promptConfig.SystemPrompt, promptConfig.UserRules, promptConfig.NumPredict)
}

func (s *OpenAIChatService) SummarizeSection(ctx context.Context, sectionTitle string, contextText string) (string, error) {
	config := summarySectionPromptConfig()
	question := strings.TrimSpace(sectionTitle)
	if question == "" {
		question = "Seccion sin titulo explicito"
	}
	return s.answerWithPrompt(ctx, question, contextText, config.SystemPrompt, config.UserRules, config.NumPredict)
}

func (s *OpenAIChatService) AnswerExpandedSummary(ctx context.Context, question string, contextText string) (string, error) {
	config := promptConfigForQueryType("summary")
	return s.answerWithPrompt(ctx, question, contextText, config.SystemPrompt, config.UserRules, config.NumPredict)
}

func (s *OpenAIChatService) answerWithPrompt(ctx context.Context, question string, contextText string, systemPrompt string, userRules string, maxOutputTokens int) (string, error) {
	if strings.TrimSpace(s.apiKey) == "" {
		return "", errors.New("OPENAI_API_KEY no configurada")
	}
	if strings.TrimSpace(s.model) == "" {
		return "", errors.New("OPENAI_MODEL no configurado")
	}

	contextText = trimContextForQuery(question, contextText)
	if strings.TrimSpace(contextText) == "" {
		return "", errors.New("contexto vacío: no hay chunks con texto para enviar a OpenAI")
	}

	body, err := json.Marshal(openAIResponseRequest{
		Model:        s.model,
		Instructions: systemPrompt,
		Input: fmt.Sprintf(
			"Contexto recuperado de chunks:\n%s\n\n%s\n\nSolicitud:\n%s",
			contextText,
			userRules,
			question,
		),
		MaxOutputTokens: maxOutputTokens,
		Store:           false,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("openai_request_error=%q", err.Error())
		return "", err
	}
	defer resp.Body.Close()

	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)

	if resp.StatusCode >= 300 {
		err := fmt.Errorf("openai responses error: status=%d body=%s", resp.StatusCode, raw.String())
		log.Printf("openai_response_error=%q", err.Error())
		return "", err
	}

	var parsed openAIResponse
	if err := json.Unmarshal(raw.Bytes(), &parsed); err != nil {
		log.Printf("openai_decode_error=%q raw=%s", err.Error(), raw.String())
		return "", err
	}
	if parsed.Error != nil {
		err := fmt.Errorf("openai responses error: type=%s code=%s message=%s", parsed.Error.Type, parsed.Error.Code, parsed.Error.Message)
		log.Printf("openai_response_error=%q", err.Error())
		return "", err
	}

	answer := strings.TrimSpace(extractOpenAIText(parsed))
	if answer == "" {
		err := errors.New("respuesta vacía del proveedor")
		log.Printf("openai_empty_response=true")
		return "", err
	}

	return answer, nil
}

func extractOpenAIText(resp openAIResponse) string {
	if strings.TrimSpace(resp.OutputText) != "" {
		return resp.OutputText
	}

	var builder strings.Builder
	for _, item := range resp.Output {
		for _, content := range item.Content {
			if content.Type != "output_text" && content.Type != "text" {
				continue
			}
			text := strings.TrimSpace(content.Text)
			if text == "" {
				continue
			}
			if builder.Len() > 0 {
				builder.WriteString("\n")
			}
			builder.WriteString(text)
		}
	}
	return builder.String()
}
