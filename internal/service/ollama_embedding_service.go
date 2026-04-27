package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"auradb-pipeline/internal/config"
)

type OllamaEmbeddingService struct {
	baseURL string
	model   string
	client  *http.Client
}

func NewOllamaEmbeddingService(cfg config.Config) *OllamaEmbeddingService {
	return &OllamaEmbeddingService{
		baseURL: strings.TrimRight(cfg.OllamaBaseURL, "/"),
		model:   cfg.OllamaEmbedModel,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
}

type ollamaEmbedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type ollamaEmbedResponse struct {
	Embeddings [][]float64 `json:"embeddings"`
	Error      string      `json:"error,omitempty"`
}

type ollamaEmbeddingsRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbeddingsResponse struct {
	Embedding []float64 `json:"embedding"`
	Error     string    `json:"error,omitempty"`
}

func (s *OllamaEmbeddingService) GenerateEmbedding(ctx context.Context, input string) ([]float64, error) {
	if s.baseURL == "" {
		return nil, errors.New("OLLAMA_BASE_URL no configurada")
	}
	if s.model == "" {
		return nil, errors.New("OLLAMA_EMBED_MODEL no configurado")
	}
	if input == "" {
		return nil, errors.New("input vacío")
	}

	vector, err := s.generateWithEmbedEndpoint(ctx, input)
	if err == nil {
		return vector, nil
	}

	fallbackVector, fallbackErr := s.generateWithEmbeddingsEndpoint(ctx, input)
	if fallbackErr == nil {
		return fallbackVector, nil
	}

	return nil, fmt.Errorf("ollama embeddings error: /api/embed=%v; /api/embeddings=%v", err, fallbackErr)
}

func (s *OllamaEmbeddingService) generateWithEmbedEndpoint(ctx context.Context, input string) ([]float64, error) {
	body, err := json.Marshal(ollamaEmbedRequest{
		Model: s.model,
		Input: input,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/api/embed", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, raw.String())
	}

	var parsed ollamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if parsed.Error != "" {
		return nil, errors.New(parsed.Error)
	}
	if len(parsed.Embeddings) == 0 || len(parsed.Embeddings[0]) == 0 {
		return nil, errors.New("respuesta sin embeddings")
	}

	return parsed.Embeddings[0], nil
}

func (s *OllamaEmbeddingService) generateWithEmbeddingsEndpoint(ctx context.Context, input string) ([]float64, error) {
	body, err := json.Marshal(ollamaEmbeddingsRequest{
		Model:  s.model,
		Prompt: input,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/api/embeddings", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, raw.String())
	}

	var parsed ollamaEmbeddingsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if parsed.Error != "" {
		return nil, errors.New(parsed.Error)
	}
	if len(parsed.Embedding) == 0 {
		return nil, errors.New("respuesta sin embeddings")
	}

	return parsed.Embedding, nil
}
