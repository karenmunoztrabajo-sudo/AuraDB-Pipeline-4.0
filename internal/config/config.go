package config

import "os"

type Config struct {
	AppPort          string
	PostgresHost     string
	PostgresPort     string
	PostgresDB       string
	PostgresUser     string
	PostgresPass     string
	NatsURL          string
	MinioEndpoint    string
	MinioAccessKey   string
	MinioSecretKey   string
	MinioBucket      string
	JWTSecret        string
	OpenAIAPIKey     string
	OpenAIModel      string
	OllamaBaseURL    string
	OllamaChatModel  string
	OllamaEmbedModel string
}

func Load() Config {
	return Config{
		AppPort:          getEnv("APP_PORT", "8080"),
		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnv("POSTGRES_PORT", "5432"),
		PostgresDB:       getEnv("POSTGRES_DB", "auradb"),
		PostgresUser:     getEnv("POSTGRES_USER", "auradb"),
		PostgresPass:     getEnv("POSTGRES_PASSWORD", "auradb123"),
		NatsURL:          getEnv("NATS_URL", "nats://localhost:4222"),
		MinioEndpoint:    getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey:   getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey:   getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioBucket:      getEnv("MINIO_BUCKET", "auradb-raw"),
		JWTSecret:        getEnv("JWT_SECRET", "supersecreto"),
		OpenAIAPIKey:     getEnv("OPENAI_API_KEY", ""),
		OpenAIModel:      getEnv("OPENAI_MODEL", "gpt-4o-mini"),
		OllamaBaseURL:    getEnv("OLLAMA_BASE_URL", "http://localhost:11434"),
		OllamaChatModel:  getEnv("OLLAMA_CHAT_MODEL", "llama3.2:3b"),
		OllamaEmbedModel: getEnv("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
	}
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
