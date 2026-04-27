package main

import (
	"context"
	"fmt"
	"log"
	nethttp "net/http"
	"time"

	"auradb-pipeline/internal/config"
	apphttp "auradb-pipeline/internal/http"
	"auradb-pipeline/internal/repository/messaging"
	"auradb-pipeline/internal/repository/postgres"
	"auradb-pipeline/internal/repository/storage"
	"auradb-pipeline/internal/service"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	cfg := config.Load()

	dbPool, err := postgres.NewPool(cfg)
	if err != nil {
		log.Fatal("error conectando a postgres:", err)
	}
	defer dbPool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatal("postgres no responde:", err)
	}

	fmt.Println("DB: auradb USER: auradb")

	tenantRepo := postgres.NewTenantRepository(dbPool)
	tenantService := service.NewTenantService(tenantRepo)
	tenantHandler := apphttp.NewTenantHandler(tenantService)

	userRepo := postgres.NewUserRepository(dbPool)
	authService := service.NewAuthService(userRepo, cfg.JWTSecret)
	authHandler := apphttp.NewAuthHandler(authService)

	minioRepo, err := storage.NewMinioRepository(cfg)
	if err != nil {
		log.Fatal("error conectando a minio:", err)
	}

	natsRepo, err := messaging.NewNatsRepository(cfg)
	if err != nil {
		log.Fatal("error conectando a nats:", err)
	}

	docRepo := postgres.NewDocumentRepository(dbPool)
	jobRepo := postgres.NewJobRepository(dbPool)
	auditRepo := postgres.NewAuditRepository(dbPool)

	documentHandler := apphttp.NewDocumentHandler(docRepo, jobRepo, auditRepo, minioRepo, natsRepo)
	simpleDocumentUploadHandler := apphttp.NewSimpleDocumentUploadHandler(docRepo, "uploads/documents")

	searchRepo := postgres.NewSearchRepository(dbPool)
	embeddingService := service.NewOllamaEmbeddingService(cfg)
	searchService := service.NewSearchServiceWithEmbedding(searchRepo, embeddingService)
	searchHandler := apphttp.NewSearchHandler(searchService)

	chatService, err := service.NewOpenAIChatService(cfg)
	if err != nil {
		log.Fatal("error configurando OpenAI:", err)
	}
	if cfg.OpenAIAPIKey == "" {
		log.Println("OPENAI_API_KEY no configurada; las consultas usarán respuesta basada en chunks.")
	}
	askLogRepo := postgres.NewAskLogRepository(dbPool)
	askHandler := apphttp.NewAskHandler(searchService, searchRepo, chatService, askLogRepo)

	mux := nethttp.NewServeMux()

	mux.HandleFunc("/health", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(nethttp.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/tenants", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.Method == nethttp.MethodPost {
			tenantHandler.Create(w, r)
			return
		}
		nethttp.Error(w, "metodo no permitido", nethttp.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/auth/register-admin", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.Method == nethttp.MethodPost {
			authHandler.RegisterAdmin(w, r)
			return
		}
		nethttp.Error(w, "metodo no permitido", nethttp.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/auth/login", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.Method == nethttp.MethodPost {
			authHandler.Login(w, r)
			return
		}
		nethttp.Error(w, "metodo no permitido", nethttp.StatusMethodNotAllowed)
	})

	protected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(apphttp.ProtectedWhoAmI))
	mux.Handle("/me", protected)

	uploadProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(documentHandler.Upload))
	mux.Handle("/documents/upload", uploadProtected)

	simpleUploadProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(simpleDocumentUploadHandler.Upload))
	mux.Handle("/api/documents/upload", simpleUploadProtected)

	searchProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(searchHandler.Search))
	mux.Handle("/search", searchProtected)

	askProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(askHandler.Ask))
	mux.Handle("/ask", askProtected)

	askHistoryProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(askHandler.History))
	mux.Handle("/ask/history", askHistoryProtected)

	deleteAskHistoryProtected := apphttp.AuthMiddleware(cfg.JWTSecret)(nethttp.HandlerFunc(askHandler.DeleteHistoryItem))
	mux.Handle("/ask/history/", deleteAskHistoryProtected)

	handlerWithCORS := nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == nethttp.MethodOptions {
			w.WriteHeader(nethttp.StatusOK)
			return
		}

		mux.ServeHTTP(w, r)
	})

	fmt.Println("API corriendo en el puerto:", cfg.AppPort)
	if err := nethttp.ListenAndServe(":"+cfg.AppPort, handlerWithCORS); err != nil {
		log.Fatal(err)
	}
}
