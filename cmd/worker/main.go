package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"path/filepath"
	"strings"
	"time"

	"auradb-pipeline/internal/config"
	"auradb-pipeline/internal/repository/postgres"
	"auradb-pipeline/internal/repository/storage"
	"auradb-pipeline/internal/service"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
)

type UploadEvent struct {
	JobID             string `json:"job_id"`
	DocumentID        string `json:"document_id"`
	DocumentVersionID string `json:"document_version_id"`
	TenantID          string `json:"tenant_id"`
	ObjectKey         string `json:"object_key"`
}

type savedChunk struct {
	ID      string
	Index   int
	Content string
}

type chunkToSave struct {
	Index      int
	PageNumber *int
	Content    string
}

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	dbPool, err := postgres.NewPool(cfg)
	if err != nil {
		log.Fatal("error conectando a postgres:", err)
	}
	defer dbPool.Close()

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatal("error conectando a nats:", err)
	}
	defer nc.Close()
	log.Printf("worker_nats_url=%s", cfg.NatsURL)

	jobRepo := postgres.NewJobRepository(dbPool)
	parserRepo := postgres.NewParserRepository(dbPool)
	chunkRepo := postgres.NewChunkRepository(dbPool)
	embeddingRepo := postgres.NewEmbeddingRepository(dbPool)

	embeddingService := service.NewOllamaEmbeddingService(cfg)

	fmt.Println("Worker Auradb iniciado y escuchando eventos...")

	const uploadSubject = "documents.uploaded"
	_, err = nc.Subscribe(uploadSubject, func(msg *nats.Msg) {
		fmt.Println("\n--- Nuevo evento recibido ---")

		var event UploadEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			fmt.Println("error parseando evento:", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		minioRepo, err := storage.NewMinioRepository(cfg)
		if err != nil {
			fmt.Println("error inicializando minio:", err)
			return
		}

		fileName := filenameFromObjectKey(event.ObjectKey)
		objectInfo, err := minioRepo.Client().StatObject(
			ctx,
			minioRepo.Bucket(),
			event.ObjectKey,
			minio.StatObjectOptions{},
		)
		if err != nil {
			log.Printf(
				"warning obteniendo metadata de objeto | object_key=%s error=%v",
				event.ObjectKey,
				err,
			)
		}
		contentType := objectInfo.ContentType

		obj, err := minioRepo.Client().GetObject(
			ctx,
			minioRepo.Bucket(),
			event.ObjectKey,
			minio.GetObjectOptions{},
		)
		if err != nil {
			fmt.Println("error obteniendo objeto:", err)
			return
		}
		defer obj.Close()

		fileBytes, err := io.ReadAll(obj)
		if err != nil {
			fmt.Println("error leyendo contenido:", err)
			return
		}

		parseResult, err := service.ParseDocument(fileName, contentType, fileBytes)
		detectedExtension := parseResult.Extension
		if detectedExtension == "" {
			detectedExtension = strings.ToLower(filepath.Ext(fileName))
		}
		log.Printf(
			"parser_result | filename=%s detected_extension=%s detected_type=%s parser=%s mime_type=%s text_length=%d",
			fileName,
			detectedExtension,
			parseResult.DetectedType,
			parseResult.ParserName,
			contentType,
			len(parseResult.Content),
		)
		if errors.Is(err, service.ErrUnsupportedFormat) {
			log.Printf(
				"formato no soportado | filename=%s detected_extension=%s detected_type=%s mime_type=%s",
				fileName,
				detectedExtension,
				parseResult.DetectedType,
				contentType,
			)

			if stepErr := jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "parsed_document", 2, "skipped"); stepErr != nil {
				fmt.Println("error creando job_step parsed_document:", stepErr)
				return
			}

			if statusErr := jobRepo.UpdateJobStatus(ctx, event.JobID, "unsupported_format", "unsupported_format"); statusErr != nil {
				fmt.Println("error actualizando job unsupported_format:", statusErr)
				return
			}

			return
		}
		if isParseFailure(parseResult.DetectedType, err) {
			logParseFailure(fileName, detectedExtension, parseResult, err)
			if stepErr := jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "parsed_document", 2, "failed"); stepErr != nil {
				fmt.Println("error creando job_step parsed_document:", stepErr)
				return
			}
			if statusErr := jobRepo.UpdateJobStatus(ctx, event.JobID, "failed_parse", "failed_parse"); statusErr != nil {
				fmt.Println("error actualizando job failed_parse:", statusErr)
				return
			}
			return
		}
		if err != nil {
			log.Printf(
				"error parseando documento | filename=%s detected_extension=%s detected_type=%s parser=%s error=%v",
				fileName,
				detectedExtension,
				parseResult.DetectedType,
				parseResult.ParserName,
				err,
			)
			if stepErr := jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "parsed_document", 2, "failed"); stepErr != nil {
				fmt.Println("error creando job_step parsed_document:", stepErr)
				return
			}
			if statusErr := jobRepo.UpdateJobStatus(ctx, event.JobID, "failed_partial", "parsed_document"); statusErr != nil {
				fmt.Println("error actualizando job:", statusErr)
				return
			}
			return
		}

		content := parseResult.Content

		err = parserRepo.SaveParsedDocumentWithMetadata(
			ctx,
			event.TenantID,
			event.DocumentID,
			event.DocumentVersionID,
			content,
			parseResult.ParserName,
			parseResult.DetectedType,
			"completed",
		)
		if err != nil {
			fmt.Println("error guardando en parser_repo:", err)
			return
		}

		err = jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "parsed_document", 2, "completed")
		if err != nil {
			fmt.Println("error creando job_step parsed_document:", err)
			return
		}

		chunks := service.SplitIntoChunks(content, 500)
		if parseResult.DetectedType == "xlsx" && parseResult.ExcelData != nil {
			excelChunks, excelStats := service.SplitExcelIntoChunks(parseResult.ExcelData, 500)
			if len(excelChunks) > 0 {
				chunks = excelChunks
			}
			log.Printf(
				"excel_sheet_count=%d excel_sheet_names=%q excel_rows_processed=%d excel_chunks_generated=%d document_id=%s",
				excelStats.SheetCount,
				strings.Join(excelStats.SheetNames, ","),
				excelStats.RowsProcessed,
				excelStats.ChunksGenerated,
				event.DocumentID,
			)
		}
		totalChunkLen := 0
		for _, chunk := range chunks {
			totalChunkLen += len([]rune(chunk))
		}
		avgChunkLen := 0
		if len(chunks) > 0 {
			avgChunkLen = totalChunkLen / len(chunks)
		}
		log.Printf(
			"document_id=%s document_version_id=%s parser=%s text_length=%d chunks_generados=%d chunk_size_promedio=%d",
			event.DocumentID,
			event.DocumentVersionID,
			parseResult.ParserName,
			len([]rune(content)),
			len(chunks),
			avgChunkLen,
		)
		log.Printf("chunks_found=%d", len(chunks))

		chunksToSave := buildChunksToSave(parseResult, chunks)
		savedChunks := make([]savedChunk, 0, len(chunksToSave))
		for _, chunk := range chunksToSave {
			chunkID, err := chunkRepo.SaveChunkWithPage(
				ctx,
				event.TenantID,
				event.DocumentID,
				event.DocumentVersionID,
				chunk.PageNumber,
				chunk.Index,
				chunk.Content,
			)
			if err != nil {
				fmt.Println("error guardando chunk:", err)
				return
			}

			savedChunks = append(savedChunks, savedChunk{
				ID:      chunkID,
				Index:   chunk.Index,
				Content: chunk.Content,
			})
		}

		err = jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "chunks_created", 3, "completed")
		if err != nil {
			fmt.Println("error creando job_step chunks_created:", err)
			return
		}

		embeddingsOK := 0
		embeddingsFailed := 0

		for _, chunk := range savedChunks {
			vector, err := embeddingService.GenerateEmbedding(ctx, chunk.Content)
			if err != nil {
				embeddingsFailed++
				log.Printf(
					"error generando embedding | document_id=%s chunk_index=%d chunk_id=%s error=%v",
					event.DocumentID,
					chunk.Index,
					chunk.ID,
					err,
				)
				continue
			}

			err = embeddingRepo.SaveEmbedding(ctx, event.TenantID, chunk.ID, vector)
			if err != nil {
				embeddingsFailed++
				log.Printf(
					"error guardando embedding | document_id=%s chunk_index=%d chunk_id=%s error=%v",
					event.DocumentID,
					chunk.Index,
					chunk.ID,
					err,
				)
				continue
			}

			embeddingsOK++
		}

		embeddingStepStatus := "completed"
		jobStatus := "embedded"
		if embeddingsFailed > 0 {
			embeddingStepStatus = "failed"
			jobStatus = "failed_partial"
		}

		log.Printf(
			"document_id=%s chunks_generados=%d embeddings_ok=%d embeddings_failed=%d final_status=%s",
			event.DocumentID,
			len(chunks),
			embeddingsOK,
			embeddingsFailed,
			jobStatus,
		)

		err = jobRepo.UpdateJobStatus(ctx, event.JobID, jobStatus, "embeddings_created")
		if err != nil {
			fmt.Println("error actualizando job:", err)
			return
		}

		err = jobRepo.AddJobStep(ctx, event.TenantID, event.JobID, "embeddings_created", 5, embeddingStepStatus)
		if err != nil {
			fmt.Println("error creando job_step:", err)
			return
		}

		fmt.Println("Documento procesado correctamente ID:", event.DocumentID)
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("subscribed_subject=%s", uploadSubject)

	select {}
}

func buildChunksToSave(parseResult service.ParseResult, chunks []string) []chunkToSave {
	if len(parseResult.PageTexts) == 0 {
		items := make([]chunkToSave, 0, len(chunks))
		for i, chunk := range chunks {
			items = append(items, chunkToSave{Index: i, Content: chunk})
		}
		return items
	}

	items := make([]chunkToSave, 0)
	index := 0
	for _, page := range parseResult.PageTexts {
		pageChunks := service.SplitIntoChunks(page.Content, 500)
		pageNumber := page.PageNumber
		for _, chunk := range pageChunks {
			items = append(items, chunkToSave{
				Index:      index,
				PageNumber: &pageNumber,
				Content:    chunk,
			})
			index++
		}
	}
	if len(items) == 0 {
		for i, chunk := range chunks {
			items = append(items, chunkToSave{Index: i, Content: chunk})
		}
	}
	return items
}

func filenameFromObjectKey(objectKey string) string {
	base := path.Base(objectKey)
	parts := strings.SplitN(base, "_", 2)
	if len(parts) == 2 && parts[1] != "" {
		return parts[1]
	}
	return base
}

func isParseFailure(detectedType string, err error) bool {
	if err == nil {
		return false
	}
	if detectedType != "pdf" && detectedType != "docx" && detectedType != "xlsx" {
		return false
	}
	return true
}

func logParseFailure(fileName string, detectedExtension string, parseResult service.ParseResult, err error) {
	if parseResult.DetectedType == "pdf" && errors.Is(err, service.ErrNoExtractableText) {
		log.Printf(
			"pdf sin texto extraíble | filename=%s detected_extension=%s parser=%s text_length=%d error=%v",
			fileName,
			detectedExtension,
			parseResult.ParserName,
			len(parseResult.Content),
			err,
		)
		return
	}

	if parseResult.DetectedType == "docx" {
		log.Printf(
			"fallo de extracción docx | filename=%s detected_extension=%s parser=%s text_length=%d error=%v",
			fileName,
			detectedExtension,
			parseResult.ParserName,
			len(parseResult.Content),
			err,
		)
		return
	}

	if parseResult.DetectedType == "xlsx" {
		log.Printf(
			"fallo de extracción xlsx | filename=%s detected_extension=%s parser=%s text_length=%d error=%v",
			fileName,
			detectedExtension,
			parseResult.ParserName,
			len(parseResult.Content),
			err,
		)
		return
	}

	log.Printf(
		"fallo de extracción | filename=%s detected_extension=%s detected_type=%s parser=%s text_length=%d error=%v",
		fileName,
		detectedExtension,
		parseResult.DetectedType,
		parseResult.ParserName,
		len(parseResult.Content),
		err,
	)
}
