package http

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"auradb-pipeline/internal/repository/messaging"
	"auradb-pipeline/internal/repository/postgres"
	"auradb-pipeline/internal/repository/storage"

	"github.com/minio/minio-go/v7"
)

const (
	maxDocumentUploadBytes = 25 * 1024 * 1024
	maxImageUploadBytes    = 10 * 1024 * 1024
)

type DocumentHandler struct {
	docRepo   *postgres.DocumentRepository
	jobRepo   *postgres.JobRepository
	auditRepo *postgres.AuditRepository
	minioRepo *storage.MinioRepository
	natsRepo  *messaging.NatsRepository
}

type uploadResponse struct {
	DocumentID        string `json:"document_id"`
	DocumentVersionID string `json:"document_version_id"`
	JobID             string `json:"job_id"`
	ObjectKey         string `json:"object_key"`
	Message           string `json:"message"`
	Legacy            string `json:"legacy"`
}

func NewDocumentHandler(
	docRepo *postgres.DocumentRepository,
	jobRepo *postgres.JobRepository,
	auditRepo *postgres.AuditRepository,
	minioRepo *storage.MinioRepository,
	natsRepo *messaging.NatsRepository,
) *DocumentHandler {
	return &DocumentHandler{
		docRepo:   docRepo,
		jobRepo:   jobRepo,
		auditRepo: auditRepo,
		minioRepo: minioRepo,
		natsRepo:  natsRepo,
	}
}

func (h *DocumentHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("upload_error cause=method_not_allowed method=%s", r.Method)
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}

	ipcCtx, ok := GetIPC(r)
	if !ok {
		log.Printf("upload_error cause=unauthorized")
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxDocumentUploadBytes+1024*1024)

	err := r.ParseMultipartForm(maxDocumentUploadBytes)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			log.Printf("upload_error cause=size_limit filename=%s size=%d type=%s", "", 0, "")
			http.Error(w, "El archivo supera el tamaño máximo permitido (25MB)", http.StatusRequestEntityTooLarge)
			return
		}
		log.Printf("upload_error cause=parse_multipart error=%v", err)
		http.Error(w, "error leyendo multipart", http.StatusBadRequest)
		return
	}

	file, header, err := openUploadFile(r)
	if err != nil {
		log.Printf("upload_error cause=missing_file_field error=%v", err)
		http.Error(w, "archivo requerido en campo file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	log.Printf("upload_received filename=%s size=%d type=%s", header.Filename, header.Size, contentType)

	if !isAllowedDocumentUpload(header.Filename, contentType) {
		log.Printf("upload_error cause=unsupported_format filename=%s size=%d type=%s", header.Filename, header.Size, contentType)
		http.Error(w, "Formato de archivo no soportado", http.StatusBadRequest)
		return
	}

	if err := validateUploadSize(header.Filename, contentType, header.Size); err != nil {
		log.Printf("upload_error cause=size_limit filename=%s size=%d type=%s error=%v", header.Filename, header.Size, contentType, err)
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	log.Printf("upload_validated filename=%s size=%d type=%s", header.Filename, header.Size, contentType)

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Printf("upload_error cause=read_file filename=%s size=%d type=%s error=%v", header.Filename, header.Size, contentType, err)
		http.Error(w, "error leyendo archivo", http.StatusInternalServerError)
		return
	}

	hash := sha256.Sum256(fileBytes)
	contentHash := hex.EncodeToString(hash[:])

	objectKey := fmt.Sprintf("%s/%d_%s", ipcCtx.TenantID, time.Now().UnixNano(), header.Filename)

	if h.minioRepo == nil {
		log.Printf("upload_error cause=minio_repo_nil filename=%s", header.Filename)
		http.Error(w, "error subiendo a MinIO: repositorio no inicializado", http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(fileBytes)
	_, err = h.minioRepo.Client().PutObject(
		r.Context(),
		h.minioRepo.Bucket(),
		objectKey,
		reader,
		int64(len(fileBytes)),
		minio.PutObjectOptions{
			ContentType: contentType,
		},
	)
	if err != nil {
		log.Printf("upload_error cause=minio_put filename=%s size=%d type=%s object_key=%s error=%v", header.Filename, len(fileBytes), contentType, objectKey, err)
		http.Error(w, "error subiendo a MinIO: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("upload_saved filename=%s size=%d type=%s object_key=%s", header.Filename, len(fileBytes), contentType, objectKey)

	if h.docRepo == nil {
		log.Printf("upload_error cause=document_repo_nil filename=%s object_key=%s", header.Filename, objectKey)
		http.Error(w, "error creando document: repositorio no inicializado", http.StatusInternalServerError)
		return
	}
	documentID, err := h.docRepo.CreateDocument(r.Context(), ipcCtx.TenantID, header.Filename)
	if err != nil {
		log.Printf("upload_error cause=create_document filename=%s object_key=%s error=%v", header.Filename, objectKey, err)
		http.Error(w, "error creando document: "+err.Error(), http.StatusInternalServerError)
		return
	}

	documentVersionID, err := h.docRepo.CreateDocumentVersion(
		r.Context(),
		ipcCtx.TenantID,
		documentID,
		1,
		contentHash,
		contentType,
		int64(len(fileBytes)),
		objectKey,
	)
	if err != nil {
		log.Printf("upload_error cause=create_document_version filename=%s document_id=%s object_key=%s error=%v", header.Filename, documentID, objectKey, err)
		http.Error(w, "error creando document_version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	err = h.docRepo.CreateRawObject(
		r.Context(),
		ipcCtx.TenantID,
		documentVersionID,
		objectKey,
		h.minioRepo.Bucket(),
		contentHash,
		contentType,
		int64(len(fileBytes)),
	)
	if err != nil {
		log.Printf("upload_error cause=create_raw_object filename=%s document_id=%s document_version_id=%s object_key=%s error=%v", header.Filename, documentID, documentVersionID, objectKey, err)
		http.Error(w, "error creando raw_object: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("upload_document_created filename=%s document_id=%s document_version_id=%s object_key=%s", header.Filename, documentID, documentVersionID, objectKey)

	if h.jobRepo == nil {
		log.Printf("upload_error cause=job_repo_nil filename=%s document_id=%s document_version_id=%s", header.Filename, documentID, documentVersionID)
		http.Error(w, "error creando job: repositorio no inicializado", http.StatusInternalServerError)
		return
	}
	jobID, err := h.jobRepo.CreateJob(
		r.Context(),
		ipcCtx.TenantID,
		documentID,
		documentVersionID,
		ipcCtx.UserID,
		ipcCtx.RequestID,
		ipcCtx.AuditNonce,
	)
	if err != nil {
		log.Printf("upload_error cause=create_job filename=%s document_id=%s document_version_id=%s object_key=%s error=%v", header.Filename, documentID, documentVersionID, objectKey, err)
		http.Error(w, "error creando job: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("upload_job_created filename=%s document_id=%s document_version_id=%s job_id=%s", header.Filename, documentID, documentVersionID, jobID)

	err = h.jobRepo.CreateJobStep(
		r.Context(),
		ipcCtx.TenantID,
		jobID,
		"upload_completed",
		1,
		"completed",
	)
	if err != nil {
		log.Printf("upload_error cause=create_job_step filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
	}

	if h.auditRepo != nil {
		err = h.auditRepo.Create(
			r.Context(),
			ipcCtx.TenantID,
			ipcCtx.UserID,
			jobID,
			ipcCtx.RequestID,
			ipcCtx.AuditNonce,
			"document_uploaded",
			"document",
			documentID,
			map[string]interface{}{
				"filename":            header.Filename,
				"content_type":        contentType,
				"content_hash":        contentHash,
				"document_version_id": documentVersionID,
				"object_key":          objectKey,
			},
		)
		if err != nil {
			log.Printf("upload_error cause=create_audit_log filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
		}
	}

	event := fmt.Sprintf(`{"job_id":"%s","document_id":"%s","document_version_id":"%s","tenant_id":"%s","object_key":"%s"}`,
		jobID,
		documentID,
		documentVersionID,
		ipcCtx.TenantID,
		objectKey,
	)
	if h.natsRepo == nil {
		log.Printf("upload_error cause=nats_repo_nil filename=%s document_id=%s job_id=%s", header.Filename, documentID, jobID)
		http.Error(w, "error publicando evento al worker: repositorio no inicializado", http.StatusInternalServerError)
		return
	}
	if err := h.natsRepo.Publish("documents.uploaded", []byte(event)); err != nil {
		log.Printf("upload_error cause=publish_upload_event filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
		http.Error(w, "error publicando evento al worker: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("upload_event_published filename=%s document_id=%s document_version_id=%s job_id=%s subject=%s", header.Filename, documentID, documentVersionID, jobID, "documents.uploaded")

	legacyMessage := fmt.Sprintf(
		"upload ok | document_id=%s | document_version_id=%s | job_id=%s | object_key=%s",
		documentID, documentVersionID, jobID, objectKey,
	)
	response := uploadResponse{
		DocumentID:        documentID,
		DocumentVersionID: documentVersionID,
		JobID:             jobID,
		ObjectKey:         objectKey,
		Message:           "upload ok",
		Legacy:            legacyMessage,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("upload_error cause=encode_response filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
	}
}

func validateUploadSize(filename string, contentType string, sizeBytes int64) error {
	limit := uploadSizeLimit(filename, contentType)
	if sizeBytes <= limit {
		return nil
	}

	return fmt.Errorf(
		"El archivo es demasiado grande. Tamaño máximo permitido para %s: %s.",
		uploadCategoryName(filename, contentType),
		formatMegabytes(limit),
	)
}

func uploadSizeLimit(filename string, contentType string) int64 {
	if isImageUpload(filename, contentType) {
		return maxImageUploadBytes
	}
	return maxDocumentUploadBytes
}

func uploadCategoryName(filename string, contentType string) string {
	if isImageUpload(filename, contentType) {
		return "imágenes"
	}
	if isDocumentUpload(filename, contentType) {
		return "documentos"
	}
	return "archivos"
}

func isDocumentUpload(filename string, contentType string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".txt", ".pdf", ".docx", ".xlsx", ".csv":
		return true
	}

	switch normalizeContentType(contentType) {
	case "text/plain",
		"text/csv",
		"application/csv",
		"application/pdf",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return true
	default:
		return false
	}
}

func isImageUpload(filename string, contentType string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tif", ".tiff":
		return true
	}

	return strings.HasPrefix(normalizeContentType(contentType), "image/")
}

func normalizeContentType(contentType string) string {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = contentType[:idx]
	}
	return contentType
}

func formatMegabytes(sizeBytes int64) string {
	return fmt.Sprintf("%d MB", sizeBytes/(1024*1024))
}

func isAllowedDocumentUpload(filename string, contentType string) bool {
	if isImageUpload(filename, contentType) {
		return true
	}

	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".txt", ".pdf", ".docx", ".csv", ".xlsx":
		return true
	}

	switch normalizeContentType(contentType) {
	case "text/plain",
		"text/csv",
		"application/csv",
		"application/pdf",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return true
	default:
		return false
	}
}

func openUploadFile(r *http.Request) (multipart.File, *multipart.FileHeader, error) {
	for _, fieldName := range []string{"file", "document", "document_file", "files[]"} {
		file, header, err := r.FormFile(fieldName)
		if err == nil {
			return file, header, nil
		}
	}
	return nil, nil, http.ErrMissingFile
}
