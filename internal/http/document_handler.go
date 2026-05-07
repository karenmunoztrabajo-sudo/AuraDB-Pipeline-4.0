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

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

const (
	maxDocumentUploadBytes = 25 * 1024 * 1024
	maxImageUploadBytes    = 10 * 1024 * 1024
	uploadEventSubject     = "documents.uploaded"
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
	CollectionID      string `json:"collection_id,omitempty"`
	Message           string `json:"message"`
	Legacy            string `json:"legacy"`
}

type favoriteRequest struct {
	Favorite bool `json:"favorite"`
}

type tagRequest struct {
	Tag string `json:"tag"`
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
	log.Printf("upload_attempt endpoint=%s tenant_id=%s user_id=%s", r.URL.Path, ipcCtx.TenantID, ipcCtx.UserID)

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
	collectionID := strings.TrimSpace(r.FormValue("collection_id"))
	if collectionID != "" && h.docRepo != nil {
		exists, err := h.docRepo.CollectionExists(r.Context(), ipcCtx.TenantID, collectionID)
		if err != nil {
			log.Printf("upload_error cause=validate_collection collection_id=%s error=%v", collectionID, err)
			http.Error(w, "error validando coleccion: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if !exists {
			log.Printf("upload_error cause=collection_not_found collection_id=%s", collectionID)
			http.Error(w, "coleccion no encontrada", http.StatusBadRequest)
			return
		}
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
	if collectionID != "" {
		if err := h.docRepo.AssignDocumentToCollection(r.Context(), ipcCtx.TenantID, documentID, collectionID); err != nil {
			log.Printf("upload_error cause=assign_collection filename=%s document_id=%s collection_id=%s error=%v", header.Filename, documentID, collectionID, err)
			http.Error(w, "error asignando coleccion: "+err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("upload_collection_assigned document_id=%s collection_id=%s", documentID, collectionID)
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
	log.Printf("event_publish_attempt subject=%s document_id=%s document_version_id=%s job_id=%s", uploadEventSubject, documentID, documentVersionID, jobID)
	if h.natsRepo == nil {
		log.Printf("event_publish_error subject=%s document_id=%s document_version_id=%s job_id=%s error=%s", uploadEventSubject, documentID, documentVersionID, jobID, "nats_repo_nil")
		log.Printf("upload_error cause=nats_repo_nil filename=%s document_id=%s job_id=%s", header.Filename, documentID, jobID)
		http.Error(w, "error publicando evento al worker: repositorio no inicializado", http.StatusInternalServerError)
		return
	}
	if err := h.natsRepo.Publish(uploadEventSubject, []byte(event)); err != nil {
		log.Printf("event_publish_error subject=%s document_id=%s document_version_id=%s job_id=%s error=%v", uploadEventSubject, documentID, documentVersionID, jobID, err)
		log.Printf("upload_error cause=publish_upload_event filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
		http.Error(w, "error publicando evento al worker: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("event_publish_success subject=%s document_id=%s document_version_id=%s job_id=%s", uploadEventSubject, documentID, documentVersionID, jobID)
	log.Printf("upload_event_published filename=%s document_id=%s document_version_id=%s job_id=%s subject=%s", header.Filename, documentID, documentVersionID, jobID, uploadEventSubject)

	legacyMessage := fmt.Sprintf(
		"upload ok | document_id=%s | document_version_id=%s | job_id=%s | object_key=%s",
		documentID, documentVersionID, jobID, objectKey,
	)
	response := uploadResponse{
		DocumentID:        documentID,
		DocumentVersionID: documentVersionID,
		JobID:             jobID,
		ObjectKey:         objectKey,
		CollectionID:      collectionID,
		Message:           "upload ok",
		Legacy:            legacyMessage,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("upload_error cause=encode_response filename=%s document_id=%s job_id=%s error=%v", header.Filename, documentID, jobID, err)
	}
}

func (h *DocumentHandler) Favorites(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	if h.docRepo == nil {
		http.Error(w, "repositorio no inicializado", http.StatusInternalServerError)
		return
	}

	favorites, err := h.docRepo.ListFavorites(r.Context(), ipcCtx.TenantID)
	if err != nil {
		http.Error(w, "error listando favoritos: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("favorites_loaded count=%d", len(favorites))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(favorites)
}

func (h *DocumentHandler) Action(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(strings.Trim(r.URL.Path, "/"), "/favorite") {
		h.Favorite(w, r)
		return
	}
	if strings.HasSuffix(strings.Trim(r.URL.Path, "/"), "/tags") {
		h.Tags(w, r)
		return
	}
	http.Error(w, "ruta de documento no encontrada", http.StatusNotFound)
}

func (h *DocumentHandler) Favorite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	documentID := documentIDFromFavoritePath(r.URL.Path)
	log.Printf("favorite_request_received document_id=%s path=%s", documentID, r.URL.Path)
	if documentID == "" {
		log.Printf("favorite_update_error document_id=%s error=%s", documentID, "invalid_document_id")
		http.Error(w, "document_id requerido", http.StatusBadRequest)
		return
	}

	var req favoriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("favorite_update_error document_id=%s error=%v", documentID, err)
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if h.docRepo == nil {
		log.Printf("favorite_update_error document_id=%s error=%s", documentID, "document_repo_nil")
		http.Error(w, "repositorio no inicializado", http.StatusInternalServerError)
		return
	}
	if err := h.docRepo.SetFavorite(r.Context(), ipcCtx.TenantID, documentID, req.Favorite); err != nil {
		log.Printf("favorite_update_error document_id=%s favorite=%t error=%v", documentID, req.Favorite, err)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "documento no encontrado", http.StatusNotFound)
			return
		}
		http.Error(w, "error actualizando favorito: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("favorite_update_success document_id=%s favorite=%t", documentID, req.Favorite)
	log.Printf("favorite_updated document_id=%s favorite=%t", documentID, req.Favorite)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"document_id": documentID,
		"favorite":    req.Favorite,
	})
}

func (h *DocumentHandler) Tags(w http.ResponseWriter, r *http.Request) {
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	documentID := documentIDFromTagsPath(r.URL.Path)
	if documentID == "" {
		http.Error(w, "document_id requerido", http.StatusBadRequest)
		return
	}
	if h.docRepo == nil {
		http.Error(w, "repositorio no inicializado", http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodGet:
		tags, err := h.docRepo.ListDocumentTags(r.Context(), ipcCtx.TenantID, documentID)
		if err != nil {
			http.Error(w, "error listando tags: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tags)
	case http.MethodPost:
		var req tagRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "json invalido", http.StatusBadRequest)
			return
		}
		tag := normalizeDocumentTag(req.Tag)
		if tag == "" {
			http.Error(w, "tag requerido", http.StatusBadRequest)
			return
		}
		item, err := h.docRepo.AddDocumentTag(r.Context(), ipcCtx.TenantID, documentID, tag)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "documento no encontrado", http.StatusNotFound)
				return
			}
			http.Error(w, "error agregando tag: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	case http.MethodDelete:
		tag := normalizeDocumentTag(r.URL.Query().Get("tag"))
		if tag == "" {
			var req tagRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			tag = normalizeDocumentTag(req.Tag)
		}
		if tag == "" {
			http.Error(w, "tag requerido", http.StatusBadRequest)
			return
		}
		if err := h.docRepo.DeleteDocumentTag(r.Context(), ipcCtx.TenantID, documentID, tag); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "error eliminando tag: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
	}
}

func (h *DocumentHandler) DocumentsByTag(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	tag := tagFromDocumentsByTagPath(r.URL.Path)
	if tag == "" {
		http.Error(w, "tag requerido", http.StatusBadRequest)
		return
	}
	documents, err := h.docRepo.ListDocumentsByTag(r.Context(), ipcCtx.TenantID, tag)
	if err != nil {
		http.Error(w, "error listando documentos por tag: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(documents)
}

func documentIDFromFavoritePath(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[0] != "documents" || parts[2] != "favorite" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func documentIDFromTagsPath(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[0] != "documents" || parts[2] != "tags" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func tagFromDocumentsByTagPath(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[0] != "tags" || parts[2] != "documents" {
		return ""
	}
	return normalizeDocumentTag(parts[1])
}

func normalizeDocumentTag(tag string) string {
	tag = strings.TrimSpace(strings.ToLower(tag))
	tag = strings.TrimPrefix(tag, "#")
	tag = strings.Join(strings.Fields(tag), "-")
	return tag
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
	case ".txt", ".pdf", ".docx", ".xlsx", ".xls", ".csv":
		return true
	}

	switch normalizeContentType(contentType) {
	case "text/plain",
		"text/csv",
		"application/csv",
		"application/pdf",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.ms-excel":
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
	case ".txt", ".pdf", ".docx", ".csv", ".xlsx", ".xls":
		return true
	}

	switch normalizeContentType(contentType) {
	case "text/plain",
		"text/csv",
		"application/csv",
		"application/pdf",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.ms-excel":
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
