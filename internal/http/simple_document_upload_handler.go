package http

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"auradb-pipeline/internal/repository/postgres"
)

const maxSimpleUploadBytes = 20 * 1024 * 1024

type SimpleDocumentUploadHandler struct {
	docRepo   *postgres.DocumentRepository
	uploadDir string
}

type simpleUploadResponse struct {
	ID           string `json:"id"`
	OriginalName string `json:"original_name"`
	StoredName   string `json:"stored_name"`
	MimeType     string `json:"mime_type"`
	Size         int64  `json:"size"`
	Path         string `json:"path"`
	Status       string `json:"status"`
}

func NewSimpleDocumentUploadHandler(docRepo *postgres.DocumentRepository, uploadDir string) *SimpleDocumentUploadHandler {
	if strings.TrimSpace(uploadDir) == "" {
		uploadDir = filepath.Join("uploads", "documents")
	}
	return &SimpleDocumentUploadHandler{
		docRepo:   docRepo,
		uploadDir: uploadDir,
	}
}

func (h *SimpleDocumentUploadHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}

	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	log.Printf("upload_attempt endpoint=/api/documents/upload tenant_id=%s user_id=%s", ipcCtx.TenantID, ipcCtx.UserID)

	r.Body = http.MaxBytesReader(w, r.Body, maxSimpleUploadBytes+1024)
	if err := r.ParseMultipartForm(maxSimpleUploadBytes); err != nil {
		http.Error(w, "archivo demasiado grande o multipart invalido", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "archivo requerido en campo file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	if header.Size > maxSimpleUploadBytes {
		http.Error(w, "El archivo supera el tamaño máximo permitido de 20 MB", http.StatusRequestEntityTooLarge)
		return
	}

	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(header.Filename), "."))
	if !isSimpleUploadExtensionAllowed(ext) {
		http.Error(w, "Formato de archivo no soportado", http.StatusBadRequest)
		return
	}

	if err := os.MkdirAll(h.uploadDir, 0755); err != nil {
		http.Error(w, "error creando carpeta de uploads", http.StatusInternalServerError)
		return
	}

	storedName := fmt.Sprintf("%s.%s", randomHex(16), ext)
	storedPath := filepath.Join(h.uploadDir, storedName)

	dst, err := os.OpenFile(storedPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		http.Error(w, "error creando archivo destino", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	size, err := io.Copy(dst, file)
	if err != nil {
		http.Error(w, "error guardando archivo", http.StatusInternalServerError)
		return
	}
	if size > maxSimpleUploadBytes {
		_ = os.Remove(storedPath)
		http.Error(w, "El archivo supera el tamaño máximo permitido de 20 MB", http.StatusRequestEntityTooLarge)
		return
	}

	mimeType := header.Header.Get("Content-Type")
	if strings.TrimSpace(mimeType) == "" {
		mimeType = mimeTypeForExtension(ext)
	}

	documentID, err := h.docRepo.CreateUploadedDocument(
		r.Context(),
		ipcCtx.TenantID,
		ipcCtx.UserID,
		header.Filename,
		storedName,
		mimeType,
		size,
		storedPath,
	)
	if err != nil {
		_ = os.Remove(storedPath)
		http.Error(w, "error guardando documento en base de datos: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(simpleUploadResponse{
		ID:           documentID,
		OriginalName: header.Filename,
		StoredName:   storedName,
		MimeType:     mimeType,
		Size:         size,
		Path:         storedPath,
		Status:       "UPLOADED",
	})
}

func isSimpleUploadExtensionAllowed(ext string) bool {
	switch ext {
	case "pdf", "docx", "jpg", "jpeg", "png":
		return true
	default:
		return false
	}
}

func mimeTypeForExtension(ext string) string {
	switch ext {
	case "pdf":
		return "application/pdf"
	case "docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "xls":
		return "application/vnd.ms-excel"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	default:
		return "application/octet-stream"
	}
}

func randomHex(bytesLen int) string {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return strings.ReplaceAll(fmt.Sprintf("%d", os.Getpid()), "-", "")
	}
	return hex.EncodeToString(buf)
}
