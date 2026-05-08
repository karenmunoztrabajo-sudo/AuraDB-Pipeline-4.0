package http

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"auradb-pipeline/internal/repository/postgres"

	"github.com/jackc/pgx/v5"
)

type CollectionHandler struct {
	docRepo *postgres.DocumentRepository
}

func NewCollectionHandler(docRepo *postgres.DocumentRepository) *CollectionHandler {
	return &CollectionHandler{docRepo: docRepo}
}

type createCollectionRequest struct {
	Name string `json:"name"`
}

type assignCollectionRequest struct {
	CollectionID string `json:"collection_id"`
	DocumentID   string `json:"document_id"`
}

type collectionListResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (h *CollectionHandler) Collections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.List(w, r)
	case http.MethodPost:
		h.Create(w, r)
	default:
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
	}
}

func (h *CollectionHandler) Create(w http.ResponseWriter, r *http.Request) {
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	var req createCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		http.Error(w, "nombre requerido", http.StatusBadRequest)
		return
	}

	collection, err := h.docRepo.CreateCollection(r.Context(), ipcCtx.TenantID, name)
	if err != nil {
		http.Error(w, "error creando coleccion: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(collection)
}

func (h *CollectionHandler) List(w http.ResponseWriter, r *http.Request) {
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	collections, err := h.docRepo.ListCollections(r.Context(), ipcCtx.TenantID)
	if err != nil {
		http.Error(w, "error listando colecciones: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("collections_loaded count=%d tenant_id=%s", len(collections), ipcCtx.TenantID)

	response := make([]collectionListResponse, 0, len(collections))
	for _, collection := range collections {
		response = append(response, collectionListResponse{
			ID:   collection.ID,
			Name: collection.Name,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (h *CollectionHandler) Assign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	var req assignCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	collectionID := strings.TrimSpace(req.CollectionID)
	documentID := strings.TrimSpace(req.DocumentID)
	if collectionID == "" || documentID == "" {
		http.Error(w, "collection_id y document_id requeridos", http.StatusBadRequest)
		return
	}

	if err := h.docRepo.AssignDocumentToCollection(r.Context(), ipcCtx.TenantID, documentID, collectionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "documento o coleccion no encontrada", http.StatusNotFound)
			return
		}
		http.Error(w, "error asignando documento: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *CollectionHandler) Documents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "metodo no permitido", http.StatusMethodNotAllowed)
		return
	}
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}
	collectionID := strings.TrimSpace(r.URL.Query().Get("collection_id"))
	if collectionID == "" {
		collectionID = collectionIDFromDocumentsPath(r.URL.Path)
	}
	if collectionID == "" {
		http.Error(w, "collection_id requerido", http.StatusBadRequest)
		return
	}

	documents, err := h.docRepo.ListCollectionDocuments(r.Context(), ipcCtx.TenantID, collectionID)
	if err != nil {
		http.Error(w, "error listando documentos: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("collection_documents_count=%d", len(documents))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(documents)
}

func collectionIDFromDocumentsPath(path string) string {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/collections/") || !strings.HasSuffix(path, "/documents") {
		return ""
	}
	collectionID := strings.TrimPrefix(path, "/collections/")
	collectionID = strings.TrimSuffix(collectionID, "/documents")
	return strings.Trim(collectionID, "/")
}
