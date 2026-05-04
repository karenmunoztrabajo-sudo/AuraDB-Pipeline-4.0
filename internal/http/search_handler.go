package http

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"auradb-pipeline/internal/service"
)

type SearchHandler struct {
	searchService *service.SearchService
}

func NewSearchHandler(searchService *service.SearchService) *SearchHandler {
	return &SearchHandler{searchService: searchService}
}

func (h *SearchHandler) Search(w http.ResponseWriter, r *http.Request) {
	ipcCtx, ok := GetIPC(r)
	if !ok {
		http.Error(w, "no autorizado", http.StatusUnauthorized)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "faltó parámetro q", http.StatusBadRequest)
		return
	}

	topK := 3
	if rawTopK := r.URL.Query().Get("top_k"); rawTopK != "" {
		if parsed, err := strconv.Atoi(rawTopK); err == nil && parsed > 0 {
			topK = parsed
		}
	}

	documentID := r.URL.Query().Get("document_id")
	documentIDs := parseDocumentIDs(r)
	if len(documentIDs) == 0 && documentID != "" {
		documentIDs = []string{documentID}
	}
	selectedDocumentIDs := strings.Join(documentIDs, ",")
	multiDocumentMode := len(documentIDs) == 0 || len(documentIDs) > 1
	log.Printf("search_handler selected_document_ids=%q multi_document_mode=%t", selectedDocumentIDs, multiDocumentMode)

	results, err := h.searchService.SearchWithDocumentIDs(r.Context(), ipcCtx.TenantID, query, topK, documentIDs)
	if err != nil {
		http.Error(w, "error en búsqueda: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}
