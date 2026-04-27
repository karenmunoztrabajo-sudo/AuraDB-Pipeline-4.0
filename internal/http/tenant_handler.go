package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"auradb-pipeline/internal/service"
)

type TenantHandler struct {
	service *service.TenantService
}

func NewTenantHandler(service *service.TenantService) *TenantHandler {
	return &TenantHandler{service: service}
}

type createTenantRequest struct {
	Name string `json:"name"`
	Slug string `json:"slug"`
	Plan string `json:"plan"`
}

func (h *TenantHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Slug == "" {
		http.Error(w, "name y slug son obligatorios", http.StatusBadRequest)
		return
	}

	tenant, err := h.service.CreateTenant(r.Context(), req.Name, req.Slug, req.Plan)
	if err != nil {
		if strings.Contains(err.Error(), "tenants_slug_key") {
			http.Error(w, "ya existe un tenant con ese slug", http.StatusConflict)
			return
		}
		http.Error(w, "Error al guardar en DB: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(tenant)
}