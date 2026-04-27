package service

import (
	"context"

	"auradb-pipeline/internal/model"
	"auradb-pipeline/internal/repository/postgres"
)

type TenantService struct {
	repo *postgres.TenantRepository
}

func NewTenantService(repo *postgres.TenantRepository) *TenantService {
	return &TenantService{repo: repo}
}

func (s *TenantService) CreateTenant(ctx context.Context, name, slug, plan string) (*model.Tenant, error) {
	if plan == "" {
		plan = "core"
	}
	return s.repo.Create(ctx, name, slug, plan)
}	