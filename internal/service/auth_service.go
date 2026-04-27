package service

import (
	"context"
	"errors"
	"time"

	"auradb-pipeline/internal/model"
	"auradb-pipeline/internal/repository/postgres"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	userRepo  *postgres.UserRepository
	jwtSecret string
}

func NewAuthService(userRepo *postgres.UserRepository, jwtSecret string) *AuthService {
	return &AuthService{
		userRepo:  userRepo,
		jwtSecret: jwtSecret,
	}
}

func (s *AuthService) RegisterAdmin(ctx context.Context, tenantID, email, password, fullName string) (*model.User, error) {
	if tenantID == "" || email == "" || password == "" {
		return nil, errors.New("tenant_id, email y password son obligatorios")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	return s.userRepo.Create(ctx, tenantID, email, string(hash), "admin", fullName)
}

func (s *AuthService) Login(ctx context.Context, tenantID, email, password string) (string, error) {
	user, err := s.userRepo.FindByEmail(ctx, tenantID, email)
	if err != nil {
		return "", errors.New("credenciales inválidas")
	}

	if !user.IsActive {
		return "", errors.New("usuario inactivo")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return "", errors.New("credenciales inválidas")
	}

	claims := jwt.MapClaims{
		"tenant_id": user.TenantID,
		"user_id":   user.ID,
		"role":      user.Role,
		"exp":       time.Now().Add(24 * time.Hour).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(s.jwtSecret))
	if err != nil {
		return "", err
	}

	return signed, nil
}