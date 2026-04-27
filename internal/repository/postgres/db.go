package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"auradb-pipeline/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

const legacyPostgresDSN = "postgres://auradb:auradb123@localhost:5432/auradb?sslmode=disable"

func NewPool(cfg config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	configuredDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.PostgresUser,
		cfg.PostgresPass,
		cfg.PostgresHost,
		cfg.PostgresPort,
		cfg.PostgresDB,
	)

	if pool, err := openVerifiedPool(ctx, configuredDSN); err == nil {
		return pool, nil
	} else {
		log.Printf("postgres_configured_dsn_unavailable dsn=%q error=%v", configuredDSN, err)
	}

	if configuredDSN != legacyPostgresDSN {
		if pool, err := openVerifiedPool(ctx, legacyPostgresDSN); err == nil {
			log.Printf("postgres_fallback_legacy_dsn_enabled dsn=%q", legacyPostgresDSN)
			return pool, nil
		} else {
			log.Printf("postgres_legacy_dsn_unavailable dsn=%q error=%v", legacyPostgresDSN, err)
		}
	}

	return nil, fmt.Errorf("no se pudo conectar a una base compatible usando DSN configurado ni fallback legado")
}

func openVerifiedPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	if err := verifyExpectedSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	if err := RunStartupMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func verifyExpectedSchema(ctx context.Context, pool *pgxpool.Pool) error {
	requiredTables := []string{"users", "documents", "document_versions", "jobs", "job_steps", "chunks", "embeddings"}
	for _, tableName := range requiredTables {
		var exists bool
		if err := pool.QueryRow(
			ctx,
			`SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`,
			tableName,
		).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("tabla requerida ausente: %s", tableName)
		}
	}
	return nil
}
