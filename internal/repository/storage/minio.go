package storage

import (
	"context" // Importante para las llamadas a MinIO
	"log"
	"auradb-pipeline/internal/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioRepository struct {
	client *minio.Client
	bucket string
}

func NewMinioRepository(cfg config.Config) (*MinioRepository, error) {
	client, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	// --- Lógica para asegurar que el bucket existe ---
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, cfg.MinioBucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		log.Printf("El bucket %s no existe, creándolo...", cfg.MinioBucket)
		err = client.MakeBucket(ctx, cfg.MinioBucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
		log.Printf("Bucket %s creado con éxito", cfg.MinioBucket)
	}
	// ------------------------------------------------

	return &MinioRepository{
		client: client,
		bucket: cfg.MinioBucket,
	}, nil
}

func (r *MinioRepository) Client() *minio.Client {
	return r.client
}

func (r *MinioRepository) Bucket() string {
	return r.bucket
}
