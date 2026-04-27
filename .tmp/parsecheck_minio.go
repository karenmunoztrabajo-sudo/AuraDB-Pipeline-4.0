package main

import (
  "context"
  "fmt"
  "io"
  "os"

  "auradb-pipeline/internal/config"
  "auradb-pipeline/internal/repository/storage"
  "auradb-pipeline/internal/service"

  "github.com/minio/minio-go/v7"
)

func main() {
  cfg := config.Load()
  repo, err := storage.NewMinioRepository(cfg)
  if err != nil { panic(err) }
  objectKey := "14168b79-719b-4578-832a-d571bc2da2d5/1776436130602958600_carta kelly.pdf"
  obj, err := repo.Client().GetObject(context.Background(), repo.Bucket(), objectKey, minio.GetObjectOptions{})
  if err != nil { panic(err) }
  defer obj.Close()
  data, err := io.ReadAll(obj)
  if err != nil { panic(err) }
  _ = os.WriteFile(".tmp/carta-kelly-api.pdf", data, 0644)
  fmt.Printf("bytes=%d header=%q\n", len(data), string(data[:8]))
  result, err := service.ParseDocument("carta kelly.pdf", "application/pdf", data)
  fmt.Printf("result=%+v\nerr=%v\n", result, err)
  if len(result.Content) > 500 { fmt.Println(result.Content[:500]) } else { fmt.Println(result.Content) }
}
