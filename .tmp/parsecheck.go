package main

import (
  "fmt"
  "os"
  "auradb-pipeline/internal/service"
)

func main() {
  data, err := os.ReadFile(".tmp/carta-kelly.pdf")
  if err != nil { panic(err) }
  result, err := service.ParseDocument("carta kelly.pdf", "application/pdf", data)
  fmt.Printf("result=%+v\nerr=%v\n", result, err)
  if len(result.Content) > 500 { fmt.Println(result.Content[:500]) } else { fmt.Println(result.Content) }
}
