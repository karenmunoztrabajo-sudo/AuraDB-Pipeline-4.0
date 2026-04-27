package service

import "math/rand"

func GenerateFakeEmbedding() []float64 {
	vec := make([]float64, 10)
	for i := range vec {
		vec[i] = rand.Float64()
	}
	return vec
}