package text

import (
	"math/rand"
	"testing"
)

func TestGeneratorGenerateReturnsSample(t *testing.T) {
	samples := []string{
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		"Integer feugiat massa vitae sapien consequat tincidunt.",
		"Sed posuere neque at justo commodo.",
		"Praesent interdum felis at libero malesuada.",
		"Aliquam erat volutpat, sed tincidunt massa.",
	}

	knownSamples := make(map[string]struct{}, len(samples))
	for _, sample := range samples {
		knownSamples[sample] = struct{}{}
	}

	generator := NewGenerator()
	rng := rand.New(rand.NewSource(1))
	seen := make(map[string]struct{})
	for range 50 {
		result := generator.Generate(rng)
		if _, ok := knownSamples[result]; !ok {
			t.Fatalf("Generate() returned unknown sample %q", result)
		}
		seen[result] = struct{}{}
	}

	if len(seen) < 2 {
		t.Fatalf("Generate() returned only %d distinct sample", len(seen))
	}
}
