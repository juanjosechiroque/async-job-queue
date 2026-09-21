package text

import "math/rand"

type Generator struct {
	samples []string
}

func NewGenerator() *Generator {
	return &Generator{
		samples: []string{
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
			"Integer feugiat massa vitae sapien consequat tincidunt.",
			"Sed posuere neque at justo commodo.",
			"Praesent interdum felis at libero malesuada.",
			"Aliquam erat volutpat, sed tincidunt massa.",
		},
	}
}

func (g *Generator) Generate(rng *rand.Rand) string {
	return g.samples[rng.Intn(len(g.samples))]
}
