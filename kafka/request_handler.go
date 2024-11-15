package kafka

import (
	"github.com/rs/zerolog/log"
	"github.com/xuanswe/mini-kafka/internal/models"
)

func handleRequest(request *models.Request) ([]byte, error) {
	log.Debug().Msgf("Processing request: %v", request)
	return []byte("Hello " + request.Data + "!"), nil
}
