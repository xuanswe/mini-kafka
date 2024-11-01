package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xuanswe/mini-kafka/internal/server"
	"os"
	"time"
)

func main() {
	setupLogger()

	kafkaConfig := server.KafkaServerConfig{
		Host: "0.0.0.0",
		Port: "9092",
	}
	kafkaServer, err := server.NewKafkaServer(kafkaConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating Kafka server")
	}

	if err := kafkaServer.Start(); err != nil {
		log.Fatal().Err(err).Msg("Error starting Kafka server")
	}

	defer func() {
		if err := kafkaServer.Close(); err != nil {
			log.Fatal().Err(err).Msg("Error closing Kafka server")
		}
	}()
}

func setupLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	// Set the output to console
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339Nano,
	})
}
