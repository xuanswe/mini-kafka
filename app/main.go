package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/xuanswe/mini-kafka/internal/support"
	"github.com/xuanswe/mini-kafka/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	support.SetupLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture OS interrupted signals
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGINT, syscall.SIGTERM)

	kafkaConfig := kafka.ServerConfig{
		Host:            "0.0.0.0",
		Port:            "9092",
		ConnIdleTimeout: 1 * time.Minute,
	}
	kafkaServer, err := kafka.NewServer(kafkaConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating kafka server")
	}

	go func() {
		if err := kafkaServer.Start(); err != nil {
			log.Fatal().Err(err).Msg("Error starting kafka server")
		}

		cancel()
	}()

	// Handle OS interrupted signals in a separated goroutine
	go func() {
		sig := <-interrupts
		log.Info().Msgf("Signal intercepted %v", sig)

		if err := kafkaServer.Shutdown(); err != nil {
			log.Fatal().Err(err).Msg("Error closing kafka server")
		}

		cancel()
	}()

	// Wait for the shutdown flow to send true
	<-ctx.Done()
}
