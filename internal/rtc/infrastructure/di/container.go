package di

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/infrastructure/config"
	queryserver "github.com/c4erries/mint/internal/rtc/infrastructure/in/grpc/query_server"
	livekitwebhook "github.com/c4erries/mint/internal/rtc/infrastructure/in/http/livekit_webhook"
	commandconsumer "github.com/c4erries/mint/internal/rtc/infrastructure/in/kafka/command_consumer"
	grpcclients "github.com/c4erries/mint/internal/rtc/infrastructure/out/grpc/clients"
	eventpublisher "github.com/c4erries/mint/internal/rtc/infrastructure/out/kafka/event_publisher"
	livekitclient "github.com/c4erries/mint/internal/rtc/infrastructure/out/livekit/client"
	"github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/inmemory"
)

// Container assembles rtc-api dependency graph.
type Container struct {
	HTTPServer      *http.Server
	CommandConsumer *commandconsumer.Consumer
	OutboxRelay     *eventpublisher.OutboxRelay
	QueryServer     *queryserver.Server
}

func NewContainer(cfg config.Config, logger *slog.Logger) (*Container, error) {
	roomStore := inmemory.NewRoomStore()
	grantStore := inmemory.NewGrantStore(time.Now)
	permissionClient := grpcclients.NewPermissionClient()
	liveKitClient := livekitclient.NewTokenClient(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, time.Now)

	commandService, err := application.NewCommandService(
		roomStore,
		grantStore,
		permissionClient,
		liveKitClient,
		application.CommandServiceOptions{
			DefaultTokenTTL: cfg.DefaultTokenTTL,
			Now:             time.Now,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("build command service: %w", err)
	}

	queryService := application.NewQueryService(roomStore, grantStore)
	grpcQueryServer := queryserver.New(queryService)

	producer := eventpublisher.NewInMemoryProducer()
	publisher := eventpublisher.NewPublisher(producer, cfg.RTCEventsTopic)
	outboxRelay, err := eventpublisher.NewOutboxRelay(roomStore, publisher, logger, cfg.OutboxBatchSize)
	if err != nil {
		return nil, fmt.Errorf("build outbox relay: %w", err)
	}

	consumer := commandconsumer.New(commandconsumer.NoopReader{}, commandService, logger)
	webhookHandler := livekitwebhook.New(cfg.LiveKitWebhookSecret, commandService, logger)

	mux := http.NewServeMux()
	mux.Handle("/livekit/webhook", webhookHandler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		if _, writeErr := writer.Write([]byte("ok")); writeErr != nil {
			logger.Error("failed to write health response", slog.String("error", writeErr.Error()))
		}
	})

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &Container{
		HTTPServer:      httpServer,
		CommandConsumer: consumer,
		OutboxRelay:     outboxRelay,
		QueryServer:     grpcQueryServer,
	}, nil
}
