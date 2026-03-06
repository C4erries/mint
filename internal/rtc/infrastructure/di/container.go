package di

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	redisrepo "github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/redis"
	scyllarepo "github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/scylla"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

// Container assembles rtc-api dependency graph.
type Container struct {
	HTTPServer      *http.Server
	GRPCServer      *grpc.Server
	GRPCListener    net.Listener
	CommandConsumer *commandconsumer.Consumer
	OutboxRelay     *eventpublisher.OutboxRelay
	QueryServer     *queryserver.Server

	closers []io.Closer
}

func NewContainer(cfg config.Config, logger *slog.Logger) (*Container, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	closers := make([]io.Closer, 0, 4)
	cleanup := func() {
		for index := len(closers) - 1; index >= 0; index-- {
			_ = closers[index].Close()
		}
	}

	roomStore, err := scyllarepo.NewStore(scyllarepo.Options{
		Hosts:            cfg.ScyllaHosts,
		Port:             cfg.ScyllaPort,
		Keyspace:         cfg.ScyllaKeyspace,
		Consistency:      cfg.ScyllaConsistency,
		AutoCreateSchema: cfg.ScyllaAutoCreateSchema,
		Now:              time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("build scylla room store: %w", err)
	}
	closers = append(closers, roomStore)

	grantStore, err := redisrepo.NewGrantStore(redisrepo.Options{
		Addr:      cfg.RedisAddr,
		Password:  cfg.RedisPassword,
		DB:        cfg.RedisDB,
		KeyPrefix: cfg.RedisKeyPrefix,
		Now:       time.Now,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build redis grant store: %w", err)
	}
	closers = append(closers, grantStore)

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
		cleanup()
		return nil, fmt.Errorf("build command service: %w", err)
	}

	queryService := application.NewQueryService(roomStore, grantStore)
	grpcQueryServer := queryserver.New(queryService)

	producer, err := eventpublisher.NewKafkaGoProducer(eventpublisher.KafkaGoProducerConfig{Brokers: cfg.KafkaBrokers}, nil)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build kafka producer: %w", err)
	}
	closers = append(closers, producer)

	publisher := eventpublisher.NewPublisher(producer, cfg.RTCEventsTopic)
	outboxRelay, err := eventpublisher.NewOutboxRelay(roomStore, publisher, logger, cfg.OutboxBatchSize)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build outbox relay: %w", err)
	}

	kafkaReader, err := commandconsumer.NewKafkaReader(commandconsumer.KafkaReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.RTCCommandsTopic,
		GroupID:        cfg.KafkaConsumerGroup,
		CommitInterval: 1 * time.Second,
	}, nil)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build kafka command reader: %w", err)
	}
	closers = append(closers, kafkaReader)

	consumer := commandconsumer.New(kafkaReader, commandService, logger)
	webhookHandler := livekitwebhook.New(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, commandService, logger)

	router := gin.New()
	router.Use(gin.Recovery())
	router.POST("/livekit/webhook", gin.WrapH(webhookHandler))
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcServer := grpc.NewServer()
	grpcHealthServer := grpcHealth.NewServer()
	grpcHealthV1.RegisterHealthServer(grpcServer, grpcHealthServer)
	grpcHealthServer.SetServingStatus("", grpcHealthV1.HealthCheckResponse_SERVING)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("listen grpc on %s: %w", cfg.GRPCAddr, err)
	}

	return &Container{
		HTTPServer:      httpServer,
		GRPCServer:      grpcServer,
		GRPCListener:    grpcListener,
		CommandConsumer: consumer,
		OutboxRelay:     outboxRelay,
		QueryServer:     grpcQueryServer,
		closers:         closers,
	}, nil
}

func (c *Container) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	for index := len(c.closers) - 1; index >= 0; index-- {
		err := c.closers[index].Close()
		if err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}
