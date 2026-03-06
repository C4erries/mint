package di

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
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
)

const readinessTimeout = 2 * time.Second

type readinessCheck struct {
	name  string
	check func(ctx context.Context) error
}

// Container assembles rtc-api dependency graph and controls runtime lifecycle.
type Container struct {
	httpServer      *http.Server
	grpcServer      *grpc.Server
	grpcListener    net.Listener
	commandConsumer *commandconsumer.Consumer
	outboxRelay     *eventpublisher.OutboxRelay

	outboxPollInterval time.Duration
	readinessChecks    []readinessCheck
	closers            []io.Closer
	logger             *slog.Logger
}

func NewContainer(cfg config.Config, logger *slog.Logger) (*Container, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	closers := make([]io.Closer, 0, 5)
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

	permissionClient, err := grpcclients.NewPermissionClient(grpcclients.Options{
		Address:      cfg.PermissionGRPCAddr,
		Timeout:      cfg.PermissionTimeout,
		MaxRetries:   cfg.PermissionMaxRetries,
		RetryBackoff: cfg.PermissionRetryBackoff,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build permission grpc client: %w", err)
	}

	closers = append(closers, permissionClient)

	liveKitClient, err := livekitclient.NewTokenClient(livekitclient.Options{
		HostURL:   cfg.LiveKitURL,
		APIKey:    cfg.LiveKitAPIKey,
		APISecret: cfg.LiveKitAPISecret,
		Now:       time.Now,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build livekit token client: %w", err)
	}

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

	commandDLQPublisher, err := eventpublisher.NewCommandDLQPublisher(producer, cfg.RTCCommandsDLQTopic)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build command dlq publisher: %w", err)
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

	consumer := commandconsumer.New(
		kafkaReader,
		commandService,
		commandDLQPublisher,
		logger,
		commandconsumer.ConsumerOptions{
			MaxDispatchAttempts: cfg.RTCCommandMaxDispatchAttempts,
			RetryBackoff:        cfg.RTCCommandRetryBackoff,
			Now:                 time.Now,
		},
	)
	webhookHandler := livekitwebhook.New(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, commandService, logger)

	readinessChecks := []readinessCheck{
		{name: "scylla", check: roomStore.Ping},
		{name: "redis", check: grantStore.Ping},
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.POST("/livekit/webhook", gin.WrapH(webhookHandler))
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	router.GET("/readyz", func(c *gin.Context) {
		readyCtx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
		defer cancel()

		if err := runReadinessChecks(readyCtx, readinessChecks); err != nil {
			logger.Warn("readiness check failed", slog.String("error", err.Error()))
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "error": err.Error()})

			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	grpcServer := grpc.NewServer()
	rtcv1.RegisterRTCQueryServiceServer(grpcServer, grpcQueryServer)

	grpcHealthServer := grpcHealth.NewServer()
	grpcHealthV1.RegisterHealthServer(grpcServer, grpcHealthServer)
	grpcHealthServer.SetServingStatus("", grpcHealthV1.HealthCheckResponse_SERVING)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("listen grpc on %s: %w", cfg.GRPCAddr, err)
	}

	return &Container{
		httpServer:         httpServer,
		grpcServer:         grpcServer,
		grpcListener:       grpcListener,
		commandConsumer:    consumer,
		outboxRelay:        outboxRelay,
		outboxPollInterval: cfg.OutboxPollInterval,
		readinessChecks:    readinessChecks,
		closers:            closers,
		logger:             logger,
	}, nil
}
