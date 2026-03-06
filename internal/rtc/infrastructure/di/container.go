package di

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

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
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

// Container assembles rtc-api dependency graph and controls runtime lifecycle.
type Container struct {
	httpServer      *http.Server
	grpcServer      *grpc.Server
	grpcListener    net.Listener
	commandConsumer *commandconsumer.Consumer
	outboxRelay     *eventpublisher.OutboxRelay

	outboxPollInterval time.Duration
	closers            []io.Closer
	logger             *slog.Logger
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
		closers:            closers,
		logger:             logger,
	}, nil
}

// Run starts all runtime loops and blocks until context cancellation or first fatal runtime error.
func (c *Container) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}

	errCh := make(chan error, 4)

	go func() {
		if runErr := c.outboxRelay.Run(ctx, c.outboxPollInterval); runErr != nil {
			errCh <- fmt.Errorf("run outbox relay: %w", runErr)
		}
	}()

	go func() {
		if runErr := c.commandConsumer.Run(ctx); runErr != nil {
			errCh <- fmt.Errorf("run command consumer: %w", runErr)
		}
	}()

	go func() {
		if runErr := c.httpServer.ListenAndServe(); runErr != nil && !errors.Is(runErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("run http server: %w", runErr)
		}
	}()

	go func() {
		if runErr := c.grpcServer.Serve(c.grpcListener); runErr != nil && !errors.Is(runErr, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("run grpc server: %w", runErr)
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case runErr := <-errCh:
		return runErr
	}
}

// Shutdown gracefully stops servers and closes infrastructure resources.
func (c *Container) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}

	var shutdownErr error

	if err := c.httpServer.Shutdown(ctx); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown http server: %w", err))
	}

	grpcStopped := make(chan struct{})
	go func() {
		c.grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	select {
	case <-grpcStopped:
	case <-ctx.Done():
		c.grpcServer.Stop()
	}

	if err := c.grpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close grpc listener: %w", err))
	}

	if err := c.Close(); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close infrastructure resources: %w", err))
	}

	return shutdownErr
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
