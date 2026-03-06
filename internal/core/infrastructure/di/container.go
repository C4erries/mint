package di

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"

	permissionv1 "github.com/c4erries/mint/api/permission/v1"
	"github.com/c4erries/mint/internal/core/infrastructure/config"
	permissionserver "github.com/c4erries/mint/internal/core/infrastructure/in/grpc/permission_server"
	authhandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/auth"
	rtchandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/rtc"
	workspacehandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/workspace"
	httprouter "github.com/c4erries/mint/internal/core/infrastructure/in/http/router"
	rtcqueryclient "github.com/c4erries/mint/internal/core/infrastructure/out/grpc/clients/rtcquery"
	kafkaproducer "github.com/c4erries/mint/internal/core/infrastructure/out/kafka/producer"
	identityapp "github.com/c4erries/mint/internal/identity/application"
	redisrepo "github.com/c4erries/mint/internal/identity/infrastructure/out/repository/redis"
	identityscylla "github.com/c4erries/mint/internal/identity/infrastructure/out/repository/scylla"
	jwtmanager "github.com/c4erries/mint/internal/identity/infrastructure/out/security/jwt"
	passwordhasher "github.com/c4erries/mint/internal/identity/infrastructure/out/security/password"
	workspaceapp "github.com/c4erries/mint/internal/workspace/application"
	workspaceconsumer "github.com/c4erries/mint/internal/workspace/infrastructure/in/kafka/command_consumer"
	workspacepublisher "github.com/c4erries/mint/internal/workspace/infrastructure/out/kafka/event_publisher"
	workspacescylla "github.com/c4erries/mint/internal/workspace/infrastructure/out/repository/scylla"
	"github.com/c4erries/mint/pkg/id"
)

const readinessTimeout = 2 * time.Second

type readinessCheck struct {
	name  string
	check func(ctx context.Context) error
}

// Container assembles core dependency graph and controls runtime lifecycle.
type Container struct {
	httpServer        *http.Server
	grpcServer        *grpc.Server
	grpcListener      net.Listener
	workspaceConsumer *workspaceconsumer.Consumer
	outboxRelay       *workspacepublisher.OutboxRelay

	outboxPollInterval time.Duration
	readinessChecks    []readinessCheck
	closers            []io.Closer
	logger             *slog.Logger
}

func NewContainer(cfg config.Config, logger *slog.Logger) (*Container, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	closers := make([]io.Closer, 0, 8)
	cleanup := func() {
		for index := len(closers) - 1; index >= 0; index-- {
			_ = closers[index].Close()
		}
	}

	identityStore, err := identityscylla.NewStore(identityscylla.Options{
		Hosts:            cfg.ScyllaHosts,
		Port:             cfg.ScyllaPort,
		Keyspace:         cfg.ScyllaKeyspace,
		Consistency:      cfg.ScyllaConsistency,
		AutoCreateSchema: cfg.ScyllaAutoCreateSchema,
	})
	if err != nil {
		return nil, fmt.Errorf("build identity scylla store: %w", err)
	}
	closers = append(closers, identityStore)

	revocationStore, err := redisrepo.NewRevocationStore(redisrepo.Options{
		Addr:      cfg.RedisAddr,
		Password:  cfg.RedisPassword,
		DB:        cfg.RedisDB,
		KeyPrefix: cfg.RedisKeyPrefix,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build identity redis revocation store: %w", err)
	}
	closers = append(closers, revocationStore)

	jwtTokenManager, err := jwtmanager.NewManager(cfg.JWTAccessSecret, cfg.JWTRefreshSecret, id.New)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build jwt manager: %w", err)
	}

	hasher := passwordhasher.NewBcryptHasher(0)
	identityService, err := identityapp.NewService(identityStore, identityStore, revocationStore, hasher, jwtTokenManager, identityapp.ServiceOptions{
		IDGenerator:     id.New,
		Now:             time.Now,
		AccessTokenTTL:  cfg.JWTAccessTTL,
		RefreshTokenTTL: cfg.JWTRefreshTTL,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build identity service: %w", err)
	}

	workspaceStore, err := workspacescylla.NewStore(workspacescylla.Options{
		Hosts:            cfg.ScyllaHosts,
		Port:             cfg.ScyllaPort,
		Keyspace:         cfg.ScyllaKeyspace,
		Consistency:      cfg.ScyllaConsistency,
		AutoCreateSchema: cfg.ScyllaAutoCreateSchema,
		Now:              time.Now,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build workspace scylla store: %w", err)
	}
	closers = append(closers, workspaceStore)

	workspaceCommandService, err := workspaceapp.NewCommandService(workspaceStore, workspaceapp.CommandServiceOptions{
		IDGenerator: id.New,
		Now:         time.Now,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build workspace command service: %w", err)
	}

	workspaceQueryService := workspaceapp.NewQueryService(workspaceStore)
	permissionService, err := workspaceapp.NewPermissionService(workspaceStore, workspaceapp.NewBaselinePermissionEvaluator())
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build workspace permission service: %w", err)
	}

	kafkaProducer, err := kafkaproducer.New(kafkaproducer.Config{Brokers: cfg.KafkaBrokers}, nil)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build kafka producer: %w", err)
	}
	closers = append(closers, kafkaProducer)

	workspaceOutboxPublisher := workspacepublisher.NewPublisher(kafkaProducer, cfg.WorkspaceEventsTopic)
	outboxRelay, err := workspacepublisher.NewOutboxRelay(workspaceStore, workspaceOutboxPublisher, logger, cfg.OutboxBatchSize)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build workspace outbox relay: %w", err)
	}

	workspaceReader, err := workspaceconsumer.NewKafkaReader(workspaceconsumer.KafkaReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.WorkspaceCommandsTopic,
		GroupID:        cfg.WorkspaceConsumerGroup,
		CommitInterval: 1 * time.Second,
	}, nil)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build workspace kafka reader: %w", err)
	}
	closers = append(closers, workspaceReader)

	consumer := workspaceconsumer.New(workspaceReader, workspaceCommandService, logger)

	rtcQueryClient, err := rtcqueryclient.NewClient(rtcqueryclient.Options{
		Address:      cfg.RTCGRPCAddr,
		Timeout:      cfg.RTCGRPCTimeout,
		MaxRetries:   cfg.RTCGRPCMaxRetries,
		RetryBackoff: cfg.RTCGRPCRetryBackoff,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build rtc gRPC query client: %w", err)
	}
	closers = append(closers, rtcQueryClient)

	authHTTPHandler := authhandler.NewHandler(identityService)
	workspaceHTTPHandler := workspacehandler.NewHandler(workspaceQueryService, kafkaProducer, cfg.WorkspaceCommandsTopic, id.New, time.Now)
	rtcHTTPHandler := rtchandler.NewHandler(kafkaProducer, rtcQueryClient, cfg.RTCCommandsTopic, id.New, time.Now)

	readinessChecks := []readinessCheck{
		{name: "identity_scylla", check: identityStore.Ping},
		{name: "workspace_scylla", check: workspaceStore.Ping},
		{name: "redis", check: revocationStore.Ping},
	}

	router := httprouter.New(httprouter.Dependencies{
		AuthHandler:      authHTTPHandler,
		WorkspaceHandler: workspaceHTTPHandler,
		RTCHandler:       rtcHTTPHandler,
		AuthParser:       identityService,
		ReadinessCheck: func(ctx context.Context) error {
			readyCtx, cancel := context.WithTimeout(ctx, readinessTimeout)
			defer cancel()
			return runReadinessChecks(readyCtx, readinessChecks)
		},
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
	permissionGRPCServer := permissionserver.New(permissionService)
	permissionv1.RegisterPermissionServiceServer(grpcServer, permissionGRPCServer)

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
		workspaceConsumer:  consumer,
		outboxRelay:        outboxRelay,
		outboxPollInterval: cfg.OutboxPollInterval,
		readinessChecks:    readinessChecks,
		closers:            closers,
		logger:             logger,
	}, nil
}
