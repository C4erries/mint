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

type closerStack struct {
	closers []io.Closer
}

func newCloserStack(capacity int) *closerStack {
	return &closerStack{closers: make([]io.Closer, 0, capacity)}
}

func (s *closerStack) Add(closer io.Closer) {
	if closer == nil {
		return
	}

	s.closers = append(s.closers, closer)
}

func (s *closerStack) Cleanup() {
	for index := len(s.closers) - 1; index >= 0; index-- {
		_ = s.closers[index].Close()
	}
}

func (s *closerStack) Items() []io.Closer {
	return s.closers
}

type workspaceComponents struct {
	store             *workspacescylla.Store
	commandService    *workspaceapp.CommandService
	queryService      *workspaceapp.QueryService
	permissionService *workspaceapp.PermissionService
}

func NewContainer(cfg config.Config, logger *slog.Logger) (_ *Container, err error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	closers := newCloserStack(8)

	defer func() {
		if err != nil {
			closers.Cleanup()
		}
	}()

	identityService, identityStore, revocationStore, err := buildIdentityService(cfg, closers)
	if err != nil {
		return nil, err
	}

	workspaceDeps, err := buildWorkspaceComponents(cfg, closers)
	if err != nil {
		return nil, err
	}

	kafkaProducer, err := buildKafkaProducer(cfg, closers)
	if err != nil {
		return nil, err
	}

	outboxRelay, workspaceConsumer, err := buildWorkspaceRuntime(cfg, logger, workspaceDeps, kafkaProducer, closers)
	if err != nil {
		return nil, err
	}

	rtcQueryClient, err := buildRTCQueryClient(cfg, closers)
	if err != nil {
		return nil, err
	}

	httpServer := buildHTTPServer(cfg, identityService, workspaceDeps.queryService, kafkaProducer, rtcQueryClient, identityStore, workspaceDeps.store, revocationStore)

	grpcServer, grpcListener, err := buildGRPCServer(cfg, workspaceDeps.permissionService)
	if err != nil {
		return nil, err
	}

	return &Container{
		httpServer:         httpServer,
		grpcServer:         grpcServer,
		grpcListener:       grpcListener,
		workspaceConsumer:  workspaceConsumer,
		outboxRelay:        outboxRelay,
		outboxPollInterval: cfg.OutboxPollInterval,
		readinessChecks: []readinessCheck{
			{name: "identity_scylla", check: identityStore.Ping},
			{name: "workspace_scylla", check: workspaceDeps.store.Ping},
			{name: "redis", check: revocationStore.Ping},
		},
		closers: closers.Items(),
		logger:  logger,
	}, nil
}

func buildIdentityService(
	cfg config.Config,
	closers *closerStack,
) (*identityapp.Service, *identityscylla.Store, *redisrepo.RevocationStore, error) {
	identityStore, err := identityscylla.NewStore(identityscylla.Options{
		Hosts:            cfg.ScyllaHosts,
		Port:             cfg.ScyllaPort,
		Keyspace:         cfg.ScyllaKeyspace,
		Consistency:      cfg.ScyllaConsistency,
		AutoCreateSchema: cfg.ScyllaAutoCreateSchema,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build identity scylla store: %w", err)
	}

	closers.Add(identityStore)

	revocationStore, err := redisrepo.NewRevocationStore(redisrepo.Options{
		Addr:      cfg.RedisAddr,
		Password:  cfg.RedisPassword,
		DB:        cfg.RedisDB,
		KeyPrefix: cfg.RedisKeyPrefix,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build identity redis revocation store: %w", err)
	}

	closers.Add(revocationStore)

	jwtTokenManager, err := jwtmanager.NewManager(cfg.JWTAccessSecret, cfg.JWTRefreshSecret, id.New)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build jwt manager: %w", err)
	}

	hasher := passwordhasher.NewBcryptHasher(0)

	identityService, err := identityapp.NewService(identityStore, identityStore, revocationStore, hasher, jwtTokenManager, identityapp.ServiceOptions{
		IDGenerator:     id.New,
		Now:             time.Now,
		AccessTokenTTL:  cfg.JWTAccessTTL,
		RefreshTokenTTL: cfg.JWTRefreshTTL,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build identity service: %w", err)
	}

	return identityService, identityStore, revocationStore, nil
}

func buildWorkspaceComponents(cfg config.Config, closers *closerStack) (workspaceComponents, error) {
	workspaceStore, err := workspacescylla.NewStore(workspacescylla.Options{
		Hosts:            cfg.ScyllaHosts,
		Port:             cfg.ScyllaPort,
		Keyspace:         cfg.ScyllaKeyspace,
		Consistency:      cfg.ScyllaConsistency,
		AutoCreateSchema: cfg.ScyllaAutoCreateSchema,
		Now:              time.Now,
	})
	if err != nil {
		return workspaceComponents{}, fmt.Errorf("build workspace scylla store: %w", err)
	}

	closers.Add(workspaceStore)

	workspaceCommandService, err := workspaceapp.NewCommandService(workspaceStore, workspaceapp.CommandServiceOptions{
		IDGenerator: id.New,
		Now:         time.Now,
	})
	if err != nil {
		return workspaceComponents{}, fmt.Errorf("build workspace command service: %w", err)
	}

	workspaceQueryService := workspaceapp.NewQueryService(workspaceStore)

	permissionService, err := workspaceapp.NewPermissionService(workspaceStore, workspaceapp.NewBaselinePermissionEvaluator())
	if err != nil {
		return workspaceComponents{}, fmt.Errorf("build workspace permission service: %w", err)
	}

	return workspaceComponents{
		store:             workspaceStore,
		commandService:    workspaceCommandService,
		queryService:      workspaceQueryService,
		permissionService: permissionService,
	}, nil
}

func buildKafkaProducer(cfg config.Config, closers *closerStack) (*kafkaproducer.Producer, error) {
	kafkaProducer, err := kafkaproducer.New(kafkaproducer.Config{Brokers: cfg.KafkaBrokers}, nil)
	if err != nil {
		return nil, fmt.Errorf("build kafka producer: %w", err)
	}

	closers.Add(kafkaProducer)

	return kafkaProducer, nil
}

func buildWorkspaceRuntime(
	cfg config.Config,
	logger *slog.Logger,
	workspaceDeps workspaceComponents,
	kafkaProducer *kafkaproducer.Producer,
	closers *closerStack,
) (*workspacepublisher.OutboxRelay, *workspaceconsumer.Consumer, error) {
	workspaceOutboxPublisher := workspacepublisher.NewPublisher(kafkaProducer, cfg.WorkspaceEventsTopic)

	outboxRelay, err := workspacepublisher.NewOutboxRelay(workspaceDeps.store, workspaceOutboxPublisher, logger, cfg.OutboxBatchSize)
	if err != nil {
		return nil, nil, fmt.Errorf("build workspace outbox relay: %w", err)
	}

	commandDLQPublisher, err := workspacepublisher.NewCommandDLQPublisher(kafkaProducer, cfg.WorkspaceCommandsDLQ)
	if err != nil {
		return nil, nil, fmt.Errorf("build workspace command dlq publisher: %w", err)
	}

	workspaceReader, err := workspaceconsumer.NewKafkaReader(workspaceconsumer.KafkaReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.WorkspaceCommandsTopic,
		GroupID:        cfg.WorkspaceConsumerGroup,
		CommitInterval: 1 * time.Second,
	}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build workspace kafka reader: %w", err)
	}

	closers.Add(workspaceReader)

	consumer := workspaceconsumer.New(
		workspaceReader,
		workspaceDeps.commandService,
		commandDLQPublisher,
		logger,
		workspaceconsumer.ConsumerOptions{
			MaxDispatchAttempts: cfg.WorkspaceCommandMaxAttempts,
			RetryBackoff:        cfg.WorkspaceCommandRetryBackoff,
			Now:                 time.Now,
		},
	)

	return outboxRelay, consumer, nil
}

func buildRTCQueryClient(cfg config.Config, closers *closerStack) (*rtcqueryclient.Client, error) {
	rtcQueryClient, err := rtcqueryclient.NewClient(rtcqueryclient.Options{
		Address:      cfg.RTCGRPCAddr,
		Timeout:      cfg.RTCGRPCTimeout,
		MaxRetries:   cfg.RTCGRPCMaxRetries,
		RetryBackoff: cfg.RTCGRPCRetryBackoff,
	})
	if err != nil {
		return nil, fmt.Errorf("build rtc gRPC query client: %w", err)
	}

	closers.Add(rtcQueryClient)

	return rtcQueryClient, nil
}

func buildHTTPServer(
	cfg config.Config,
	identityService *identityapp.Service,
	workspaceQueryService *workspaceapp.QueryService,
	kafkaProducer *kafkaproducer.Producer,
	rtcQueryClient *rtcqueryclient.Client,
	identityStore *identityscylla.Store,
	workspaceStore *workspacescylla.Store,
	revocationStore *redisrepo.RevocationStore,
) *http.Server {
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
		TrustedProxies:   cfg.TrustedProxies,
		ReadinessCheck: func(ctx context.Context) error {
			readyCtx, cancel := context.WithTimeout(ctx, readinessTimeout)
			defer cancel()

			return runReadinessChecks(readyCtx, readinessChecks)
		},
	})

	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}

func buildGRPCServer(cfg config.Config, permissionService *workspaceapp.PermissionService) (*grpc.Server, net.Listener, error) {
	grpcServer := grpc.NewServer()
	permissionGRPCServer := permissionserver.New(permissionService)
	permissionv1.RegisterPermissionServiceServer(grpcServer, permissionGRPCServer)

	grpcHealthServer := grpcHealth.NewServer()
	grpcHealthV1.RegisterHealthServer(grpcServer, grpcHealthServer)
	grpcHealthServer.SetServingStatus("", grpcHealthV1.HealthCheckResponse_SERVING)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen grpc on %s: %w", cfg.GRPCAddr, err)
	}

	return grpcServer, grpcListener, nil
}
