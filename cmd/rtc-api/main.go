package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/c4erries/mint/internal/rtc/infrastructure/config"
	"github.com/c4erries/mint/internal/rtc/infrastructure/di"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		panic(fmt.Errorf("load rtc-api config: %w", err))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting rtc-api",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("grpc_addr", cfg.GRPCAddr),
	)

	container, err := di.NewContainer(cfg, logger)
	if err != nil {
		panic(fmt.Errorf("build rtc-api container: %w", err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 4)

	go func() {
		if runErr := container.OutboxRelay.Run(ctx, cfg.OutboxPollInterval); runErr != nil {
			errCh <- fmt.Errorf("run outbox relay: %w", runErr)
		}
	}()

	go func() {
		if runErr := container.CommandConsumer.Run(ctx); runErr != nil {
			errCh <- fmt.Errorf("run command consumer: %w", runErr)
		}
	}()

	go func() {
		if runErr := container.HTTPServer.ListenAndServe(); runErr != nil && !errors.Is(runErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("run http server: %w", runErr)
		}
	}()

	go func() {
		if runErr := container.GRPCServer.Serve(container.GRPCListener); runErr != nil && !errors.Is(runErr, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("run grpc server: %w", runErr)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case runErr := <-errCh:
		logger.Error("rtc-api stopped with error", slog.String("error", runErr.Error()))
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()

	if err = container.HTTPServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown http server", slog.String("error", err.Error()))
		os.Exit(1)
	}

	grpcStopped := make(chan struct{})
	go func() {
		container.GRPCServer.GracefulStop()
		close(grpcStopped)
	}()

	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		container.GRPCServer.Stop()
	}

	if err = container.GRPCListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		logger.Error("failed to close grpc listener", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err = container.Close(); err != nil {
		logger.Error("failed to close infrastructure resources", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("rtc-api stopped")
}
