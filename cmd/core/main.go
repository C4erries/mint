package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/c4erries/mint/internal/core/infrastructure/config"
	"github.com/c4erries/mint/internal/core/infrastructure/di"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		panic(fmt.Errorf("load core config: %w", err))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting core",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("grpc_addr", cfg.GRPCAddr),
	)

	container, err := di.NewContainer(cfg, logger)
	if err != nil {
		panic(fmt.Errorf("build core container: %w", err))
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runErrCh := make(chan error, 1)

	go func() {
		runErrCh <- container.Run(runCtx)
	}()

	var runErr error
	select {
	case <-runCtx.Done():
		logger.Info("shutdown signal received")
	case runErr = <-runErrCh:
		if runErr != nil {
			logger.Error("core stopped with error", slog.String("error", runErr.Error()))
			stop()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()

	if err = container.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown core", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if runErr != nil {
		os.Exit(1)
	}

	logger.Info("core stopped")
}
