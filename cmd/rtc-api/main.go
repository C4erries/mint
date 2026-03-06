package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/c4erries/mint/internal/rtc/infrastructure/config"
	"github.com/c4erries/mint/internal/rtc/infrastructure/di"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		panic(fmt.Errorf("load rtc-api config: %w", err))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting rtc-api", slog.String("http_addr", cfg.HTTPAddr))

	container, err := di.NewContainer(cfg, logger)
	if err != nil {
		panic(fmt.Errorf("build rtc-api container: %w", err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 3)

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

	logger.Info("rtc-api stopped")
}
