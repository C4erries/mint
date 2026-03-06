package di

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"google.golang.org/grpc"
)

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
		if err := c.closers[index].Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}

func runReadinessChecks(ctx context.Context, checks []readinessCheck) error {
	var readinessErr error
	for _, check := range checks {
		if err := check.check(ctx); err != nil {
			readinessErr = errors.Join(readinessErr, fmt.Errorf("%s: %w", check.name, err))
		}
	}

	return readinessErr
}
