package livekitwebhook

import (
	"context"
	"errors"
	"net/http"
	"time"
)

func RunHealthProbe(ctx context.Context, endpoint string, timeout time.Duration) error {
	client := http.Client{Timeout: timeout}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return err
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}

	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode >= http.StatusBadRequest {
		return errors.New("health probe request failed")
	}

	return nil
}
