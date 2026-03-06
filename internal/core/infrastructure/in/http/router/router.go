package router

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	authhandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/auth"
	rtchandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/rtc"
	workspacehandler "github.com/c4erries/mint/internal/core/infrastructure/in/http/handlers/workspace"
	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
)

const readinessTimeout = 2 * time.Second

// ReadinessCheck checks external dependencies.
type ReadinessCheck func(ctx context.Context) error

type Dependencies struct {
	AuthHandler      *authhandler.Handler
	WorkspaceHandler *workspacehandler.Handler
	RTCHandler       *rtchandler.Handler
	AuthParser       middleware.AccessTokenParser
	TrustedProxies   []string
	ReadinessCheck   ReadinessCheck
}

func New(deps Dependencies) *gin.Engine {
	requiresAuth := deps.AuthHandler != nil || deps.WorkspaceHandler != nil || deps.RTCHandler != nil
	if requiresAuth && deps.AuthParser == nil {
		panic("core http router: auth parser is required for protected routes")
	}

	router := gin.New()
	if err := router.SetTrustedProxies(deps.TrustedProxies); err != nil {
		panic(fmt.Sprintf("core http router: invalid trusted proxies config: %v", err))
	}

	router.Use(gin.Recovery())

	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	router.GET("/readyz", func(c *gin.Context) {
		if deps.ReadinessCheck == nil {
			c.JSON(http.StatusOK, gin.H{"status": "ready"})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
		defer cancel()

		if err := deps.ReadinessCheck(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	v1 := router.Group("/api/v1")
	if deps.AuthHandler != nil {
		authPublic := v1.Group("/auth")
		deps.AuthHandler.RegisterPublicRoutes(authPublic)
	}

	protected := v1.Group("")
	if requiresAuth {
		protected.Use(middleware.RequireAuth(deps.AuthParser))
	}

	if deps.AuthHandler != nil {
		authProtected := protected.Group("/auth")
		deps.AuthHandler.RegisterProtectedRoutes(authProtected)
	}

	if deps.WorkspaceHandler != nil {
		deps.WorkspaceHandler.RegisterRoutes(protected)
	}

	if deps.RTCHandler != nil {
		deps.RTCHandler.RegisterRoutes(protected)
	}

	return router
}
