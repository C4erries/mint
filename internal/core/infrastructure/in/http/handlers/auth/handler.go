package auth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
	identityapp "github.com/c4erries/mint/internal/identity/application"
)

// Handler serves identity auth REST endpoints.
type Handler struct {
	service *identityapp.Service
}

func NewHandler(service *identityapp.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterPublicRoutes(group *gin.RouterGroup) {
	group.POST("/register", h.register)
	group.POST("/login", h.login)
	group.POST("/refresh", h.refresh)
}

func (h *Handler) RegisterProtectedRoutes(group *gin.RouterGroup) {
	group.POST("/logout", h.logout)
	group.GET("/me", h.me)
}

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type logoutRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (h *Handler) register(c *gin.Context) {
	var request registerRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	result, err := h.service.Register(c.Request.Context(), identityapp.RegisterCommand{
		Email:     request.Email,
		Password:  request.Password,
		UserAgent: c.Request.UserAgent(),
		IP:        clientIP(c),
	})
	if err != nil {
		h.writeAuthError(c, err)
		return
	}

	c.JSON(http.StatusCreated, result)
}

func (h *Handler) login(c *gin.Context) {
	var request loginRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	result, err := h.service.Login(c.Request.Context(), identityapp.LoginCommand{
		Email:     request.Email,
		Password:  request.Password,
		UserAgent: c.Request.UserAgent(),
		IP:        clientIP(c),
	})
	if err != nil {
		h.writeAuthError(c, err)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) refresh(c *gin.Context) {
	var request refreshRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	result, err := h.service.Refresh(c.Request.Context(), identityapp.RefreshCommand{RefreshToken: request.RefreshToken})
	if err != nil {
		h.writeAuthError(c, err)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) logout(c *gin.Context) {
	var request logoutRequest
	if err := c.ShouldBindJSON(&request); err != nil && !isBodyEmptyError(err) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if err := h.service.Logout(c.Request.Context(), identityapp.LogoutCommand{
		AccessClaims: middleware.MustClaims(c),
		RefreshToken: request.RefreshToken,
	}); err != nil {
		h.writeAuthError(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *Handler) me(c *gin.Context) {
	result, err := h.service.Me(c.Request.Context(), middleware.MustClaims(c))
	if err != nil {
		h.writeAuthError(c, err)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) writeAuthError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, identityapp.ErrInvalidCommand):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, identityapp.ErrInvalidCredentials),
		errors.Is(err, identityapp.ErrUnauthorized),
		errors.Is(err, identityapp.ErrTokenInvalid),
		errors.Is(err, identityapp.ErrTokenExpired):
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
	case errors.Is(err, identityapp.ErrAccountAlreadyExist):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	}
}

func clientIP(c *gin.Context) string {
	forwardedFor := strings.TrimSpace(c.GetHeader("X-Forwarded-For"))
	if forwardedFor != "" {
		parts := strings.Split(forwardedFor, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}

	return c.ClientIP()
}

func isBodyEmptyError(err error) bool {
	return strings.Contains(err.Error(), "EOF")
}
