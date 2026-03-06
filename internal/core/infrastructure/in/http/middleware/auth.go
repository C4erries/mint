package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	identityapp "github.com/c4erries/mint/internal/identity/application"
)

const (
	ContextAccountIDKey = "account_id"
	ContextSessionIDKey = "session_id"
	ContextTokenIDKey   = "token_id"
	ContextTokenKey     = "token_claims"
)

// AccessTokenParser validates and decodes access JWT.
type AccessTokenParser interface {
	ParseAccessToken(rawToken string) (identityapp.TokenClaims, error)
}

func RequireAuth(parser AccessTokenParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		authorization := strings.TrimSpace(c.GetHeader("Authorization"))
		if !strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}

		rawToken := strings.TrimSpace(authorization[len("Bearer "):])

		claims, err := parser.ParseAccessToken(rawToken)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		c.Set(ContextAccountIDKey, claims.AccountID)
		c.Set(ContextSessionIDKey, claims.SessionID)
		c.Set(ContextTokenIDKey, claims.TokenID)
		c.Set(ContextTokenKey, claims)
		c.Next()
	}
}

func MustAccountID(c *gin.Context) string {
	value, _ := c.Get(ContextAccountIDKey)
	accountID, _ := value.(string)

	return accountID
}

func MustSessionID(c *gin.Context) string {
	value, _ := c.Get(ContextSessionIDKey)
	sessionID, _ := value.(string)

	return sessionID
}

func MustTokenID(c *gin.Context) string {
	value, _ := c.Get(ContextTokenIDKey)
	tokenID, _ := value.(string)

	return tokenID
}

func MustClaims(c *gin.Context) identityapp.TokenClaims {
	value, _ := c.Get(ContextTokenKey)
	claims, _ := value.(identityapp.TokenClaims)

	return claims
}
