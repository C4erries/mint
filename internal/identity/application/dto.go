package application

import "time"

type RegisterCommand struct {
	Email     string `json:"email"`
	Password  string `json:"password"`
	UserAgent string `json:"user_agent"`
	IP        string `json:"ip"`
}

type LoginCommand struct {
	Email     string `json:"email"`
	Password  string `json:"password"`
	UserAgent string `json:"user_agent"`
	IP        string `json:"ip"`
}

type RefreshCommand struct {
	RefreshToken string `json:"refresh_token"`
}

type LogoutCommand struct {
	AccessClaims TokenClaims
	RefreshToken string
}

type AuthResult struct {
	AccountID            string    `json:"account_id"`
	Email                string    `json:"email"`
	SessionID            string    `json:"session_id"`
	AccessToken          string    `json:"access_token"`
	RefreshToken         string    `json:"refresh_token"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
	RefreshTokenExpireAt time.Time `json:"refresh_token_expires_at"`
}

type MeResult struct {
	AccountID string `json:"account_id"`
	Email     string `json:"email"`
}
