package domain

import "errors"

var (
	ErrInvalidIdentifier = errors.New("invalid identifier")
	ErrInvalidEmail      = errors.New("invalid email")
	ErrInvalidPassword   = errors.New("invalid password")
	ErrInvalidTimestamp  = errors.New("invalid timestamp")
	ErrAccountNotFound   = errors.New("account not found")
	ErrSessionNotFound   = errors.New("session not found")
	ErrSessionRevoked    = errors.New("session revoked")
)
