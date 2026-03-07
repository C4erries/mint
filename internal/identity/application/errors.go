package application

import "errors"

var (
	ErrInvalidCommand      = errors.New("invalid command")
	ErrInvalidCredentials  = errors.New("invalid credentials")
	ErrAccountAlreadyExist = errors.New("account already exists")
	ErrTokenInvalid        = errors.New("token is invalid")
	ErrTokenExpired        = errors.New("token is expired")
	ErrUnauthorized        = errors.New("unauthorized")
)
