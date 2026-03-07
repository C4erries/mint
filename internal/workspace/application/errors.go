package application

import "errors"

var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrInvalidQuery   = errors.New("invalid query")
)
