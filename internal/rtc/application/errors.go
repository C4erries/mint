package application

import "errors"

var (
	ErrPermissionDenied = errors.New("rtc application: permission denied")
	ErrInvalidCommand   = errors.New("rtc application: invalid command")
	ErrInvalidQuery     = errors.New("rtc application: invalid query")
)
