package domain

import "errors"

var (
	ErrInvalidIdentifier       = errors.New("invalid identifier")
	ErrInvalidName             = errors.New("invalid name")
	ErrInvalidTimestamp        = errors.New("invalid timestamp")
	ErrWorkspaceNotFound       = errors.New("workspace not found")
	ErrChannelNotFound         = errors.New("channel not found")
	ErrMemberNotFound          = errors.New("member not found")
	ErrMemberBanned            = errors.New("member banned")
	ErrPermissionDenied        = errors.New("permission denied")
	ErrCommandAlreadyProcessed = errors.New("command already processed")
)
