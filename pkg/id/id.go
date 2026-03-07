package id

import "github.com/google/uuid"

// New returns RFC 4122 UUID string.
func New() string {
	return uuid.NewString()
}
