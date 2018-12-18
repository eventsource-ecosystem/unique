package unique

import (
	"context"

	"github.com/eventsource-ecosystem/eventsource"
)

// Repository represents a function to execute a command that returns the version number
// of the event after the command was applied
type Repository interface {
	Apply(ctx context.Context, command eventsource.Command) (int, error)
}

// RepositoryFunc provides a func convenience wrapper for Repository
type RepositoryFunc func(ctx context.Context, command eventsource.Command) (int, error)

// Apply satisfies the Repository interface
func (fn RepositoryFunc) Apply(ctx context.Context, command eventsource.Command) (int, error) {
	return fn(ctx, command)
}
