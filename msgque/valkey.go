package msgque

import (
	"context"
	"errors"
)

var valki valky

/* Minimal interface for interval state. Caller inject implementation via Init */
type valky interface {
	GetMap(ctx context.Context, key string) (map[string]string, error)
	SetMap(ctx context.Context, key string, fields map[string]any) error
	SetField(ctx context.Context, key, field string, value any) error
	Delete(ctx context.Context, key string) error
	Tx(ctx context.Context, fn func(tx valky) error, watch ...string) error
}

/* TODO: replace with your not-found error */
var ErrNotFound = errors.New("kvstorage: key not found")
