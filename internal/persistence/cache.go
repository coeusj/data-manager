package persistence

import "context"

type Cache interface {
	Save(ctx context.Context, id string, value []byte) error
	Delete(ctx context.Context, id string) error
	Close()
}
