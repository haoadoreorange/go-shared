//go:build unit

package rabbitmq

import (
	"context"
	"os"
	"testing"

	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
)

var ctx = context.Background()

func TestMain(m *testing.M) {
	zlog.Suppress()
	opentel.Init(ctx)
	os.Exit(m.Run())
}

/*
 * Mutate package-level inited+once — cannot parallel
 */
func TestInit(t *testing.T) {
	assert.Panics(t, func() { Publish(ctx, "", "", nil, false) })
	assert.Panics(t, func() { Consume(ctx, "", 0) })
	assert.Panics(t, func() { Route("", "") })
	Init(ctx)
	Init(ctx) // second call noop
	assert.NotPanics(t, func() { Publish(ctx, "", "", nil, false) })
	assert.NotPanics(t, func() { Consume(ctx, "", 0) })
	assert.NotPanics(t, func() { Route("", "") })
}

func Test_cache_EmptyAddr(t *testing.T) {
	t.Parallel()
	r := _cache(ctx, "a", "", 10)
	assert.NotNil(t, r)
}

func Test_cache_InvalidAddr(t *testing.T) {
	t.Parallel()
	r := _cache(ctx, "b", "localhost:0", 10)
	assert.NotNil(t, r)
}

func Test_cache_Dedup(t *testing.T) {
	t.Parallel()
	a := _cache(ctx, "c", "", 10)
	b := _cache(ctx, "c", "", 10)
	assert.Same(t, a, b)
}
