//go:build unit && !mct

package msgque

import (
	"context"
	"os"
	"testing"

	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	zlog.Suppress()
	opentel.Init(context.Background())
	os.Exit(m.Run())
}

/*
 * Mutate package-level inited+once — cannot parallel
 */
func TestInit(t *testing.T) {
	ctx := context.Background()
	inited = false
	assert.Panics(t, func() { Pub(ctx, "", nil) })
	assert.Panics(t, func() { Rpc(ctx, "", nil) })
	assert.Panics(t, func() { Sub(ctx, "", "", nil) })
	assert.Panics(t, func() { Interval(ctx, "", nil, "", 0) })
	assert.Panics(t, func() { CancelInterval(ctx, "") })
	assert.Panics(t, func() { StatusInterval(ctx, "") })
	Init(ctx, nil)
	Init(ctx, nil) // second call noop
	assert.NotPanics(t, func() { Pub(ctx, "", nil) })
	assert.NotPanics(t, func() { _, _ = Rpc(ctx, "", nil) })
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	assert.NotPanics(t, func() { _ = Sub(canceled, "", "", nil) })
	assert.NotPanics(t, func() { Interval(ctx, "", nil, "", 0) })
}
