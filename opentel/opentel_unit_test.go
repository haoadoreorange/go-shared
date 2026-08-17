//go:build unit

package opentel

import (
	"context"
	"os"
	"testing"

	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
 * Suppress package-level zlog output
 * Tests that assert on log content redirect to their own buffer
 */
func TestMain(m *testing.M) {
	zlog.Suppress()
	os.Exit(m.Run())
}

/*
 * Mutate package-level inited+providers — cannot parallel
 * defaultAddr is empty in the test env, so Init() take the noop path
 */
func TestInit(t *testing.T) {
	ctx := context.Background()
	inited = false
	assert.Panics(t, func() { Tracer("") })
	assert.Panics(t, func() { Start(ctx, "") })
	Init(ctx)
	Init(ctx) // second call noop
	assert.NotPanics(t, func() { Tracer("") })
	assert.NotPanics(t, func() { _ = Start(ctx, "") })
}

/*
 * Start(otel.Ctx(), "c") and otel.Start("c") produce identical parent-child linking
 * Cannot parallel — mutate defaultProvider and inited
 */
func TestStart_EquivalentToOtelStart(t *testing.T) {
	parent, getSpans, _ := Test("p")
	defaultProvider = parent.t
	inited = true

	viaPackage := Start(parent.Ctx(), "p")
	viaMethod := parent.Start("m")
	viaPackage.End()
	viaMethod.End()
	parent.End()

	spans := getSpans() // [0] viaPackage, [1] viaMethod, [2] parent (End order)
	require.Len(t, spans, 3)

	assert.Equal(t, spans[2].SpanContext.TraceID(), spans[0].SpanContext.TraceID())
	assert.Equal(t, spans[2].SpanContext.TraceID(), spans[1].SpanContext.TraceID())
	assert.Equal(t, spans[2].SpanContext.SpanID(), spans[0].Parent.SpanID())
	assert.Equal(t, spans[2].SpanContext.SpanID(), spans[1].Parent.SpanID())
}

/*
 * cacheProvider with empty addr return ephemeral noop — not stored in providers
 */
func TestCacheProvider_NoopNotCached(t *testing.T) {
	t.Parallel()

	a := cacheProvider(context.Background(), "x", "", "")
	b := cacheProvider(context.Background(), "x", "", "")

	assert.NotNil(t, a)
	assert.NotNil(t, b)
	assert.NotSame(t, a, b)
}
