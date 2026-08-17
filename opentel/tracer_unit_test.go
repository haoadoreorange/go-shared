//go:build unit

package opentel

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	noopm "go.opentelemetry.io/otel/metric/noop"
	noopt "go.opentelemetry.io/otel/trace/noop"
)

/*
 * Return the same *tracer for the same name (write-once, lock-free reads)
 */
func TestCacheTracer_Dedup(t *testing.T) {
	t.Parallel()

	tracers := &sync.Map{}
	tp := noopt.NewTracerProvider()
	mp := noopm.NewMeterProvider()

	a := cacheTracer("x", tp, mp, tracers)
	b := cacheTracer("x", tp, mp, tracers)
	c := cacheTracer("y", tp, mp, tracers)

	assert.Same(t, a, b)
	assert.NotSame(t, a, c)
}
