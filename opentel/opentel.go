package opentel

/*
 * Public API layer for the OTel bridge — delegates to tracer.go machinery.
 *
 * Reads standard OTel env vars at package load time:
 *   OTEL_EXPORTER_OTLP_ENDPOINT — collector gRPC address (e.g. "collector:4317")
 *   OTEL_SERVICE_NAME           — identifies this service in the backend
 *
 * If OTEL_EXPORTER_OTLP_ENDPOINT is empty, all operations return usable
 * no-ops. Importing the package is always safe regardless of environment
 */

import (
	"context"
	"sync"

	"github.com/haoadoreorange/go-shared/cons"
	"github.com/haoadoreorange/go-shared/util"
	"github.com/haoadoreorange/go-shared/zlog"

	noopm "go.opentelemetry.io/otel/metric/noop"
	noopt "go.opentelemetry.io/otel/trace/noop"
)

var once sync.Once
var ctxi context.Context
var inited = false

/*
 * Gate all package-level calls — must be called once at startup
 */
func Init(ctx context.Context) {
	once.Do(func() { // memory barrier
		zlog.Init()
		cacheProvider(
			ctx,
			defaultId,
			defaultAddr,
			defaultId,
		)
		ctxi = ctx
		inited = true
	})
}

/*
 * Return a tracer scoped to custom instrumentation name
 */
func Tracer(name string) *tracer {
	if !inited {
		panic("opentel.Tracer(): missing Init()")
	}
	return cacheProvider(ctxi, defaultId, defaultAddr, defaultId).Tracer(name)
}

/*
 * Start otel, caller must End() to send the span; metric and log fire immediately
 *
 *   otel := tracer.Start(ctx, "op",
 *       opentel.Bag("user_id", "abc"),
 *       opentel.Attr("request_id", "r1"),
 *   )
 */
func Start(ctx context.Context, name string, kvs ...kv) *Otel {
	if !inited {
		panic("opentel.Start(): missing Init()")
	}
	return cacheProvider(ctxi, defaultId, defaultAddr, defaultId).Start(ctx, name, kvs...)
}

var defaultId = util.Getenv(cons.OTEL_SERVICE_NAME, cons.DEFAULT)
var defaultAddr = util.GetenvTrim(cons.OTEL_EXPORTER_OTLP_ENDPOINT)
var defaultProvider *tracer // optimization, default skip sync.Map
var providers sync.Map      // id → *tracer (provider)

/*
Cache-hit: return stored provider. Miss: try newProvider (real gRPC), fall back to
ephemeral noop if addr empty or connection fail. Noop is NOT stored — each call with
empty addr get a fresh instance, so subsequent calls retry newProvider and recover
automatically when the collector come back up
*/
func cacheProvider(ctx context.Context, id, addr, service string) *tracer {
	if id == defaultId && defaultProvider != nil { // check id first, faster for non defalt
		return defaultProvider
	}
	p, ok := providers.Load(id)
	if !ok {
		noop := func() *tracer {
			return cacheTracer(service, noopt.NewTracerProvider(), noopm.NewMeterProvider(), &sync.Map{})
		}
		if addr == "" {
			zlog.Info().Msgf("opentel.cacheProvider(%v): empty addr, returning noop", id)
			return noop()
		}
		t := newProvider(ctx, addr, service)
		if t == nil {
			zlog.Warn().Msgf("opentel.cacheProvider(%v): fail, returning noop", id)
			return noop()
		}
		p, _ = providers.LoadOrStore(id, t)
	}
	t := p.(*tracer)
	if id == defaultId {
		defaultProvider = t
	}
	return t
}
