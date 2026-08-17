package opentel

import (
	"context"
	"sync"

	"github.com/haoadoreorange/go-shared/zlog"

	grpcm "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	grpct "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	interfacem "go.opentelemetry.io/otel/metric"
	noopm "go.opentelemetry.io/otel/metric/noop"
	m "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	t "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	interfacet "go.opentelemetry.io/otel/trace"
)

/*
 * tracer is a dual-role type: the same struct act as both provider (root) and
 * scoped tracer (sub). The root owns the SDK TracerProvider + MeterProvider
 * and is keyed by service name (e.g. "mentatbe"). Subs share the same tp/mp
 * via pointer but scope spans and instruments to their own name (e.g.
 * "catalog", "storage"). OTel uses this name to attribute telemetry to the
 * component that emitted it.
 *
 * The tracers sync.Map is shared across the tree — root and all subs point to
 * the same map, so Tracer("x") dedup works regardless of which node you call
 * it on.
 *
 * Instrument caching (counters, histograms, gauges) follow OTel's "create
 * once, reuse many" guidance. sync.Map is ideal: write once per name,
 * lock-free reads after
 */
type tracer struct {
	name string
	tp   interfacet.TracerProvider
	mp   interfacem.MeterProvider
	// cache
	tracers    *sync.Map // shared across root + subs → dedup by name → *tracer
	counters   sync.Map  // name → Float64Counter
	histograms sync.Map  // name → Float64Histogram
	gauges     sync.Map  // name → Float64Gauge
}

/*
Create a root tracer backed by gRPC exporters. Spawn a goroutine that
flush+shutdown on ctx cancellation. Return nil on any setup failure
*/
func newProvider(ctx context.Context, addr, service string) *tracer {
	grpctrace, err := grpct.New(ctx, grpct.WithInsecure(), grpct.WithEndpoint(addr))
	if err != nil {
		zlog.Info().Err(err).Msgf("opentel.newProvider(%v, %v): fail create grpc trace", addr, service)
		return nil
	}
	grpcmetric, err := grpcm.New(ctx, grpcm.WithInsecure(), grpcm.WithEndpoint(addr))
	if err != nil {
		zlog.Info().Err(err).Msgf("opentel.newProvider(%v, %v): fail create grpc metric", addr, service)
		return nil
	}
	r, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)))
	if err != nil {
		zlog.Info().Err(err).Msgf("opentel.newProvider(%v): fail create resource", service)
		return nil
	}

	tp := t.NewTracerProvider(t.WithBatcher(grpctrace), t.WithResource(r))
	mp := m.NewMeterProvider(m.WithReader(m.NewPeriodicReader(grpcmetric)), m.WithResource(r))
	go func() {
		<-ctx.Done()
		if err := tp.Shutdown(context.Background()); err != nil {
			zlog.Info().Err(err).Msgf("opentel: shutdown(%v) fail flush traces", service)
		}
		if err := mp.Shutdown(context.Background()); err != nil {
			zlog.Info().Err(err).Msgf("opentel: shutdown(%v) fail flush metrics", service)
		}
	}()
	return cacheTracer(service, tp, mp, &sync.Map{})
}

/* Dedup constructor — LoadOrStore ensure one *tracer per name in the shared map */
func cacheTracer(name string, tp interfacet.TracerProvider, mp interfacem.MeterProvider, tracers *sync.Map) *tracer {
	c, ok := tracers.Load(name)
	if !ok {
		c, _ = tracers.LoadOrStore(name, &tracer{name: name, tp: tp, mp: mp, tracers: tracers})
	}
	return c.(*tracer)
}

/*
 * Instrument cache — lazily create on first use, return noop on failure.
 * Keyed by instrument name in per-tracer sync.Map
 */

func (t *tracer) counter(cname string) interfacem.Float64Counter {
	c, ok := t.counters.Load(cname)
	if !ok {
		m, err := t.mp.Meter(t.name).Float64Counter(cname)
		if err != nil {
			zlog.Info().Err(err).Msgf("opentel.counter(%v, %v): fail, returning noop", t.name, cname)
			return noopm.Float64Counter{}
		}
		c, _ = t.counters.LoadOrStore(cname, m)
	}
	return c.(interfacem.Float64Counter)
}

func (t *tracer) histogram(hname string) interfacem.Float64Histogram {
	h, ok := t.histograms.Load(hname)
	if !ok {
		m, err := t.mp.Meter(t.name).Float64Histogram(hname)
		if err != nil {
			zlog.Info().Err(err).Msgf("opentel.histogram(%v, %v): fail, returning noop", t.name, hname)
			return noopm.Float64Histogram{}
		}
		h, _ = t.histograms.LoadOrStore(hname, m)
	}
	return h.(interfacem.Float64Histogram)
}

func (t *tracer) gauge(gname string) interfacem.Float64Gauge {
	g, ok := t.gauges.Load(gname)
	if !ok {
		m, err := t.mp.Meter(t.name).Float64Gauge(gname)
		if err != nil {
			zlog.Info().Err(err).Msgf("opentel.gauge(%v, %v): fail, returning noop", t.name, gname)
			return noopm.Float64Gauge{}
		}
		g, _ = t.gauges.LoadOrStore(gname, m)
	}
	return g.(interfacem.Float64Gauge)
}
