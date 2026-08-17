package opentel

import (
	"context"

	"github.com/haoadoreorange/go-shared/zlog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	interfacet "go.opentelemetry.io/otel/trace"
)

/*
 * correlate trace (span), metric, and log context into one bundle
 * Caller get all three observability signals from a single Start() call:
 *   otel := tracer.Start(ctx, "op", kvs...)
 *   otel.Event("cache miss")  // span event
 *   otel.Info().Msg("done")   // log with trace_id, span_id, kvs
 *   otel.Count("requests", 1) // metric correlated via exemplar
 *   otel.End()                // finalize span (Ok if no Error was called)
 */
type Otel struct {
	ctx context.Context
	s   interfacet.Span
	ok  bool
	t   *tracer
	l   zlog.LazyLogger
}

/*
 * Return a tracer scoped to custom instrumentation name
 */
func (t *tracer) Tracer(name string) *tracer {
	return cacheTracer(name, t.tp, t.mp, t.tracers)
}

/*
 * Start otel, caller must End() to send the span; metric and log fire immediately
 *
 *   otel := tracer.Start(ctx, "op",
 *       opentel.Attr("request_id", "r1"),
 *       opentel.Bag("user_id", "abc"),
 *   )
 */
func (t *tracer) Start(ctx context.Context, spanName string, kvs ...kv) *Otel {
	ctx, s := t.tp.Tracer(t.name).Start(ctx, spanName)
	otel := (&Otel{
		ctx: ctx,
		s:   s,
		ok:  true,
		t:   t,
		l: logAkvs(
			s,
			zlog.With().Str("span", spanName).
				Str("trace_id", s.SpanContext().TraceID().String()).
				Str("span_id", s.SpanContext().SpanID().String()),
			_getBags(ctx)...,
		),
	})
	return otel.Kvs(kvs...)
}

func _getBags(ctx context.Context) []attribute.KeyValue {
	members := baggage.FromContext(ctx).Members()
	bags := make([]attribute.KeyValue, len(members))
	for i, m := range members {
		bags[i] = attribute.String(m.Key(), m.Value())
	}
	return bags
}

func (o *Otel) Ctx() context.Context { return o.ctx }

/*
 * Start a child from this otel context, caller must End() to send the span; metric and log fire immediately
 *
 *   child := otel.Start(ctx, "op",
 *       opentel.Attr("request_id", "r1"),
 *       opentel.Bag("user_id", "abc"),
 *   )
 */
func (parent *Otel) Start(spanName string, kvs ...kv) *Otel {
	return parent.t.Start(parent.Ctx(), spanName, kvs...)
}

/*
 * Add kvs midway. Accept both Attr and Bag
 */
func (o *Otel) Kvs(kvs ...kv) *Otel {
	if lenkvs := len(kvs); lenkvs > 0 {
		akvs := make([]attribute.KeyValue, lenkvs)
		hasBag := false
		for i, kv := range kvs {
			akvs[i] = kv.akv
			if kv.bag {
				hasBag = true
			}
		}
		o.l = logAkvs(o.s, o.l.With(), akvs...)
		if hasBag {
			o.ctx = _appendBags(o.ctx, kvs...)
		}
	}
	return o
}

func _appendBags(ctx context.Context, bags ...kv) context.Context {
	if len(bags) == 0 {
		return ctx
	}
	bagage := baggage.FromContext(ctx)
	for _, bag := range bags {
		if !bag.bag {
			continue
		}
		k, v := string(bag.akv.Key), bag.akv.Value.String()
		m, err := baggage.NewMember(k, v)
		if err != nil {
			zlog.Info().Err(err).Msgf("opentel._appendBags(%v, %v): fail create member", k, v)
			continue
		}
		bagage, err = bagage.SetMember(m)
		if err != nil {
			zlog.Info().Err(err).Msgf("opentel._appendBags(%v, %v): fail set member", k, v)
		}
	}
	return baggage.ContextWithBaggage(ctx, bagage)
}

/*
 * Add a timestamped event to the span
 */
func (o *Otel) Event(ename string) *Otel {
	o.s.AddEvent(ename)
	return o
}

/*
 * Count an additive counter; exemplar link active span
 */
func (o *Otel) Count(cname string, val float64) *Otel {
	o.t.counter(cname).Add(o.ctx, val)
	return o
}

/*
 * Record a histogram measurement; exemplar link active span
 */
func (o *Otel) Record(hname string, val float64) *Otel {
	o.t.histogram(hname).Record(o.ctx, val)
	return o
}

/*
 * Gauge a point-in-time value; exemplar link active span
 */
func (o *Otel) Gauge(gname string, val float64) *Otel {
	o.t.gauge(gname).Record(o.ctx, val)
	return o
}

/*
 * Send the span
 */
func (o *Otel) End() {
	if o.ok {
		o.s.SetStatus(codes.Ok, "")
	}
	o.s.End()
}
