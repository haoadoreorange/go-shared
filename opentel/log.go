package opentel

/*
 * Log methods return *zerolog.Event for caller to chain.
 * trace_id/span_id are already on o.l from Start time.
 *
 * Span status
 *   Error/Panic/Fatal → always SetStatus(Error); RecordError if err passed
 *   Debug/Info/Warn   → no err: End() set Ok; with err: stay Unset (recoverable)
 */

import (
	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	interfacet "go.opentelemetry.io/otel/trace"
)

type E = *zerolog.Event // so lazier don't need to import zerolog

func (o *Otel) With() zerolog.Context { return o.l.With() }

func (o *Otel) Debug(errs ...error) zlog.LazyEvent {
	e := o.l.Debug()
	_ = o.firstErr(errs, e.Event)
	return e
}

func (o *Otel) Info(errs ...error) zlog.LazyEvent {
	e := o.l.Info()
	_ = o.firstErr(errs, e.Event)
	return e
}

func (o *Otel) Warn(errs ...error) *zerolog.Event {
	e := o.l.Warn()
	_ = o.firstErr(errs, e)
	return e
}

func (o *Otel) Error(errs ...error) *zerolog.Event {
	e := o.l.Error()
	o.s.SetStatus(codes.Error, o.firstErr(errs, e))
	o.ok = false
	return e
}

func (o *Otel) Panic(errs ...error) *zerolog.Event {
	e := o.l.Panic()
	o.s.SetStatus(codes.Error, o.firstErr(errs, e))
	o.ok = false
	return e
}

func (o *Otel) Fatal(errs ...error) *zerolog.Event {
	e := o.l.Fatal()
	o.s.SetStatus(codes.Error, o.firstErr(errs, e))
	o.ok = false
	return e
}

/* record + log error if present, return error message */
func (o *Otel) firstErr(errs []error, e *zerolog.Event) string {
	if len(errs) == 0 {
		return ""
	}
	o.s.RecordError(errs[0])
	o.ok = false
	e.Err(errs[0])
	return errs[0].Error()
}

func logAkvs(s interfacet.Span, l zerolog.Context, akvs ...attribute.KeyValue) zlog.LazyLogger {
	if len(akvs) > 0 {
		s.SetAttributes(akvs...)
		for _, akv := range akvs {
			switch akv.Value.Type() {
			case attribute.BOOL:
				l = l.Bool(string(akv.Key), akv.Value.AsBool())
			case attribute.INT64:
				l = l.Int64(string(akv.Key), akv.Value.AsInt64())
			case attribute.FLOAT64:
				l = l.Float64(string(akv.Key), akv.Value.AsFloat64())
			default:
				l = l.Str(string(akv.Key), akv.Value.String())
			}
		}
	}
	return zlog.LazyLogger{Logger: l.Logger()}
}
