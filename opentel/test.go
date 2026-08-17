//go:build unit || mct || api

package opentel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

/*
 * Isolated otel for test, caller must End() to send the span; metric and log fire immediately
 * No side effects on package-level state
 *
 *   otel, getSpans, findMetric := opentel.Test("op",
 *       opentel.Attr("request_id", "r1"),
 *       opentel.Bag("user_id", "abc"),
 *   )
 */
func Test(spanName string, kvs ...kv) (*Otel, func() tracetest.SpanStubs, func(string) metricdata.Aggregation) {
	exporter := tracetest.NewInMemoryExporter()
	reader := metric.NewManualReader()

	otel := cacheTracer(
		"test",
		trace.NewTracerProvider(trace.WithSyncer(exporter)),
		metric.NewMeterProvider(metric.WithReader(reader), metric.WithExemplarFilter(exemplar.AlwaysOnFilter)),
		&sync.Map{},
	).Start(context.Background(), spanName, kvs...)

	getSpans := func() tracetest.SpanStubs {
		return exporter.GetSpans()
	}

	findMetric := func(name string) metricdata.Aggregation {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return nil
		}
		for _, sm := range rm.ScopeMetrics {
			if sm.Scope.Name != "test" {
				continue
			}
			for i := range sm.Metrics {
				if sm.Metrics[i].Name == name {
					return sm.Metrics[i].Data
				}
			}
		}
		return nil
	}

	return otel, getSpans, findMetric
}
