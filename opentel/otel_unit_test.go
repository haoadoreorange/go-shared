//go:build unit

package opentel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	interfacet "go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/baggage"
)

func Test_getBags(t *testing.T) {
	t.Parallel()

	m, _ := baggage.NewMember("k", "v")
	bagage, _ := baggage.New(m)
	ctx := baggage.ContextWithBaggage(context.Background(), bagage)
	bags := _getBags(ctx)

	assert.Len(t, bags, 1)
	assert.Equal(t, "k", string(bags[0].Key))
	assert.Equal(t, "v", bags[0].Value.AsString())
}

func TestKvs_AttrOnSpanAndLogger(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Kvs(Attr("k", "v"))
	otel.End()
	spans := getSpans()
	require.Len(t, spans, 1)

	assert.Contains(t, spans[0].Attributes, attribute.String("k", "v"))
}

func TestKvs_BagOnContext(t *testing.T) {
	t.Parallel()

	otel, _, _ := Test("")
	otel.Kvs(Bag("id", "abc"))
	otel.End()
	bagage := baggage.FromContext(otel.Ctx())

	assert.Equal(t, "abc", bagage.Member("id").Value())
}

func Test_appendBags_SkipAttr(t *testing.T) {
	t.Parallel()
	ctx := _appendBags(context.Background(), Attr("key", "val"))
	assert.Empty(t, _getBags(ctx))
}

func Test_appendBags_Empty(t *testing.T) {
	t.Parallel()
	ctx := _appendBags(context.Background())
	assert.Empty(t, _getBags(ctx))
}

func Test_appendBags_NewMembers(t *testing.T) {
	t.Parallel()

	ctx := _appendBags(context.Background(), Bag("k1", "v1"), Bag("k2", "v2"))
	bags := _getBags(ctx)
	bagage := baggage.FromContext(ctx)

	assert.Len(t, bags, 2)
	assert.Equal(t, "v1", bagage.Member("k1").Value())
	assert.Equal(t, "v2", bagage.Member("k2").Value())
}

func Test_appendBags_PreservesExisting(t *testing.T) {
	t.Parallel()

	m, _ := baggage.NewMember("existing", "val")
	bagage, _ := baggage.New(m)
	ctx := baggage.ContextWithBaggage(context.Background(), bagage)
	ctx = _appendBags(ctx, Bag("", ""))
	bagage = baggage.FromContext(ctx)

	assert.Equal(t, "val", bagage.Member("existing").Value())
}

func TestOtel_Ctx(t *testing.T) {
	t.Parallel()
	otel, _, _ := Test("")
	defer otel.End()
	assert.Equal(t, otel.ctx, otel.Ctx())
}

/*
 * Child and parent share trace, child's parent is parent's span
 */
func TestOtel_Start_ChildSpan(t *testing.T) {
	t.Parallel()

	parent, getSpans, _ := Test("p")
	child := parent.Start("c")
	child.End()
	parent.End()
	spans := getSpans()
	require.Len(t, spans, 2)

	assert.Equal(t, spans[0].SpanContext.TraceID(), spans[1].SpanContext.TraceID())
	assert.Equal(t, spans[1].SpanContext.SpanID(), spans[0].Parent.SpanID())
}

func TestEvent(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Event("cache miss")
	otel.End()

	spans := getSpans()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].Events, 1)

	assert.Equal(t, "cache miss", spans[0].Events[0].Name)
}

func TestCount(t *testing.T) {
	t.Parallel()

	otel, _, findMetric := Test("")
	defer otel.End()
	otel.Count("requests", 1)
	otel.Count("requests", 1)

	found := findMetric("requests")
	require.NotNil(t, found)
	sum := found.(metricdata.Sum[float64])
	require.Len(t, sum.DataPoints, 1)

	assert.Equal(t, 2.0, sum.DataPoints[0].Value)
	assertExemplar(t, found, otel.s.SpanContext())
}

func TestRecord(t *testing.T) {
	t.Parallel()

	otel, _, findMetric := Test("")
	defer otel.End()
	otel.Record("latency_ms", 42.5)

	found := findMetric("latency_ms")
	require.NotNil(t, found)
	hist := found.(metricdata.Histogram[float64])
	require.Len(t, hist.DataPoints, 1)

	assert.Equal(t, 42.5, hist.DataPoints[0].Sum)
	assert.Equal(t, uint64(1), hist.DataPoints[0].Count)
	assertExemplar(t, found, otel.s.SpanContext())
}

func TestGauge(t *testing.T) {
	t.Parallel()

	otel, _, findMetric := Test("")
	defer otel.End()
	otel.Gauge("queue_depth", 7)

	found := findMetric("queue_depth")
	require.NotNil(t, found)
	gauge := found.(metricdata.Gauge[float64])
	require.Len(t, gauge.DataPoints, 1)

	assert.Equal(t, 7.0, gauge.DataPoints[0].Value)
	assertExemplar(t, found, otel.s.SpanContext())
}

func TestOtel_Chain(t *testing.T) {
	t.Parallel()

	otel, getSpans, findMetric := Test("")
	otel.Kvs(Attr("k", "v")).
		Count("c", 1).
		Record("r", 2.5).
		Event("e").
		End()

	spans := getSpans()
	require.Len(t, spans, 1)
	c := findMetric("c")
	require.NotNil(t, c)
	r := findMetric("r")
	require.NotNil(t, r)

	assert.Contains(t, spans[0].Attributes, attribute.String("k", "v"))
	assert.Equal(t, "e", spans[0].Events[0].Name)
	assert.Equal(t, 1.0, c.(metricdata.Sum[float64]).DataPoints[0].Value)
	assert.Equal(t, 2.5, r.(metricdata.Histogram[float64]).DataPoints[0].Sum)
}

/* assertExemplar verify the metric data point carry an exemplar linked to the span */
func assertExemplar(t *testing.T, data metricdata.Aggregation, sc interfacet.SpanContext) {
	t.Helper()

	var exemplars []metricdata.Exemplar[float64]
	switch d := data.(type) {
	case metricdata.Sum[float64]:
		require.NotEmpty(t, d.DataPoints)
		exemplars = d.DataPoints[0].Exemplars
	case metricdata.Histogram[float64]:
		require.NotEmpty(t, d.DataPoints)
		exemplars = d.DataPoints[0].Exemplars
	case metricdata.Gauge[float64]:
		require.NotEmpty(t, d.DataPoints)
		exemplars = d.DataPoints[0].Exemplars
	default:
		t.Fatalf("unexpected metric data type: %T", data)
	}
	require.NotEmpty(t, exemplars)

	traceID := sc.TraceID()
	spanID := sc.SpanID()
	assert.Equal(t, traceID[:], exemplars[0].TraceID)
	assert.Equal(t, spanID[:], exemplars[0].SpanID)
}
