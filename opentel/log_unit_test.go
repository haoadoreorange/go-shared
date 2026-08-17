//go:build unit

package opentel

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

/*
 * Start populate the logger with trace context and kvs
 * Bag propagate to child, Attr don't
 */
func TestStart_LogFields(t *testing.T) {
	t.Parallel()

	otel, _, _ := Test("",
		Attr("user_id", 123),
		Bag("count", "42"),
	)
	defer otel.End()
	var buf bytes.Buffer
	otel.l = zlog.LazyLogger{Logger: otel.l.Output(&buf)}
	otel.Info().Msg("")

	out := buf.String()
	assert.NotEmpty(t, gjson.Get(out, "trace_id").String())
	assert.NotEmpty(t, gjson.Get(out, "span_id").String())
	assert.Equal(t, int64(123), gjson.Get(out, "user_id").Int())
	assert.Equal(t, "42", gjson.Get(out, "count").String())

	child := otel.Start("c")
	defer child.End()
	buf.Reset()
	child.l = zlog.LazyLogger{Logger: child.l.Output(&buf)}
	child.Info().Msg("")

	out = buf.String()
	assert.Empty(t, gjson.Get(out, "user_id").String())
	assert.Equal(t, "42", gjson.Get(out, "count").String())
}

/*
 * Info(err) record the error but span status stay Unset (recoverable)
 */
func TestInfo_WithErr(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Info(fmt.Errorf("")).Msg("")
	otel.End()

	spans := getSpans()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].Events, 1)

	assert.Equal(t, codes.Unset, spans[0].Status.Code)
	assert.Equal(t, "exception", spans[0].Events[0].Name)
}

/*
 * Info() set span status Ok via End()
 */
func TestInfo_NoErr(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Info().Msg("")
	otel.End()

	spans := getSpans()
	require.Len(t, spans, 1)

	assert.Equal(t, codes.Ok, spans[0].Status.Code)
}

/*
 * Error(err) set span status Error and record the error on the span
 */
func TestError_WithErr(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Error(fmt.Errorf("refused")).Msg("")
	otel.End()

	spans := getSpans()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].Events, 1)

	assert.Equal(t, codes.Error, spans[0].Status.Code)
	assert.Equal(t, "refused", spans[0].Status.Description)
	assert.Equal(t, "exception", spans[0].Events[0].Name)
}

/*
 * Error() still set span status Error
 */
func TestError_NoErr(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("")
	otel.Error().Msg("")
	otel.End()

	spans := getSpans()
	require.Len(t, spans, 1)

	assert.Equal(t, codes.Error, spans[0].Status.Code)
}

func TestLogAkvs(t *testing.T) {
	t.Parallel()

	otel, getSpans, _ := Test("",
		Attr("b", true),
		Attr("i", 42),
		Attr("f", 3.14),
		Attr("s", "hello"),
	)
	var buf bytes.Buffer
	otel.l = zlog.LazyLogger{Logger: otel.l.Output(&buf)}
	otel.Info().Msg("")
	otel.End()

	out := buf.String()
	spans := getSpans()
	require.Len(t, spans, 1)

	assert.Equal(t, true, gjson.Get(out, "b").Bool())
	assert.Equal(t, int64(42), gjson.Get(out, "i").Int())
	assert.InDelta(t, 3.14, gjson.Get(out, "f").Float(), 0.001)
	assert.Equal(t, "hello", gjson.Get(out, "s").String())

	assert.Contains(t, spans[0].Attributes, attribute.Bool("b", true))
	assert.Contains(t, spans[0].Attributes, attribute.Int64("i", 42))
	assert.Contains(t, spans[0].Attributes, attribute.Float64("f", 3.14))
	assert.Contains(t, spans[0].Attributes, attribute.String("s", "hello"))
}
