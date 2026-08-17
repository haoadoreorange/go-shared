//go:build unit

package opentel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestAttr_NotPropagate(t *testing.T) {
	t.Parallel()
	a := Attr("k", "v")
	assert.False(t, a.bag)
}

func TestBag_Propagate(t *testing.T) {
	t.Parallel()
	b := Bag("k", "v")
	assert.True(t, b.bag)
}

func Test_toKV_Bool(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", true)
	assert.Equal(t, attribute.BOOL, kv.Value.Type())
	assert.True(t, kv.Value.AsBool())
}

func Test_toKV_Int(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", 42)
	assert.Equal(t, attribute.INT64, kv.Value.Type())
	assert.Equal(t, int64(42), kv.Value.AsInt64())
}

func Test_toKV_Int64(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", int64(99))
	assert.Equal(t, attribute.INT64, kv.Value.Type())
	assert.Equal(t, int64(99), kv.Value.AsInt64())
}

func Test_toKV_Float64(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", 3.14)
	assert.Equal(t, attribute.FLOAT64, kv.Value.Type())
	assert.Equal(t, 3.14, kv.Value.AsFloat64())
}

func Test_toKV_String(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", "val")
	assert.Equal(t, attribute.STRING, kv.Value.Type())
	assert.Equal(t, "val", kv.Value.AsString())
}

func Test_toKV_Fallback(t *testing.T) {
	t.Parallel()
	kv := _toKV("k", []int{1, 2})
	assert.Equal(t, attribute.STRING, kv.Value.Type())
	assert.Equal(t, "[1 2]", kv.Value.AsString())
}
