package opentel

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

type kv struct {
	akv attribute.KeyValue
	bag bool
}

/*
 * Attach to this span and logger only
 */
func Attr(key string, value any) kv {
	return kv{_toKV(key, value), false}
}

func _toKV(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int64(key, int64(v))
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case string:
		return attribute.String(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}

/*
 * Propagate via context to child spans and across services
 * String-only — OTel baggage travel as HTTP headers, no type system
 */
func Bag(key, value string) kv {
	return kv{attribute.String(key, value), true}
}
