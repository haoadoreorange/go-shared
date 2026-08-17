package zlog

import (
	"bytes"
	"io"

	"github.com/tidwall/gjson"
)

type orderJsonWriter struct {
	out   io.Writer
	order map[string]int
}

func (w orderJsonWriter) Write(in []byte) (n int, err error) {
	if len(in) == 0 || in[0] != '{' {
		return w.out.Write(in)
	}

	type field struct{ key, value string }
	fields := make([]field, len(w.order))
	gjson.ParseBytes(in).ForEach(func(key, value gjson.Result) bool {
		k := key.String()
		if i, ok := w.order[k]; ok {
			fields[i] = field{k, value.Raw}
		} else {
			fields = append(fields, field{k, value.Raw})
		}
		return true
	})

	out := bytes.Buffer{}
	out.Grow(len(in))
	out.WriteByte('{')

	for _, f := range fields {
		if f.key == "" {
			continue
		}
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		out.WriteByte('"')
		out.WriteString(f.key)
		out.WriteByte('"')
		out.WriteByte(':')
		out.WriteString(f.value)
	}

	out.WriteByte('}')
	out.WriteByte('\n')
	return w.out.Write(out.Bytes())
}
