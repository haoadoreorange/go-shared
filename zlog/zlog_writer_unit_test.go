//go:build unit

package zlog

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderJsonWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		order  map[string]int
		in     string
		expect string
	}{
		{
			name:   "empty input",
			order:  nil,
			in:     "",
			expect: "",
		},
		{
			name:   "non-json passthrough",
			order:  nil,
			in:     "plain text log\n",
			expect: "plain text log\n",
		},
		{
			name:   "reorder",
			order:  map[string]int{"time": 0, "level": 1, "message": 2},
			in:     `{"host":"localhost","level":"info","time":"2026-01-01","message":"hello"}` + "\n",
			expect: `{"time":"2026-01-01","level":"info","message":"hello","host":"localhost"}` + "\n",
		},
		{
			name:   "already ordered",
			order:  map[string]int{"time": 0, "level": 1, "message": 2},
			in:     `{"time":"2026-01-01","level":"info","message":"hello"}` + "\n",
			expect: `{"time":"2026-01-01","level":"info","message":"hello"}` + "\n",
		},
		{
			name:   "missing ordered field",
			order:  map[string]int{"time": 0, "message": 2},
			in:     `{"level":"info","host":"localhost"}` + "\n",
			expect: `{"level":"info","host":"localhost"}` + "\n",
		},
		{
			name:   "no order",
			order:  nil,
			in:     `{"level":"info","time":"2026-01-01"}` + "\n",
			expect: `{"level":"info","time":"2026-01-01"}` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			orderJsonWriter{&out, test.order}.Write([]byte(test.in))
			assert.Equal(t, test.expect, out.String())
		})
	}
}
