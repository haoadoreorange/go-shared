//go:build unit

package memmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		key     string
		expect  bool
	}{
		{"exact match", "order.created", "order.created", true},
		{"exact mismatch", "order.created", "order.deleted", false},
		{"star one word", "order.*", "order.created", true},
		{"star mismatch multi", "order.*", "order.us.created", false},
		{"hash zero words", "order.#", "order", true},
		{"hash one word", "order.#", "order.created", true},
		{"hash multi words", "order.#", "order.us.east.created", true},
		{"star middle", "order.*.done", "order.created.done", true},
		{"star middle mismatch", "order.*.done", "order.created.v1.done", false},
		{"star wrap", "*.created.*", "order.created.v1", true},
		{"star wrap mismatch", "*.created.*", "order.deleted.v1", false},
		{"star wrap left only", "*.created.*", "order.created", false},
		{"star wrap right only", "*.created.*", "created.v1", false},
		{"hash middle", "order.#.done", "order.us.east.done", true},
		{"hash middle zero", "order.#.done", "order.done", true},
		{"hash wrap", "#.created.#", "a.b.created.c.d", true},
		{"hash wrap zero", "#.created.#", "created", true},
		{"hash wrap left only", "#.created.#", "a.created", true},
		{"hash wrap right only", "#.created.#", "created.a", true},
		{"all hash", "#", "anything.goes.here", true},
		{"empty key exact", "", "", true},
		{"empty pattern mismatch", "", "order", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, match(test.pattern, test.key))
		})
	}
}
