//go:build unit

package util

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

type mock[T comparable] struct {
	name    string
	value   string
	defaulv T
	expect  T
	unset   bool
}

const key = "TEST"

func TestGetenv_string(t *testing.T) {
	_TestGetenv(t, []mock[string]{
		{name: "unset", defaulv: "default", expect: "default", unset: true},
		{name: "empty", defaulv: "default", expect: "default"},
		{name: "not empty", value: "test_value", defaulv: "default", expect: "test_value"},
		{name: "with spaces", value: "hello world", defaulv: "default", expect: "hello world"},
	})
}

func TestGetenv_bool(t *testing.T) {
	log.Logger = zerolog.Nop()
	_TestGetenv(t, []mock[bool]{
		{name: "valid", value: "true", defaulv: false, expect: true},
		{name: "invalid", value: "maybe", defaulv: true, expect: true},
	})
}

func TestGetenv_float(t *testing.T) {
	log.Logger = zerolog.Nop()
	_TestGetenv(t, []mock[float64]{
		{name: "valid", value: "3.14", defaulv: 0.0, expect: 3.14},
		{name: "invalid", value: "not_a_float", defaulv: 1.0, expect: 1.0},
	})
}

func TestGetenvTrim(t *testing.T) {
	t.Setenv(key, "  hello  ")
	assert.Equal(t, "hello", GetenvTrim(key))
}

func _TestGetenv[T comparable](t *testing.T, tests []mock[T]) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.unset {
				t.Setenv(key, "")
			} else {
				t.Setenv(key, test.value)
			}
			assert.Equal(t, test.expect, Getenv(key, test.defaulv))
		})
	}
}
