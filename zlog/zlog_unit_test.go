//go:build unit

package zlog

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

/*
 * Mutate package-level inited+once — cannot parallel
 */
func TestInit(t *testing.T) {
	assert.Panics(t, func() { Debug() })
	assert.Panics(t, func() { Info() })
	assert.Panics(t, func() { Warn() })
	assert.Panics(t, func() { Error() })
	assert.Panics(t, func() { With() })
	Init()
	Init() // second call noop
	assert.NotPanics(t, func() { Debug() })
	assert.NotPanics(t, func() { Info() })
	assert.NotPanics(t, func() { Warn() })
	assert.NotPanics(t, func() { Error() })
	assert.NotPanics(t, func() { With() })
}

func Test_cacheSameId(t *testing.T) {
	Init()
	a := _cache("x", os.Stdout, nil, zerolog.InfoLevel)
	b := _cache("x", os.Stdout, nil, zerolog.InfoLevel)
	assert.Equal(t, a, b)
}

func Test_cacheDifferentId(t *testing.T) {
	a := _cache("a", os.Stdout, nil, zerolog.InfoLevel)
	b := _cache("b", os.Stdout, nil, zerolog.WarnLevel)
	assert.NotEqual(t, a, b)
}
