package util

import (
	"fmt"
)

func Wraperr(ctx string, err error) error {
	return fmt.Errorf("%v: %w", ctx, err)
}
