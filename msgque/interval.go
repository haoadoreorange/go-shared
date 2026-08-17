package msgque

import (
	"context"
	"errors"

	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/go-viper/mapstructure/v2"
)

type interval struct {
	Id    string
	Fence int
	// valkey only, time as nanoseconds int64 — wire-proof
	Body      []byte
	Duration  int64
	CreatedAt int64
	LastAt    int64 // 0 if never ran
	Ok        int
	Er        int
	Err       string // last err, empty if ok
}

func (iv *interval) isCorrupt() bool {
	if iv.isNil("isCorrupt") {
		return true
	}
	return iv.Id == "" || iv.Fence < 0 || iv.Duration <= 0 || iv.CreatedAt <= 0 ||
		iv.LastAt < 0 || iv.Ok < 0 || iv.Er < 0
}

func (iv *interval) vkey() string {
	if iv.isNil("vkey") {
		return ""
	}
	return "msgque/interval/" + iv.Id
}

func (iv *interval) isNil(caller string) bool {
	if iv == nil {
		zlog.Error().Msgf("interval.%v(): nil receiver", caller)
		return true
	}
	return false
}

func fetchInterval(ctx context.Context, valki valky, id string) (*interval, error) {
	v, err := valki.GetMap(ctx, (&interval{Id: id}).vkey())
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil { // valkey disconnect
		return nil, err
	}
	var iv interval
	mapstructure.WeakDecode(v, &iv)
	return &iv, nil
}
