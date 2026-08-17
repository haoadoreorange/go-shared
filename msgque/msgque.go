package msgque

import (
	"context"
	"sync"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/zlog"
)

var once sync.Once
var inited bool

/*
 * MUST opentel.Init() first
 */
func Init(ctx context.Context, kv valky) {
	once.Do(func() {
		rabbitmq.Init(ctx)
		valki = kv
		inited = true
	})
}

func requireInit(name string) {
	if !inited {
		zlog.Panic().Msgf("msgque.%v(): missing Init()", name)
	}
}
