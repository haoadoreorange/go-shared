package msgque

import (
	"context"
	"fmt"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/util"
)

/*
 * Fire and forget. Key is dot-delimited, matched by Sub's key pattern
 * (e.g. Pub "order.created" match Sub "order.*" or "order.#")
 */
func Pub(ctx context.Context, key string, bytes []byte) {
	requireInit("Pub")
	otel := opentel.Start(ctx, "msgque.Pub", opentel.Attr("key", key))
	ctx = otel.Ctx()
	defer otel.End()

	otel.Debug().Lazy(func(l opentel.E) { l.Msgf("%d bytes: %v", len(bytes), bytes) })
	_, err := rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, key, bytes, false)
	if err != nil {
		otel.Warn(err).Msg("fail")
	}
}

/*
 * Like Pub() but return res() that block until the reply arrive and return the raw body
 */
func Rpc(ctx context.Context, key string, data []byte) (func() ([]byte, error), error) {
	requireInit("Rpc")
	otel := opentel.Start(ctx, "msgque.Rpc", opentel.Attr("key", key))
	ctx = otel.Ctx()
	defer otel.End()

	otel.Debug().Lazy(func(l opentel.E) { l.Msgf("%d bytes: %v", len(data), data) })
	reply, err := rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, key, data, true)
	if err != nil {
		return nil, util.Wraperr("msgque.Rpc()", err)
	}

	return func() ([]byte, error) {
		otel := otel.Start("msgque.res", opentel.Attr("key", key))
		defer otel.End()

		select {
		case <-ctx.Done():
			otel.Event("ctx cancel")
			return nil, nil
		case re, ok := <-reply:
			if ctx.Err() != nil {
				otel.Event("ctx cancel")
				return nil, nil
			}
			if !ok {
				return nil, fmt.Errorf("interrupted channel")
			}
			return re.Body, nil
		}
	}, nil
}
