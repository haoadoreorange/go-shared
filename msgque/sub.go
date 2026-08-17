package msgque

import (
	"context"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Msg struct {
	Key   string
	Bytes []byte
}

/*
 * Subscribe to a key pattern and block until ctx cancel
 * Caller wrap in `go Sub(...)` for async
 *
 * Subscribers with the same id share subscription (horizontal scale)
 * Pattern is dot-delimited with wildcards
 *   order.created  → exact match
 *   order.*        → one word wildcard (order.created, order.deleted)
 *   order.#        → zero or more words (order.created, order.us.east.created)
 *
 * Handler receive message and two callbacks
 *   return([]byte)	→ publish bytes if ReplyTo set, try ack
 *   error(error)	→ try nack with requeue (transient failure, should retry)
 *   neither called → fallback to return(nil) to unblock RPC caller
 *
 * Return error on setup failure, nil on ctx cancel or channel close
 */
func Sub(ctx context.Context, id, keyPattern string, handler func(Msg, func([]byte), func(error))) error {
	requireInit("Sub")
	otel := opentel.Start(ctx, "msgque.Sub", opentel.Bag("sub_id", id), opentel.Attr("pattern", keyPattern))
	ctx = otel.Ctx()
	err := rabbitmq.Route(id, keyPattern)
	if err != nil {
		return util.Wraperr("msgqueue.Sub()", err)
	}
	msgs, err := rabbitmq.Consume(ctx, id, 50)
	if err != nil {
		return util.Wraperr("msgqueue.Sub()", err)
	}
	otel.End()

	for {
		if ctx.Err() != nil {
			otel.Debug().Msg("ctx cancel")
			return nil
		}
		select { // 50% chance NOT return even if cancel, need the above guard
		case <-ctx.Done():
			otel.Debug().Msg("ctx cancel")
			return nil
		case msg, ok := <-msgs:
			if !ok {
				otel.Info().Msg("closed publisher")
				return nil
			}
			_ = handleInterval(ctx, handler, &msg) || handle(ctx, handler, &msg)
		}
	}
}

func handle(ctx context.Context, handler func(Msg, func([]byte), func(error)), msg *amqp.Delivery) bool {
	otel := opentel.Start(ctx, "msgque.handle", opentel.Attr("route_key", msg.RoutingKey))
	ctx = otel.Ctx()
	defer otel.End()

	var red bool
	var erd bool
	var res []byte
	re := func(result []byte) {
		if red {
			return
		}
		if msg.ReplyTo != "" {
			_, err := rabbitmq.Publish(ctx, "", msg.ReplyTo, result, false)
			if err != nil {
				otel.Info(err).Msg("fail pub reply")
				res = result
				return
			}
		}
		ack(otel, msg)
		red = true
	}
	er := func(err error) {
		if erd {
			return
		}
		otel.Info(err).Msg("fail")
		nack(otel, msg)
		erd = true
	}

	otel.Event("execute")
	handler(Msg{msg.RoutingKey, msg.Body}, re, er)
	if !red && !erd {
		re(res)
	}
	return true
}

func ack(otel *opentel.Otel, msg *amqp.Delivery) {
	otel.Event("try ack")
	err := rabbitmq.Ack(msg)
	if err != nil {
		otel.Info(err).Msg("fail ack")
	}
}

func nack(otel *opentel.Otel, msg *amqp.Delivery) {
	otel.Event("try nack")
	err := rabbitmq.Nack(msg)
	if err != nil {
		otel.Info(err).Msg("fail nack")
	}
}
