package rabbit

import (
	"context"

	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * Create a dedicated channel with an exclusive auto-delete queue
 * if expectReply is true. Mutate msg to set ReplyTo and CorrelationId.
 *
 * Return a channel that deliver the reply (nil otherwise), the reply is not guaranteed to present or valid
 */
func (r *rabbit) routeReply(ctx context.Context, msg *amqp.Publishing, expectReply bool) (<-chan amqp.Delivery, error) {
	if !expectReply {
		return nil, nil
	}

	reply := make(chan amqp.Delivery, 1)
	ch, err := r.con.Channel()
	if err != nil {
		return nil, util.Wraperr("rabbit.setupReply()", err)
	}

	drop := func() {
		close(reply)
		ch.Close()
	}
	go func() {
		<-ctx.Done()
		drop()
	}()

	reque, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		drop()
		return nil, util.Wraperr("rabbit.setupReply()", err)
	}
	replies, err := ch.ConsumeWithContext(ctx, reque.Name, "", true, true, false, false, nil)
	if err != nil {
		drop()
		return nil, util.Wraperr("rabbit.setupReply()", err)
	}

	go func() {
		select {
		case <-ctx.Done():
		case re, ok := <-replies:
			otel := opentel.Start(ctx, "rabbit.reply")
			defer otel.End()
			defer drop()
			if !ok {
				otel.Warn().Msg("unexpect closed channel")
			} else if reque.Name != re.CorrelationId {
				otel.Warn().Msgf("expect correlationId %v, get %v", reque.Name, re.CorrelationId)
			} else {
				reply <- re
			}
		}
	}()
	msg.ReplyTo = reque.Name
	msg.CorrelationId = reque.Name
	return reply, nil
}
