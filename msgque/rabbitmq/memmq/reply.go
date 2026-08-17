package memmq

import (
	"context"
	"fmt"

	"github.com/haoadoreorange/go-shared/opentel"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * routeReply create a one-shot reply queue if expectReply is true.
 * Set delivery.ReplyTo and CorrelationId. Start a goroutine that
 * wait for the reply, validate correlationId, forward to replyCh,
 * then close and delete the queue
 */
func (m *memMq) routeReply(ctx context.Context, msg *amqp.Delivery, expectReply bool) <-chan amqp.Delivery {
	if !expectReply {
		return nil
	}

	reply := make(chan amqp.Delivery, 1)
	replies := make(chan amqp.Delivery, 1)
	requeName := fmt.Sprintf("amq.mem.reply.%d", msg.DeliveryTag)
	m.rw.Lock()
	m.queues[requeName] = &mq{pub: replies}
	m.rw.Unlock()

	drop := func() {
		close(reply)
		m.rw.Lock()
		defer m.rw.Unlock()
		delete(m.queues, requeName)
	}
	go func() {
		<-ctx.Done()
		drop()
	}()

	go func() {
		select {
		case <-ctx.Done():
		case re, ok := <-replies:
			otel := opentel.Start(ctx, "memmq.reply")
			defer otel.End()
			defer drop()
			if !ok {
				otel.Warn().Msg("unexpect closed channel")
			} else if requeName != re.CorrelationId {
				otel.Warn().Msgf("expect correlationId %v, get %v", requeName, re.CorrelationId)
			} else {
				reply <- re
			}
		}
	}()
	msg.ReplyTo = requeName
	msg.CorrelationId = requeName
	return reply
}
