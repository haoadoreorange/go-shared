package rabbit

import (
	"context"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * rabbitMQ implementation
 *
 * Publish use a channel pool — borrow, publish, return. If error, attempt to repair
 * Consume create one channel per call (standard AMQP pattern for multiple consumers)
 *
 * Resources lifecycle is ctx-driven, no explicit Close() needed
 */

func (r *rabbit) Publish(ctx context.Context, exchange, key string, bytes []byte, expectReply bool, headers ...map[string]any) (<-chan amqp.Delivery, error) {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         bytes,
	}
	if len(headers) > 0 {
		msg.Headers = amqp.Table(headers[0])
	}
	if exchange == "" {
		msg.CorrelationId = key // safe guard in case something bypass the broker reply
	}

	reply, err := r.routeReply(ctx, &msg, expectReply)
	if err != nil {
		return nil, util.Wraperr("rabbit.Publish()", err)
	}

	ch := <-r.pubPool
	defer func() { r.pubPool <- ch }()

	err = ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		ch = r.repair(ch)
		return nil, util.Wraperr("rabbit.Publish()", err)
	}

	return reply, nil
}

func (r *rabbit) Consume(ctx context.Context, queue string, prefetch int) (<-chan amqp.Delivery, error) {
	ch, err := r.con.Channel()
	if err != nil {
		return nil, util.Wraperr("rabbit.Consume()", err)
	}

	drop := func() {
		ch.Close()
	}
	go func() {
		<-ctx.Done()
		drop()
	}()

	if err := ch.Qos(prefetch, 0, false); err != nil {
		drop()
		return nil, util.Wraperr("rabbit.Consume()", err)
	}
	msgs, err := ch.ConsumeWithContext(ctx, queue, "", false, false, false, false, nil)
	if err != nil {
		drop()
		return nil, util.Wraperr("rabbit.Consume()", err)
	}
	return msgs, nil
}

func (r *rabbit) Route(queue, keyPattern string) error {
	ch, err := r.con.Channel()
	if err != nil {
		return util.Wraperr("rabbit.Route()", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return util.Wraperr("rabbit.Route()", err)
	}
	err = ch.QueueBind(queue, keyPattern, cons.DEFAULT_TOPIC, false, nil)
	if err != nil {
		return util.Wraperr("rabbit.Route()", err)
	}
	return nil
}

func (r *rabbit) Ack(msg *amqp.Delivery) error {
	return msg.Ack(false)
}

func (r *rabbit) Nack(msg *amqp.Delivery) error {
	return msg.Nack(false, true)
}

func (r *rabbit) Dlq(msg *amqp.Delivery) error {
	return msg.Nack(false, false)
}
