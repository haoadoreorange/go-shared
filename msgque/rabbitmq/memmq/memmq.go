package memmq

import (
	"context"
	"fmt"

	"github.com/haoadoreorange/go-shared/opentel"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * rabbitMq in-memory implementation
 * Use for testing and fallback when RabbitMQ is unavailable
 *
 * Back-pressure: publisher block when queue full
 * Nack write to a secondary requeue channel, a merge goroutine
 * drain requeue with priority then new messages to the output channel
 *
 * Limitations vs real RabbitMQ
 *   - No persistence across restarts
 *   - No distributed consumers
 *   - No unack redelivery, keep this implementation simple, also, no need
 *     If porcelain crash, this crash as well, so it's pointless
 *	   Porcelain can decide to reque unack message if needed
 */

func (m *memMq) Publish(ctx context.Context, exchange, key string, bytes []byte, expectReply bool, headers ...map[string]any) (<-chan amqp.Delivery, error) {
	otel := opentel.Start(ctx, "memmq.Publish", opentel.Attr("ex", exchange), opentel.Attr("key", key))
	ctx = otel.Ctx()
	defer otel.End()

	msg := amqp.Delivery{
		Exchange:    exchange,
		RoutingKey:  key,
		DeliveryTag: m.counter.Add(1),
		Body:        bytes,
	}
	if len(headers) > 0 {
		msg.Headers = amqp.Table(headers[0])
	}
	if exchange == "" {
		// weak logic guard only if something bypass this broker, anything published here pass even if not supposed to
		msg.CorrelationId = key
	}

	reply := m.routeReply(ctx, &msg, expectReply)

	m.rw.RLock()
	defer m.rw.RUnlock()

	if exchange == "" {
		q, ok := m.queues[key]
		if ok {
			msg.ConsumerTag = key // hack: source queue for reque
			q.pub <- msg          // block if full (back-pressure)
		} else {
			otel.Debug().Msg("no direct queue")
		}
	} else {
		matched := false
		for route := range m.routes {
			if match(route.keyPattern, key) {
				matched = true
				q, ok := m.queues[route.queue]
				if !ok {
					otel.Debug().Msgf("unexpect no queue %v, routing %v should create it", route.queue, route.keyPattern)
					return nil, fmt.Errorf("memmq.Publish(): unexpect no queue match key %v", key)
				}
				msg.ConsumerTag = route.queue // hack: source queue for reque
				q.pub <- msg                  // block if full (back-pressure)
			}
		}
		if !matched {
			otel.Debug().Msg("no queue matched")
		}
	}

	return reply, nil
}

func (m *memMq) Consume(_ context.Context, queue string, _ int) (<-chan amqp.Delivery, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	q, ok := m.queues[queue]
	if !ok {
		return nil, fmt.Errorf("memmq.Consume(): no queue %q", queue)
	}
	return q.msgs, nil
}

func (m *memMq) Route(queue, keyPattern string) error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if _, ok := m.queues[queue]; !ok {
		m.queues[queue] = m._newMq()
	}
	m.routes[route{queue, keyPattern}] = struct{}{}
	return nil
}

func (m *memMq) Ack(_ *amqp.Delivery) error {
	m.acks.Add(1)
	return nil
}

func (m *memMq) Nack(msg *amqp.Delivery) error {
	m.nacks.Add(1)
	return m._reque(msg)
}

func (m *memMq) _reque(msg *amqp.Delivery) error {
	m.rw.RLock()
	defer m.rw.RUnlock()

	q, ok := m.queues[msg.ConsumerTag] // hack: source queue (set in Publish)
	if !ok {
		return fmt.Errorf("memmq._reque(): unexpect no source queue %v", msg.ConsumerTag)
	}
	select {
	case q.reque <- *msg:
	default: // drop if reque is full
	}
	return nil
}

func (m *memMq) Dlq(msg *amqp.Delivery) error {
	m.nacks.Add(1)
	return nil
}

func (m *memMq) Stats() (acks, nacks uint64) {
	return m.acks.Load(), m.nacks.Load()
}
