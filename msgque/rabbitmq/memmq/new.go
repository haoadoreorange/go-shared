package memmq

import (
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type memMq struct {
	rw      sync.RWMutex
	queues  map[string]*mq     // pointer: map index return copy, can't assign field on value type
	routes  map[route]struct{} // idempotent Route(), same as RabbitMQ QueueBind
	counter atomic.Uint64      // atomic: Publish hold RLock, multiple publishers increment concurrently
	acks    atomic.Uint64
	nacks   atomic.Uint64
	pubSize int
}

type mq struct {
	pub   chan amqp.Delivery // publish
	reque chan amqp.Delivery // nack
	msgs  chan amqp.Delivery // consume, merge pub with prioritized reque
}

type route struct {
	queue      string
	keyPattern string
}

func New(pubSize int) *memMq {
	return &memMq{
		queues:  make(map[string]*mq),
		routes:  make(map[route]struct{}),
		pubSize: pubSize,
	}
}

func (m *memMq) _newMq() *mq {
	size := m.pubSize * 100
	pub := make(chan amqp.Delivery, size)
	reque := make(chan amqp.Delivery, size)
	msgs := make(chan amqp.Delivery, size)

	go func() {
		for {
		ratio: // ~2:1 for reque
			for range 2 {
				select {
				case msg := <-reque:
					msgs <- msg
				default:
					break ratio
				}
			}
			select {
			case msg := <-reque:
				msgs <- msg
			case msg := <-pub:
				msgs <- msg
			}
		}
	}()

	return &mq{pub, reque, msgs}
}
