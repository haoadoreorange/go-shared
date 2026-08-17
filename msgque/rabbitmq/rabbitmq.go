package rabbitmq

/*
 * Usage: import rabbitmq, then rabbitmq.Init(ctx) and rabbitmq.GetDefault()
 */

import (
	"context"
	"sync"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/memmq"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/rabbit"
	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/util"
	"github.com/haoadoreorange/go-shared/zlog"

	amqp "github.com/rabbitmq/amqp091-go"
)

var once sync.Once
var inited = false

/*
 * Gate all package-level calls — must be called once at startup
 */
func Init(ctx context.Context) {
	once.Do(func() {
		_cache(ctx, defaultId, util.GetenvTrim(cons.RABBITMQ_ADDR), util.Getenv(cons.RABBITMQ_PUB_SIZE, 10))
		inited = true
	})
}

func Publish(ctx context.Context, exchange, key string, bytes []byte, expectReply bool, headers ...map[string]any) (<-chan amqp.Delivery, error) {
	requireInit("Publish")
	return defaultRat.Publish(ctx, exchange, key, bytes, expectReply, headers...)
}

func Consume(ctx context.Context, queue string, prefetch int) (<-chan amqp.Delivery, error) {
	requireInit("Consume")
	return defaultRat.Consume(ctx, queue, prefetch)
}

func Route(queue, keyPattern string) error {
	requireInit("Route")
	return defaultRat.Route(queue, keyPattern)
}

func Ack(message *amqp.Delivery) error {
	requireInit("Ack")
	return defaultRat.Ack(message)
}

func Nack(message *amqp.Delivery) error {
	requireInit("Nack")
	return defaultRat.Nack(message)
}

func Dlq(message *amqp.Delivery) error {
	requireInit("Dlq")
	return defaultRat.Dlq(message)
}

func Default() rabbitMq {
	requireInit("Default")
	return defaultRat
}

func requireInit(name string) {
	if !inited {
		zlog.Panic().Msgf("rabbitmq.%v(): missing Init()", name)
	}
}

/*
Thin RabbitMQ abstraction → can mock for test
Serialization is the caller's concern on both sides,
Publish accepts []byte, reply and Consume returns amqp.Delivery (caller reads .Body)
These are plumbings, app often use pub/sub porcelains
*/
type rabbitMq interface {
	Publish(ctx context.Context, exchange, key string, bytes []byte, expectReply bool, headers ...map[string]any) (<-chan amqp.Delivery, error)
	Consume(ctx context.Context, queue string, prefetch int) (<-chan amqp.Delivery, error)
	Route(queue, keyPattern string) error
	Ack(message *amqp.Delivery) error
	Nack(message *amqp.Delivery) error
	Dlq(message *amqp.Delivery) error
}

var defaultId = cons.DEFAULT
var defaultRat rabbitMq
var rabbits sync.Map

/* Cache-hit: return stored. Miss: try dial, fallback to memmq */
func _cache(ctx context.Context, id, addr string, pubSize int) rabbitMq {
	if id == defaultId && inited {
		return defaultRat
	}
	otel := opentel.Start(ctx, "rabbitmq._cache", opentel.Attr("id", id))
	ctx = otel.Ctx()
	defer otel.End()

	rb, ok := rabbits.Load(id)
	if !ok {
		if addr == "" {
			otel.Info().Msg("empty addr, fallback to memmq")
			rb, _ = rabbits.LoadOrStore(id, memmq.New(pubSize))
		} else {
			r := rabbit.New(ctx, addr, pubSize)
			if r == nil {
				otel.Warn().Msg("fail dial, fallback to memmq")
				rb, _ = rabbits.LoadOrStore(id, memmq.New(pubSize))
			} else {
				rb, _ = rabbits.LoadOrStore(id, r)
			}
		}
	}
	rat := rb.(rabbitMq)
	if id == defaultId {
		defaultRat = rat
	}
	return rat
}
