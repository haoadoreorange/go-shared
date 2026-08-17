package rabbit

import (
	"context"

	"github.com/haoadoreorange/go-shared/opentel"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbit struct {
	pubPool chan *amqp.Channel
	con     *amqp.Connection
	repair  func(*amqp.Channel) *amqp.Channel
}

func New(ctx context.Context, addr string, pubSize int) *rabbit {
	otel := opentel.Start(ctx, "rabbit.New", opentel.Attr("addr", addr), opentel.Attr("pub_size", pubSize))
	defer otel.End()

	/* Separate TCP connections for pub/consume, advised by RabbitMQ */
	pub, err := amqp.Dial(addr)
	if err != nil {
		otel.Error(err).Msg("fail dial pub")
		return nil
	}
	con, err := amqp.Dial(addr)
	if err != nil {
		pub.Close()
		otel.Error(err).Msg("fail dial con")
		return nil
	}

	drop := func() {
		pub.Close()
		con.Close()
	}
	go func() {
		<-ctx.Done()
		drop()
	}()

	pubPool := make(chan *amqp.Channel, pubSize)
	for range pubSize {
		ch, err := pub.Channel()
		if err != nil {
			drop()
			otel.Error(err).Msg("fail create pub channel")
			return nil
		}
		pubPool <- ch
	}

	repair := func(broken *amqp.Channel) *amqp.Channel {
		otel := otel.Start("rabbit.repair", opentel.Attr("addr", addr))
		defer otel.End()

		new, err := pub.Channel()
		if err != nil {
			otel.Info(err).Msg("fail create amqp.Channel")
			return broken
		}
		broken.Close()
		return new
	}
	return &rabbit{pubPool, con, repair}
}
