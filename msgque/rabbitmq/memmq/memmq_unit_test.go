//go:build unit

package memmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	zlog.Suppress()
	opentel.Init(context.Background())
	os.Exit(m.Run())
}

var ctx = context.Background()

func TestMemMq_basic(t *testing.T) { // route bind queue to pattern, publish matching key, verify delivery
	t.Parallel()

	m := New(1)
	require.NoError(t, m.Route("orders", "order.*"))
	msgs, err := m.Consume(ctx, "orders", 50)
	require.NoError(t, err)
	_, err = m.Publish(ctx, cons.DEFAULT_TOPIC, "order.created", []byte("hello"), false)
	require.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, "hello", string(msg.Body))
		assert.Equal(t, "order.created", msg.RoutingKey)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout: no message received")
	}
}

func TestMemMq_no_route(t *testing.T) { // publish non-matching key, verify no delivery
	t.Parallel()

	m := New(1)
	require.NoError(t, m.Route("orders", "order.*"))
	msgs, err := m.Consume(ctx, "orders", 50)
	require.NoError(t, err)
	_, err = m.Publish(ctx, cons.DEFAULT_TOPIC, "x", nil, false)
	require.NoError(t, err)

	select {
	case <-msgs:
		t.Fatal("should not receive message")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMemMq_direct_to_queue(t *testing.T) { // exchange="" route directly by queue name, ignore pattern
	t.Parallel()

	m := New(1)
	require.NoError(t, m.Route("q", ""))
	msgs, err := m.Consume(ctx, "q", 50)
	require.NoError(t, err)
	_, err = m.Publish(ctx, "", "q", []byte("direct"), false)
	require.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, "direct", string(msg.Body))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout: no message received")
	}
}

func TestMemMq_reply(t *testing.T) { // publish with reply, consumer echo back via default exchange
	t.Parallel()

	m := New(1)
	require.NoError(t, m.Route("q", "rpc"))
	msgs, err := m.Consume(ctx, "q", 50)
	require.NoError(t, err)

	go func() { // consumer: echo body back via ReplyTo
		for msg := range msgs {
			m.Publish(ctx, "", msg.ReplyTo, msg.Body, false)
		}
	}()

	reply, err := m.Publish(ctx, cons.DEFAULT_TOPIC, "rpc", []byte("ping"), true)
	require.NoError(t, err)

	select {
	case re := <-reply:
		assert.Equal(t, "ping", string(re.Body))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout: no reply received")
	}
}
