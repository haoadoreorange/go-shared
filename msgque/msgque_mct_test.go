//go:build mct

package msgque

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/opentel"
	"github.com/haoadoreorange/go-shared/zlog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestMain(m *testing.M) {
	zlog.Suppress()
	opentel.Init(ctx)
	Init(ctx, nil)
	os.Exit(m.Run())
}

func TestPub(t *testing.T) { // Pub fire-and-forget, Sub receive correct body
	received := make(chan Msg, 1)
	go Sub(ctx, "pub", "pub", func(msg Msg, _ func([]byte), _ func(error)) {
		received <- msg
	})
	time.Sleep(10 * time.Millisecond)

	Pub(ctx, "pub", []byte("hello"))

	select {
	case msg := <-received:
		assert.Equal(t, "hello", string(msg.Bytes))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout: Sub did not receive message")
	}
}

func TestRPC_reply(t *testing.T) { // handler call re → reply published back to caller, ack
	go Sub(ctx, "rpc.reply", "rpc.reply", func(msg Msg, re func([]byte), _ func(error)) {
		re(append(msg.Bytes, []byte(" pong")...))
	})
	time.Sleep(10 * time.Millisecond)

	res, err := Rpc(ctx, "rpc.reply", []byte("ping"))
	require.NoError(t, err)

	result, err := res()
	require.NoError(t, err)
	assert.Equal(t, "ping pong", string(result))
}

func TestRPC_no_reply(t *testing.T) { // handler call neither → re(nil) → res() return nil, ack
	go Sub(ctx, "rpc.noreply", "rpc.noreply", func(_ Msg, _ func([]byte), _ func(error)) {})
	time.Sleep(10 * time.Millisecond)

	res, err := Rpc(ctx, "rpc.noreply", []byte("ping"))
	require.NoError(t, err)

	result, err := res()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestRPC_ctx_cancel(t *testing.T) { // no consumer, ctx cancel, res() return nil, nil
	ctx, cancel := context.WithCancel(context.Background())
	res, err := Rpc(ctx, "rpc.nobody", []byte(""))
	require.NoError(t, err)

	cancel()
	result, err := res()
	assert.Nil(t, result)
	assert.NoError(t, err)
}

type canStats interface{ Stats() (uint64, uint64) }

func TestSub_ack(t *testing.T) { // handler call neither → default message consumed, ack
	s := rabbitmq.Default().(canStats)
	acksBefore, _ := s.Stats()

	done := make(chan bool) // sync: deterministic signal that handler finished
	go Sub(ctx, "sub.ack", "sub.ack", func(_ Msg, _ func([]byte), _ func(error)) {
		close(done)
	})
	time.Sleep(10 * time.Millisecond)

	Pub(ctx, "sub.ack", []byte(""))
	<-done

	time.Sleep(10 * time.Millisecond)
	acksAfter, _ := s.Stats()
	assert.Equal(t, acksBefore+1, acksAfter)
}

func TestSub_nack(t *testing.T) { // handler call er → nack requeue
	s := rabbitmq.Default().(canStats)
	_, nacksBefore := s.Stats()

	done := make(chan bool) // sync: deterministic signal that handler finished
	go Sub(ctx, "sub.nack", "sub.nack", func(_ Msg, _ func([]byte), er func(error)) {
		done <- true
		er(assert.AnError)
	})
	time.Sleep(10 * time.Millisecond)

	Pub(ctx, "sub.nack", []byte(""))
	<-done

	time.Sleep(10 * time.Millisecond)
	_, nacksAfter := s.Stats()
	assert.GreaterOrEqual(t, nacksAfter, nacksBefore+1)
}
