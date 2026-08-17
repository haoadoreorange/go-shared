//go:build mct

package msgque

import (
	"context"
	"fmt"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── intervalHeaders ──────────────────────────────────────────────────────────

func TestIntervalHeaders_valid(t *testing.T) {
	id, fence := intervalHeaders(map[string]any{"Interval": "abc", "Fence": 3})
	assert.Equal(t, "abc", id)
	assert.Equal(t, 3, fence)
}

func TestIntervalHeaders_missing(t *testing.T) {
	id, fence := intervalHeaders(map[string]any{"Foo": "bar"})
	assert.Equal(t, "", id)
	assert.Equal(t, -1, fence)
}

func TestIntervalHeaders_nil(t *testing.T) {
	id, fence := intervalHeaders(nil)
	assert.Equal(t, "", id)
	assert.Equal(t, -1, fence)
}

// ── integration: interval lifecycle ──────────────────────────────────────────

func TestInterval_cycle(t *testing.T) { // schedule, handler execute, cycle repeat
	received := make(chan Msg, 3)
	go Sub(ctx, "interval", "interval", func(msg Msg, _ func([]byte), _ func(error)) {
		received <- msg
	})
	time.Sleep(10 * time.Millisecond)

	Interval(ctx, "interval", func() []byte { return []byte("tick") }, "test-interval-1", 50*time.Millisecond)

	select {
	case msg := <-received:
		assert.Equal(t, "tick", string(msg.Bytes))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout: expect first execution")
	}
	select {
	case msg := <-received:
		assert.Equal(t, "tick", string(msg.Bytes))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout: expect second execution")
	}
	CancelInterval(ctx, "test-interval-1")
}

func TestInterval_replace(t *testing.T) { // replace keep cycle alive, old goroutine stop
	var newCount atomic.Int32
	var oldBytes atomic.Int32
	go Sub(ctx, "interval.replace", "interval.replace", func(msg Msg, _ func([]byte), _ func(error)) {
		if string(msg.Bytes) == "new" {
			newCount.Add(1)
		}
	})
	time.Sleep(10 * time.Millisecond)

	Interval(ctx, "interval.replace", func() []byte {
		oldBytes.Add(1)
		return []byte("old")
	}, "test-interval-replace", 50*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	Interval(ctx, "interval.replace", func() []byte { return []byte("new") }, "test-interval-replace", 50*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	/* Cycle must continue after replace — at least 2 executions with "new" body
	proves reschedule republished (not stopped on replace) */
	assert.GreaterOrEqual(t, int(newCount.Load()), 2, "expect cycle continue after replace")

	// Old bytes() goroutine must stop
	oldAfterSettle := oldBytes.Load()
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, oldAfterSettle, oldBytes.Load(), "expect old bytes() goroutine stopped")
	CancelInterval(ctx, "test-interval-replace")
}

func TestInterval_cancel(t *testing.T) { // CancelInterval stop the cycle and goroutine
	var count atomic.Int32
	var bytesCount atomic.Int32
	go Sub(ctx, "interval.cancel", "interval.cancel", func(_ Msg, _ func([]byte), _ func(error)) {
		count.Add(1)
	})
	time.Sleep(10 * time.Millisecond)

	Interval(ctx, "interval.cancel", func() []byte {
		bytesCount.Add(1)
		return []byte("tick")
	}, "test-interval-cancel", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, int(count.Load()), 1)

	CancelInterval(ctx, "test-interval-cancel")
	time.Sleep(100 * time.Millisecond)
	countAfterSettle := count.Load()
	bytesAfterSettle := bytesCount.Load()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, countAfterSettle, count.Load(), "expect no execution after cancel settle")
	assert.Equal(t, bytesAfterSettle, bytesCount.Load(), "expect bytes() goroutine stopped after cancel")
}

func TestInterval_status(t *testing.T) { // StatusInterval return correct state
	go Sub(ctx, "interval.state", "interval.state", func(_ Msg, _ func([]byte), _ func(error)) {})
	time.Sleep(10 * time.Millisecond)

	Interval(ctx, "interval.state", func() []byte { return []byte("tick") }, "test-interval-state", 50*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	state := StatusInterval(ctx, "test-interval-state")
	require.NotNil(t, state)
	assert.Equal(t, "test-interval-state", state.Id)
	assert.Equal(t, 50*time.Millisecond, state.Duration)
	assert.WithinDuration(t, time.Now(), state.CreatedAt, 5*time.Second)
	assert.False(t, state.LastAt.IsZero())
	assert.GreaterOrEqual(t, state.Ok, 1)
	assert.Equal(t, 0, state.Er)
	CancelInterval(ctx, "test-interval-state")
}

// ── integration: error handler ───────────────────────────────────────────────

func TestInterval_handler_error(t *testing.T) { // er() increment state.Er and set state.Err
	go Sub(ctx, "interval.er", "interval.er", func(_ Msg, _ func([]byte), er func(error)) {
		er(fmt.Errorf("something broke"))
	})
	time.Sleep(10 * time.Millisecond)

	Interval(ctx, "interval.er", func() []byte { return []byte("tick") }, "test-interval-er", 50*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	state := StatusInterval(ctx, "test-interval-er")
	require.NotNil(t, state)
	assert.GreaterOrEqual(t, state.Er, 1)
	assert.Equal(t, "something broke", state.Err)
	assert.Equal(t, 0, state.Ok)
	CancelInterval(ctx, "test-interval-er")
}

// ── integration: dynamic bytes ───────────────────────────────────────────────

func TestInterval_dynamic_bytes(t *testing.T) { // bytes() called each tick, updated body propagate
	var lastBody atomic.Value
	go Sub(ctx, "interval.dyn", "interval.dyn", func(msg Msg, _ func([]byte), _ func(error)) {
		lastBody.Store(string(msg.Bytes))
	})
	time.Sleep(10 * time.Millisecond)

	seq := atomic.Int32{}
	Interval(ctx, "interval.dyn", func() []byte {
		n := seq.Add(1)
		return []byte(fmt.Sprintf("v%d", n))
	}, "test-interval-dyn", 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	body, _ := lastBody.Load().(string)
	assert.NotEqual(t, "v1", body, "expect body updated beyond initial value")
	assert.GreaterOrEqual(t, int(seq.Load()), 3, "expect bytes() called multiple times")
	CancelInterval(ctx, "test-interval-dyn")
}

func TestInterval_bytes_empty(t *testing.T) { // bytes() return nil still propagate to handler
	var lastBody atomic.Value
	lastBody.Store("unset")
	go Sub(ctx, "interval.empty", "interval.empty", func(msg Msg, _ func([]byte), _ func(error)) {
		lastBody.Store(string(msg.Bytes))
	})
	time.Sleep(10 * time.Millisecond)

	seq := atomic.Int32{}
	Interval(ctx, "interval.empty", func() []byte {
		if seq.Add(1) == 1 {
			return []byte("init")
		}
		return nil // ticker writes nil Body to valkey
	}, "test-interval-empty", 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Ticker writes nil Body → handler eventually receives empty body
	body, _ := lastBody.Load().(string)
	assert.Equal(t, "", body, "expect body empty after bytes() return nil")
	assert.GreaterOrEqual(t, int(seq.Load()), 3, "expect ticker still calling bytes()")
	CancelInterval(ctx, "test-interval-empty")
}

// ── integration: context cancellation ────────────────────────────────────────

func TestInterval_ctx_cancel(t *testing.T) { // ctx cancel stop the ticker goroutine
	ictx, cancel := context.WithCancel(ctx)
	seq := atomic.Int32{}
	go Sub(ctx, "interval.ctx", "interval.ctx", func(_ Msg, _ func([]byte), _ func(error)) {})
	time.Sleep(10 * time.Millisecond)

	Interval(ictx, "interval.ctx", func() []byte {
		seq.Add(1)
		return []byte("tick")
	}, "test-interval-ctx", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	countAfter := seq.Load()
	time.Sleep(200 * time.Millisecond)

	// Ticker goroutine exits on ctx.Done(), no more bytes() calls
	assert.Equal(t, countAfter, seq.Load(), "expect no more bytes() calls after ctx cancel")
	CancelInterval(ctx, "test-interval-ctx")
}

// ── partition: stale fence ───────────────────────────────────────────────────

/*
 * Sequential only (memmq, single consumer). Concurrent case (two consumers
 * handle both msgs simultaneously) not reproducible here — concurrent
 * correctness rely on design analysis in subinterval.go
 */
func TestInterval_stale_fence_discarded(t *testing.T) { // two messages with different fence converge to 1
	var count atomic.Int32
	go Sub(ctx, "interval.stale", "interval.stale", func(_ Msg, _ func([]byte), _ func(error)) {
		count.Add(1)
	})
	time.Sleep(10 * time.Millisecond)

	// Seed state at Fence=5, inject two messages simulating partition
	valki.SetMap(ctx, "msgque/interval/test-interval-stale", map[string]any{
		"Id":        "test-interval-stale",
		"Fence":     5,
		"Duration":  int64(200_000_000), // 200ms
		"CreatedAt": time.Now().UnixNano(),
		"Body":      []byte("tick"),
	})
	h5 := map[string]any{"Interval": "test-interval-stale", "Fence": 5}
	h6 := map[string]any{"Interval": "test-interval-stale", "Fence": 6}
	rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, "interval.stale", nil, false, h5)
	rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, "interval.stale", nil, false, h6)

	// Both handle immediately (2 executions), then each reschedule sleeps 200ms
	// and publishes children with different fences (6 and 7). Higher writes
	// Fence=7, lower child(fence=6) get discarded → converge to 1
	time.Sleep(600 * time.Millisecond)
	countAtConverge := count.Load()

	// After convergence: only 1 execution per 200ms interval
	time.Sleep(400 * time.Millisecond)
	countAfter := count.Load()
	execsDuring400ms := countAfter - countAtConverge

	// 400ms / 200ms = 2 cycles max at single rate. If doubled, would be 4
	assert.LessOrEqual(t, int(execsDuring400ms), 2, "expect single-cycle rate after convergence")
	assert.GreaterOrEqual(t, int(execsDuring400ms), 1, "expect cycle still running")
	valki.Delete(ctx, "msgque/interval/test-interval-stale")
}
