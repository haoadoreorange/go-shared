package msgque

import (
	"context"
	"time"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/opentel"

	"github.com/go-viper/mapstructure/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * handle() → reschedule() → handle() form the self-reschedule loop
 */
func handleInterval(ctx context.Context, handler func(Msg, func([]byte), func(error)), msg *amqp.Delivery) bool {
	iid, fence := intervalHeaders(msg.Headers)
	if iid == "" || fence == -1 {
		return false
	}
	otel := opentel.Start(ctx, "msgque.handleInterval", opentel.Bag("interval_id", iid))
	ctx = otel.Ctx()
	defer otel.End()

	interval, err := fetchInterval(ctx, valki, iid)
	if err != nil {
		otel.Info(err).Msg("fail fetch interval")
		nack(otel, msg)
		return true
	}
	if interval == nil || fence < interval.Fence {
		otel.Debug().Msg("cancelled or fenced")
		ack(otel, msg)
		return true
	}
	if interval.isCorrupt() {
		otel.Error().Msgf("interval isCorrupt %+v", interval)
		dlq(otel, msg)
		return true
	}
	/* Fence from header, not Valkey. reschedule publish child at Fence+1.
	On partition (2 msgs in flight), both read same Valkey Fence, both compute
	same Fence+1, children are same-fence — can never fence each other out
	(N < N is false). Stays 2 messages forever.

	Example: msg(fence=5) and msg(fence=6), Valkey Fence=5
	Valkey Fence: both read 5, both publish child(fence=6), both pass 6<6
	Header Fence: msg(5)→child(6), msg(6)→child(7), write Fence=7, child(6) arrive 6<7 → discard → collapse to 1

	Can't fix by always incrementing Valkey Fence in step 3 either — second
	writer advance Fence past what was published (5→6→7), both children carry
	fence=6, both arrive 6<7 → discard → cycle die, no one published fence=7 */
	interval.Fence = fence

	var red bool
	var erd bool
	re := func(_ []byte) {
		if red {
			return
		}
		interval.Ok += 1
		interval.Err = ""
		red = true
	}
	er := func(err error) {
		if erd {
			return
		}
		otel.Info(err).Msg("fail")
		interval.Er += 1
		interval.Err = err.Error()
		erd = true
	}

	otel.Kvs(opentel.Attr("duration", time.Duration(interval.Duration)), opentel.Attr("iteration", interval.Ok+interval.Er))
	otel.Event("execute")
	handler(Msg{msg.RoutingKey, interval.Body}, re, er)
	interval.LastAt = time.Now().UnixNano()
	if !red && !erd {
		re(nil)
	}
	go func() { // MUST use go routine to not block
		if reschedule(ctx, msg.RoutingKey, interval) {
			ack(otel, msg)
		} else {
			nack(otel, msg)
		}
	}()
	return true
}

/*
Update state in valkey, re-publish message, order of state update vs publish
1. No cross-system transaction between RabbitMQ and Valkey, one go first
2. Publish cannot first: wait interval → also delay state (should be updated as soon as handled)
3. Therefore state MUST go first
4. But state (Fence+1) → publish fail → old message invalid → cycle die, uncoverable
5. Fence is the guard, Fence MUST be updated after publish
*/
func reschedule(ctx context.Context, key string, newI *interval) bool {
	duration := time.Duration(newI.Duration)
	otel := opentel.Start(ctx, "msgque.reschedule", opentel.Attr("duration", duration), opentel.Attr("iteration", newI.Ok+newI.Er))
	ctx = otel.Ctx()
	defer otel.End()

	/* Step 1: update state without advancing Fence */
	cancelOrFence := false
	err := valki.Tx(ctx, func(tx valky) error {
		currentI, er := fetchInterval(ctx, tx, newI.Id)
		if er != nil {
			return er
		}
		if currentI == nil || currentI.Fence > newI.Fence {
			cancelOrFence = true
			return nil
		}
		if currentI.CreatedAt > newI.CreatedAt {
			return nil // replaced, skip state write, still republish for new owner
		}
		var interval map[string]any
		mapstructure.Decode(newI, &interval)
		delete(interval, "Body")                     // update by publisher, not overwrite here
		return tx.SetMap(ctx, newI.vkey(), interval) // Fence included, catch up valkey → may converge faster
	}, newI.vkey())
	if err != nil {
		otel.Info(err).Msg("fail update state")
	}
	if cancelOrFence {
		otel.Debug().Msg("cancelled or fenced")
		return true
	}

	/* Step 2: publish, advance Fence+1 */
	select {
	case <-ctx.Done():
		otel.Event("ctx cancel")
		return false
	case <-time.After(duration):
		if ctx.Err() != nil {
			otel.Event("ctx cancel")
			return false
		}
	}
	newI.Fence += 1
	headers := make(map[string]any)
	headers["Interval"] = newI.Id
	headers["Fence"] = newI.Fence
	_, err = rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, key, nil, false, headers)
	if err != nil {
		otel.Warn(err).Msgf("fail re-publish to %v", key)
		return false
	}

	/* Step 3: update Fence, failure may partition (duplicate message) but eventual consistent */
	err = valki.Tx(ctx, func(tx valky) error {
		currentI, er := fetchInterval(ctx, tx, newI.Id)
		if er != nil { // network partition, cannot know if cancel, attempt to Fence might corrupt it
			return er // accept partition, Fence will eventual consistent
		}
		if currentI == nil || currentI.CreatedAt > newI.CreatedAt || currentI.Fence >= newI.Fence {
			return nil
		}
		return tx.SetField(ctx, newI.vkey(), "Fence", newI.Fence)
	}, newI.vkey())
	if err != nil {
		otel.Debug(err).Msg("fail Fence")
	}
	return true
}

func intervalHeaders(headers map[string]any) (string, int) {
	var h struct {
		Interval string
		Fence    int
	}
	mapstructure.WeakDecode(headers, &h)
	if h.Interval == "" || h.Fence == 0 {
		return "", -1
	}
	return h.Interval, h.Fence
}

func dlq(otel *opentel.Otel, msg *amqp.Delivery) {
	otel.Event("try dlq")
	err := rabbitmq.Dlq(msg)
	if err != nil {
		otel.Info(err).Msg("fail dlq")
		return
	}
}
