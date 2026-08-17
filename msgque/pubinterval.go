package msgque

import (
	"context"
	"fmt"
	"time"

	"github.com/haoadoreorange/go-shared/msgque/rabbitmq"
	"github.com/haoadoreorange/go-shared/msgque/rabbitmq/cons"
	"github.com/haoadoreorange/go-shared/opentel"

	"github.com/go-viper/mapstructure/v2"
)

/*
 * Like Pub() with interval. id uniquely identify the interval for
 * replace (call again with same id), Cancel() and State()
 *
 * bytes() is sent on interval, timing is soft, i.e. replace, Cancel() and bytes()
 * are not immediate, in-flight message may execute one more time with stale data
 * before detected. See subinterval.go comment
 */
func Interval(ctx context.Context, key string, bytes func() []byte, id string, duration time.Duration) {
	requireInit("Interval")
	otel := opentel.Start(ctx, "msgque.Interval", opentel.Bag("interval_id", id), opentel.Attr("duration", duration))
	ctx = otel.Ctx()
	defer otel.End()

	if bytes == nil {
		bytes = func() []byte { return nil }
	}
	newI := &interval{
		Id:        id,
		Body:      bytes(),
		Duration:  duration.Nanoseconds(),
		CreatedAt: time.Now().UnixNano(),
	}
	otel.Debug().Lazy(func(l opentel.E) {
		l.Msgf("%d bytes: %v", len(newI.Body), newI.Body)
	})
	if newI.isCorrupt() || !schedule(ctx, key, newI) {
		return
	}

	go func() { // bytes() on interval
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		i := 0 // for logging
		for {
			select {
			case <-ctx.Done():
				otel.Debug().Msg("ctx cancel")
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					otel.Debug().Msg("ctx cancel")
					return
				}

				i += 1
				otel := otel.Start("msgque.Interval.tick", opentel.Attr("iteration", i))

				bytes := bytes()
				otel.Debug().Lazy(func(l opentel.E) {
					l.Msgf("try send %v bytes: %v", len(bytes), bytes)
				})

				cancelOrReplace := false // set body if still valid
				err := valki.Tx(ctx, func(tx valky) error {
					currentI, er := fetchInterval(ctx, tx, newI.Id)
					if er != nil { // network partition, cannot know if cancel, attempt to bytes() might corrupt it
						return er
					}
					if currentI == nil || currentI.CreatedAt > newI.CreatedAt {
						cancelOrReplace = true
						return nil
					}
					return tx.SetField(ctx, newI.vkey(), "Body", bytes)
				}, newI.vkey())
				if err != nil {
					otel.Info(err).Msg("fail send bytes()")
				}
				otel.End()
				if cancelOrReplace {
					return
				}
			}
		}
	}()
}

/*
Write actual state in valkey, publish message for coordination and fencing
Order of state write vs publish
1. No cross-system transaction between RabbitMQ and Valkey, one go first
2. Publish first → message might arrive without state → unable to distinguish cancel
3. Therefore state MUST go first
4. But state → publish may crash/fail → can't rely on state exist to avoid republish
5. Check Fence instead, Fence MUST be updated after publish
*/
func schedule(ctx context.Context, key string, newI *interval) bool {
	otel := opentel.Start(ctx, "msgque.schedule", opentel.Attr("duration", time.Duration(newI.Duration)))
	ctx = otel.Ctx()
	defer otel.End()

	/* Step 1: write state without advancing Fence */
	err := valki.Tx(ctx, func(tx valky) error {
		currentI, er := fetchInterval(ctx, tx, newI.Id)
		if er != nil { // network partition, cannot know if replaced, deliberate choice to continue instead of fail
			otel.Info(er).Msg("fail fetch interval, might overwrite a newer one")
		}
		if currentI != nil {
			if currentI.CreatedAt >= newI.CreatedAt {
				return fmt.Errorf("replaced")
			}
			if currentI.Fence > 0 {
				newI.Fence = currentI.Fence
			}
		}
		var interval map[string]any
		mapstructure.Decode(newI, &interval)
		return tx.SetMap(ctx, newI.vkey(), interval)
	}, newI.vkey())
	if err != nil {
		otel.Warn(err).Msg("fail write state")
		return false
	}

	/* Step 2: publish, advance Fence+1 */
	if newI.Fence > 0 {
		otel.Debug().Msg("already in-flight, skip publish")
		return true
	}
	newI.Fence += 1
	headers := make(map[string]any)
	headers["Interval"] = newI.Id
	headers["Fence"] = newI.Fence
	_, err = rabbitmq.Publish(ctx, cons.DEFAULT_TOPIC, key, nil, false, headers)
	if err != nil {
		otel.Warn(err).Msgf("fail publish to %v", key)
		return false
	}

	/* Step 3: update Fence, failure may partition (duplicate message) but eventual consistent */
	cancelOrReplace := false
	err = valki.Tx(ctx, func(tx valky) error {
		currentI, er := fetchInterval(ctx, tx, newI.Id)
		if er != nil { // network partition, cannot know if cancel, attempt to Fence might corrupt it
			return er // accept partition, Fence will eventual consistent
		}
		if currentI == nil || currentI.CreatedAt > newI.CreatedAt {
			cancelOrReplace = true
			return nil
		}
		if currentI.Fence > 0 {
			otel.Error().Msg("wrong code logic, should be impossible — other racer with CreatedAt <= this one could never make it here")
		}
		return tx.SetField(ctx, newI.vkey(), "Fence", newI.Fence)
	}, newI.vkey())
	if err != nil {
		otel.Debug(err).Msg("fail Fence")
	}
	return !cancelOrReplace
}

/*
 * Not immediate, in-flight message may execute one more time with stale data
 * before detected. See subinterval.go comment
 */
func CancelInterval(ctx context.Context, id string) {
	requireInit("CancelInterval")
	otel := opentel.Start(ctx, "msgque.CancelInterval", opentel.Attr("interval_id", id))
	defer otel.End()

	err := valki.Delete(ctx, (&interval{Id: id}).vkey())
	if err != nil {
		otel.Warn(err).Msg("fail")
	}
}

type status struct {
	Id        string
	Duration  time.Duration
	CreatedAt time.Time
	LastAt    time.Time
	Ok        int
	Er        int
	Err       string
}

func StatusInterval(ctx context.Context, id string) *status {
	requireInit("StatusInterval")
	otel := opentel.Start(ctx, "msgque.StatusInterval", opentel.Attr("interval_id", id))
	defer otel.End()

	interval, err := fetchInterval(otel.Ctx(), valki, id)
	if interval == nil {
		if err != nil {
			otel.Warn(err).Msg("fail")
		}
		return nil
	}
	return &status{
		Id:        interval.Id,
		Duration:  time.Duration(interval.Duration),
		CreatedAt: time.Unix(0, interval.CreatedAt),
		LastAt:    time.Unix(0, interval.LastAt),
		Ok:        interval.Ok,
		Er:        interval.Er,
		Err:       interval.Err,
	}
}
