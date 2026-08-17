package zlog

import "github.com/rs/zerolog"

type LazyLogger struct {
	zerolog.Logger
}

type LazyEvent struct {
	*zerolog.Event
}

func (l LazyLogger) Level(lvl zerolog.Level) LazyLogger {
	return LazyLogger{l.Logger.Level(lvl)}
}

func (l LazyLogger) Debug() LazyEvent {
	return LazyEvent{l.Logger.Debug()}
}

func (l LazyLogger) Info() LazyEvent {
	return LazyEvent{l.Logger.Info()}
}
