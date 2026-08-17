package zlog

/*
 * Usage: import zlog, then zlog.Debug(), etc.
 */

import (
	"io"
	"os"
	"sync"

	"github.com/haoadoreorange/go-shared/cons"
	"github.com/haoadoreorange/go-shared/util"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var once sync.Once
var inited = false

/*
 * Gate all package-level calls — must be called once at startup
 */
func Init() {
	once.Do(func() { // memory barrier
		level := util.GetenvTrim(cons.LOG_LEVEL)
		zlevel, _ := zerolog.ParseLevel(level)
		if zlevel == zerolog.NoLevel {
			if level != "" {
				log.Warn().Msgf("zlog.Init(): invalid log level %q, fallback to info", level)
			}
			zlevel = zerolog.InfoLevel
		}
		_cache(defaultId, os.Stdout, []string{"time", "level", "span", "message"}, zlevel)
		inited = true
	})
}

func Suppress() func() { // Useful sometimes, e.g. test
	Init() // this first, otherwise Init() overwrite Suppress()
	old := defaultLogger
	defaultLogger = LazyLogger{old.Output(io.Discard)}
	return func() { defaultLogger = old } // revert
}

/*
 * Even with Debug disabled, `zlog.Debug().Msgf("%v", expensive())` still execute
 * expensive() since all arguments are evaluated at call site
 * This wrapper evaluate it only if the level is enabled
 *
 * zlog.Debug/Info().Lazy(func (e) {
 *     e.Msgf("%v", expensive())
 * })
 *
 * This is still zero-alloc because it's short enough to be inline and the closure
 * doesn't escape, aka zero cost abstraction.
 * It does have the cost that the binary is bigger because of duplicate inlines,
 * don't use by default, only when it's expensive()
 *
 * Can also just check `if zlog.Debug().Enabled()` directly
 */
type E = *zerolog.Event // so lazier don't need to import zerolog
func (l LazyEvent) Lazy(f func(E)) {
	if l.Enabled() {
		f(l.Event)
	}
}

func With() zerolog.Context              { requireInit("With"); return defaultLogger.With() }
func Level(lvl zerolog.Level) LazyLogger { requireInit("Level"); return defaultLogger.Level(lvl) }
func Debug() LazyEvent                   { requireInit("Debug"); return defaultLogger.Debug() }
func Info() LazyEvent                    { requireInit("Info"); return defaultLogger.Info() }
func Warn() *zerolog.Event               { requireInit("Warn"); return defaultLogger.Warn() }
func Error() *zerolog.Event              { requireInit("Error"); return defaultLogger.Error() }
func Panic() *zerolog.Event              { requireInit("Panic"); return defaultLogger.Panic() }
func Fatal() *zerolog.Event              { requireInit("Fatal"); return defaultLogger.Fatal() }

func requireInit(name string) {
	if !inited {
		panic("zlog." + name + "(): missing Init()")
	}
}

var defaultId = cons.DEFAULT
var defaultLogger LazyLogger
var zlogs sync.Map

/* Cache-hit: return stored logger. Miss: new logger with custom fields order, e.g. {time, level, ...} */
func _cache(id string, out io.Writer, fieldsOrder []string, level zerolog.Level) LazyLogger {
	if id == defaultId && inited { // check id first, faster for non defalt
		return defaultLogger
	}
	z, ok := zlogs.Load(id)
	if !ok {
		order := make(map[string]int, len(fieldsOrder))
		for i, k := range fieldsOrder {
			order[k] = i
		}
		z, _ = zlogs.LoadOrStore(id, &LazyLogger{
			zerolog.New(orderJsonWriter{out, order}).With().Timestamp().Logger().Level(level),
		})
	}
	l := *z.(*LazyLogger)
	if id == defaultId {
		defaultLogger = l
	}
	return l
}
