package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

/*
 * Get the environment variable parsed to the same type as defaulv
 * If the variable is unset, empty, or fail to parse, return defaulv
 */
func Getenv[T any](key string, defaulv T) T {
	value := GetenvTrim(key)
	if value == "" {
		return defaulv
	}

	switch any(defaulv).(type) {
	case string:
		return any(value).(T) // cast to T
	case bool:
		boo, err := strconv.ParseBool(value) // cannot use Sscan, it silently accept invalid bool (return false, nil)
		if err != nil {
			log.Warn().Msgf("util.Getenv(): expect bool, get %v=%v", key, value)
			return defaulv
		}
		return any(boo).(T) // cast to T
	default:
		var num T
		if _, err := fmt.Sscan(value, &num); err != nil {
			log.Warn().Err(err).Msgf("util.Getenv(): expect numeric, get %v=%v", key, value)
			return defaulv
		}
		return num
	}
}

func GetenvTrim(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}
