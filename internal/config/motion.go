package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func (c *Config) loadMotion() error {
	for _, flag := range []struct {
		name string
		dst  *bool
	}{
		{"MOTION_API", &c.MotionAPI}, {"SOUND_API", &c.SoundAPI},
	} {
		raw, exists := os.LookupEnv(flag.name)
		if !exists {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "true", "1", "yes", "on":
			*flag.dst = true
		case "false", "0", "no", "off":
			*flag.dst = false
		default:
			return fmt.Errorf("%s must be true or false", flag.name)
		}
	}
	for _, option := range []struct {
		name     string
		dst      *time.Duration
		fallback time.Duration
		maximum  time.Duration
	}{
		{"MOTION_INT", &c.MotionInterval, 5 * time.Second, time.Minute},
		{"MOTION_RESET", &c.MotionReset, 10 * time.Second, 5 * time.Minute},
		{"SOUND_RESET", &c.SoundReset, 10 * time.Second, 5 * time.Minute},
		{"MOTION_MAX_AGE", &c.MotionMaxAge, 30 * time.Second, 5 * time.Minute},
	} {
		*option.dst = option.fallback
		raw, exists := os.LookupEnv(option.name)
		if !exists {
			continue
		}
		value, err := time.ParseDuration(raw)
		if err != nil {
			seconds, parseErr := strconv.ParseFloat(raw, 64)
			if parseErr != nil || !(seconds >= 1 && seconds <= option.maximum.Seconds()) {
				return fmt.Errorf("%s must be a duration or seconds between 1 and %g", option.name, option.maximum.Seconds())
			}
			value = time.Duration(seconds * float64(time.Second))
		}
		if value < time.Second || value > option.maximum {
			return fmt.Errorf("%s must be between 1s and %s", option.name, option.maximum)
		}
		*option.dst = value
	}
	if (c.MotionAPI || c.SoundAPI) && c.MotionMaxAge < c.MotionInterval {
		return fmt.Errorf("MOTION_MAX_AGE must be at least MOTION_INT")
	}
	return nil
}
