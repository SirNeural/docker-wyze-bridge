// Package motion forwards camera-native detections without decoding video.
package motion

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/IDisposable/docker-wyze-bridge/internal/camera"
	"github.com/IDisposable/docker-wyze-bridge/internal/config"
	"github.com/IDisposable/docker-wyze-bridge/internal/wyzeapi"
)

type EventSource interface {
	GetMotionEvents(context.Context, []string, int64, int64) ([]wyzeapi.MotionEvent, error)
}

type activeMotion struct {
	camera *camera.Camera
	until  time.Time
}

type Poller struct {
	source         EventSource
	cameras        func() []*camera.Camera
	interval       time.Duration
	reset          time.Duration
	maxAge         time.Duration
	startedAt      time.Time
	seen           map[string]time.Time
	active         map[string]activeMotion
	known          map[string]*camera.Camera
	log            zerolog.Logger
	OnMotion       func(*camera.Camera)
	OnStatus       func(error)
	OnCameraChange func(oldCamera, newCamera *camera.Camera)
}

func New(source EventSource, cameras func() []*camera.Camera, cfg *config.Config, log zerolog.Logger) *Poller {
	return &Poller{
		source: source, cameras: cameras, interval: cfg.MotionInterval,
		reset: cfg.MotionReset, maxAge: cfg.MotionMaxAge, log: log,
		seen: make(map[string]time.Time), active: make(map[string]activeMotion),
		known: make(map[string]*camera.Camera),
	}
}

type result struct {
	events []wyzeapi.MotionEvent
	err    error
}

// Run owns all event state. Network requests run separately so a slow or
// failing cloud API cannot leave a motion signal permanently active.
func (p *Poller) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p.startedAt = time.Now()
	results := make(chan result)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.poll(ctx, results)
	}()
	defer func() {
		cancel()
		<-done
		for key := range p.active {
			p.clear(key)
		}
		if p.OnStatus != nil {
			p.OnStatus(context.Canceled)
		}
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.expire(now)
		case r := <-results:
			if p.OnStatus != nil {
				p.OnStatus(r.err)
			}
			if r.err != nil {
				p.log.Warn().Err(r.err).Msg("Wyze motion event polling failed")
				continue
			}
			p.apply(r.events, time.Now())
		}
	}
}

func (p *Poller) poll(ctx context.Context, results chan<- result) {
	delay := time.Duration(0)
	failures := 0
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		cameras := p.cameras()
		macs := make([]string, 0, len(cameras))
		for _, cam := range cameras {
			macs = append(macs, cam.GetInfo().MAC)
		}
		delay = p.interval
		if len(macs) == 0 {
			continue // An empty Wyze filter could fetch unrelated account devices.
		}
		now := time.Now()
		begin := now.Add(-p.maxAge)
		if begin.Before(p.startedAt) {
			begin = p.startedAt
		}
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		events, err := p.source.GetMotionEvents(requestCtx, macs, begin.UnixMilli(), now.UnixMilli())
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures++
			delay = retryDelay(err, failures, p.interval, now)
		} else {
			failures = 0
		}
		select {
		case <-ctx.Done():
			return
		case results <- result{events, err}:
		}
	}
}

func normalizeID(id string) string {
	return strings.ToUpper(strings.NewReplacer(":", "", "-", "").Replace(id))
}

func (p *Poller) apply(events []wyzeapi.MotionEvent, now time.Time) {
	p.expire(now)
	cameras := make(map[string]*camera.Camera)
	for _, cam := range p.cameras() {
		cameras[normalizeID(cam.GetInfo().MAC)] = cam
	}
	for _, event := range events {
		id := normalizeID(event.DeviceID)
		cam := cameras[id]
		key := id + "/" + event.ID
		if cam == nil || event.Timestamp.Before(p.startedAt) || now.Sub(event.Timestamp) > p.maxAge || event.Timestamp.After(now) {
			continue
		}
		if _, duplicate := p.seen[key]; duplicate {
			continue
		}
		p.seen[key] = event.Timestamp.Add(p.maxAge)
		if !event.Timestamp.After(cam.Snapshot().MotionTimestamp) {
			continue // Out-of-order events must not extend or replay a pulse.
		}
		cam.SetMotion(true, event.Timestamp)
		p.active[id] = activeMotion{cam, now.Add(p.reset)}
		if p.OnMotion != nil {
			p.OnMotion(cam)
		}
	}
}

func (p *Poller) expire(now time.Time) {
	current := make(map[string]*camera.Camera)
	for _, cam := range p.cameras() {
		current[normalizeID(cam.GetInfo().MAC)] = cam
	}
	for id, active := range p.active {
		if !now.Before(active.until) || current[id] != active.camera {
			p.clear(id)
		}
	}
	for id, old := range p.known {
		if current[id] == nil && p.OnCameraChange != nil {
			p.OnCameraChange(old, nil)
		}
	}
	for id, cam := range current {
		if old := p.known[id]; old != cam {
			var timestamp time.Time
			if old != nil {
				timestamp = old.Snapshot().MotionTimestamp
			}
			cam.SetMotion(false, timestamp)
			if p.OnCameraChange != nil {
				p.OnCameraChange(old, cam)
			}
		}
	}
	p.known = current
	for key, until := range p.seen {
		if !now.Before(until) {
			delete(p.seen, key)
		}
	}
}

func (p *Poller) clear(id string) {
	cam := p.active[id].camera
	cam.SetMotion(false, time.Time{})
	delete(p.active, id)
	if p.OnMotion != nil {
		p.OnMotion(cam)
	}
}

func retryDelay(err error, failures int, interval time.Duration, now time.Time) time.Duration {
	delay := min(interval*time.Duration(1<<min(failures, 6)), 5*time.Minute)
	var rateLimit *wyzeapi.RateLimitError
	if errors.As(err, &rateLimit) {
		delay = max(delay, time.Minute, rateLimit.RetryAfter)
		if reset, parseErr := strconv.ParseInt(rateLimit.ResetBy, 10, 64); parseErr == nil {
			until := time.Unix(reset, 0)
			if reset > 1e12 {
				until = time.UnixMilli(reset)
			}
			delay = max(delay, until.Sub(now))
		} else if until, parseErr := time.Parse(time.RFC3339, rateLimit.ResetBy); parseErr == nil {
			delay = max(delay, until.Sub(now))
		} else if until, parseErr := time.Parse(time.UnixDate, rateLimit.ResetBy); parseErr == nil {
			delay = max(delay, until.Sub(now))
		}
	}
	return delay
}
