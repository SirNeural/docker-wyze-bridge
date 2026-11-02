package motion

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/IDisposable/docker-wyze-bridge/internal/camera"
	"github.com/IDisposable/docker-wyze-bridge/internal/config"
	"github.com/IDisposable/docker-wyze-bridge/internal/wyzeapi"
)

type eventSourceFunc func(context.Context, []string, int64, int64) ([]wyzeapi.MotionEvent, error)

func (f eventSourceFunc) GetDetectionEvents(ctx context.Context, macs []string, begin, end int64, motion, sound bool) ([]wyzeapi.DetectionEvent, error) {
	return f(ctx, macs, begin, end)
}

func TestMotionLifecycle_DuplicatesLateEventsAndRediscovery(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cam := camera.NewCamera(wyzeapi.CameraInfo{MAC: "AABBCC", Name: "front"}, "hd", true, false)
	inventory := []*camera.Camera{cam}
	p := New(nil, func() []*camera.Camera { return inventory }, &config.Config{
		MotionAPI: true, MotionInterval: time.Second, MotionReset: time.Second, MotionMaxAge: 30 * time.Second,
	}, zerolog.Nop())
	p.startedAt = now.Add(-time.Minute)
	changes := 0
	inventoryChanges := 0
	p.OnMotion = func(*camera.Camera) { changes++ }
	p.OnCameraChange = func(oldCamera, newCamera *camera.Camera) { inventoryChanges++ }
	event := wyzeapi.MotionEvent{ID: "first", DeviceID: "aa:bb:cc", Timestamp: now}
	p.apply([]wyzeapi.MotionEvent{event}, now)
	p.apply([]wyzeapi.MotionEvent{event, {ID: "late", DeviceID: "AABBCC", Timestamp: now.Add(-time.Second)}}, now.Add(500*time.Millisecond))
	if changes != 1 || !cam.Snapshot().MotionDetected {
		t.Fatal("duplicate or out-of-order motion changed the pulse")
	}
	next := wyzeapi.MotionEvent{ID: "next", DeviceID: "AABBCC", Timestamp: now.Add(700 * time.Millisecond)}
	p.apply([]wyzeapi.MotionEvent{next}, next.Timestamp)
	p.expire(now.Add(1100 * time.Millisecond))
	if !cam.Snapshot().MotionDetected {
		t.Fatal("new motion did not extend the pulse")
	}
	p.expire(now.Add(1800 * time.Millisecond))
	if cam.Snapshot().MotionDetected || !cam.Snapshot().MotionTimestamp.Equal(next.Timestamp) || changes != 3 {
		t.Fatal("clear lost the timestamp or failed to notify")
	}
	p.apply([]wyzeapi.MotionEvent{next}, now.Add(2*time.Second))
	if changes != 3 {
		t.Fatal("cleared event was replayed")
	}
	p.apply([]wyzeapi.MotionEvent{{ID: "fresh", DeviceID: "AABBCC", Timestamp: now.Add(3 * time.Second)}}, now.Add(3*time.Second))
	inventory = []*camera.Camera{camera.NewCamera(wyzeapi.CameraInfo{MAC: "AABBCC", Name: "renamed"}, "hd", true, false)}
	p.expire(now.Add(3200 * time.Millisecond))
	if cam.Snapshot().MotionDetected || inventory[0].Snapshot().MotionDetected {
		t.Fatal("rediscovery left the removed camera active or replayed its event")
	}
	if inventoryChanges != 2 || !inventory[0].Snapshot().MotionTimestamp.Equal(now.Add(3*time.Second)) {
		t.Fatal("camera replacement did not update discovery and preserve event ordering")
	}
	inventory = nil
	p.expire(now.Add(4 * time.Second))
	if inventoryChanges != 3 {
		t.Fatal("camera removal was not announced")
	}
	p.expire(now.Add(time.Minute))
	if len(p.seen) != 0 || len(p.active) != 0 {
		t.Fatal("expired event metadata was not bounded")
	}
}

func TestMotionLifecycle_RejectsHistoricalFutureAndUnknownEvents(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cam := camera.NewCamera(wyzeapi.CameraInfo{MAC: "AA"}, "hd", false, false)
	p := New(nil, func() []*camera.Camera { return []*camera.Camera{cam} }, &config.Config{
		MotionAPI: true, MotionReset: time.Second, MotionMaxAge: 30 * time.Second,
	}, zerolog.Nop())
	p.startedAt = now.Add(-time.Minute)
	p.apply([]wyzeapi.MotionEvent{
		{ID: "old", DeviceID: "AA", Timestamp: now.Add(-31 * time.Second)},
		{ID: "startup replay", DeviceID: "AA", Timestamp: now.Add(-2 * time.Minute)},
		{ID: "future", DeviceID: "AA", Timestamp: now.Add(time.Second)},
		{ID: "unknown", DeviceID: "BB", Timestamp: now},
	}, now)
	if cam.Snapshot().MotionDetected || len(p.active) != 0 {
		t.Fatal("an invalid-window or unknown-camera event triggered motion")
	}
}

func TestRun_ClearsWhileCloudRequestIsBlockedAndCancels(t *testing.T) {
	cam := camera.NewCamera(wyzeapi.CameraInfo{MAC: "AA"}, "hd", false, false)
	blocked := make(chan struct{})
	calls := 0
	source := eventSourceFunc(func(ctx context.Context, macs []string, begin, end int64) ([]wyzeapi.MotionEvent, error) {
		calls++
		if calls == 1 {
			return []wyzeapi.MotionEvent{{ID: "one", DeviceID: "AA", Timestamp: time.Now()}}, nil
		}
		close(blocked)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p := New(source, func() []*camera.Camera { return []*camera.Camera{cam} }, &config.Config{
		MotionAPI: true, MotionInterval: 10 * time.Millisecond, MotionReset: 20 * time.Millisecond, MotionMaxAge: 30 * time.Second,
	}, zerolog.Nop())
	changes := make(chan bool, 4)
	p.OnMotion = func(cam *camera.Camera) { changes <- cam.Snapshot().MotionDetected }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	select {
	case active := <-changes:
		if !active {
			t.Fatal("initial detection did not activate")
		}
	case <-time.After(time.Second):
		t.Fatal("initial detection was not delivered")
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("second request did not start")
	}
	select {
	case active := <-changes:
		if active {
			t.Fatal("motion was not cleared")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked network request prevented motion reset")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller or request goroutine did not shut down")
	}
}

func TestMotionBackoff_RespectsRateLimitAndCapsOrdinaryFailures(t *testing.T) {
	now := time.Unix(1700000000, 0)
	if delay := retryDelay(&wyzeapi.RateLimitError{RetryAfter: 3 * time.Minute}, 1, time.Second, now); delay != 3*time.Minute {
		t.Fatalf("Retry-After was not respected: %s", delay)
	}
	for _, reset := range []string{
		fmt.Sprint(now.Add(2 * time.Minute).Unix()),
		fmt.Sprint(now.Add(2 * time.Minute).UnixMilli()),
		now.Add(2 * time.Minute).UTC().Format(time.RFC3339),
		now.Add(2 * time.Minute).UTC().Format(time.UnixDate),
	} {
		err := fmt.Errorf("wrapped: %w", &wyzeapi.RateLimitError{ResetBy: reset})
		if delay := retryDelay(err, 1, time.Second, now); delay != 2*time.Minute {
			t.Errorf("reset %q gave %s, want 2m", reset, delay)
		}
	}
	if delay := retryDelay(errors.New("network"), 100, time.Minute, now); delay != 5*time.Minute {
		t.Errorf("unbounded network backoff: %s", delay)
	}
}

func TestSoundLifecycle_IsIndependentOfMotion(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cam := camera.NewCamera(wyzeapi.CameraInfo{MAC: "AA"}, "hd", true, false)
	inventory := []*camera.Camera{cam}
	p := New(nil, func() []*camera.Camera { return inventory }, &config.Config{
		MotionAPI: true, SoundAPI: true, MotionReset: time.Second, SoundReset: 2 * time.Second, MotionMaxAge: 30 * time.Second,
	}, zerolog.Nop())
	p.startedAt = now.Add(-time.Minute)
	motionChanges, soundChanges := 0, 0
	p.OnMotion = func(*camera.Camera) { motionChanges++ }
	p.OnSound = func(*camera.Camera) { soundChanges++ }
	sound := wyzeapi.DetectionEvent{ID: "shared", DeviceID: "AA", Timestamp: now, Kind: wyzeapi.SoundDetection}
	p.apply([]wyzeapi.DetectionEvent{sound}, now)
	p.apply([]wyzeapi.DetectionEvent{sound}, now.Add(500*time.Millisecond))
	if cam.Snapshot().MotionDetected || !cam.Snapshot().SoundDetected || motionChanges != 0 || soundChanges != 1 {
		t.Fatal("sound triggered motion or a duplicate sound notification")
	}
	motion := wyzeapi.DetectionEvent{ID: "shared", DeviceID: "AA", Timestamp: now.Add(500 * time.Millisecond)}
	p.apply([]wyzeapi.DetectionEvent{motion}, motion.Timestamp)
	p.expire(now.Add(1600 * time.Millisecond))
	if cam.Snapshot().MotionDetected || !cam.Snapshot().SoundDetected {
		t.Fatal("motion reset incorrectly cleared sound")
	}
	p.expire(now.Add(2100 * time.Millisecond))
	if cam.Snapshot().SoundDetected || !cam.Snapshot().SoundTimestamp.Equal(now) || soundChanges != 2 {
		t.Fatal("sound did not clear independently while preserving its timestamp")
	}
	p.apply([]wyzeapi.DetectionEvent{sound}, now.Add(2200*time.Millisecond))
	if soundChanges != 2 {
		t.Fatal("cleared sound event was replayed")
	}
	p.apply([]wyzeapi.DetectionEvent{{ID: "new", DeviceID: "AA", Timestamp: now.Add(3 * time.Second), Kind: wyzeapi.SoundDetection}}, now.Add(3*time.Second))
	inventory = nil
	p.expire(now.Add(3100 * time.Millisecond))
	if cam.Snapshot().SoundDetected {
		t.Fatal("removed camera retained sound")
	}
}

func TestSoundOnly_DoesNotCreateMotionAndSharesPolling(t *testing.T) {
	cam := camera.NewCamera(wyzeapi.CameraInfo{MAC: "AA"}, "hd", false, false)
	var p *Poller
	source := eventSourceFunc(func(context.Context, []string, int64, int64) ([]wyzeapi.MotionEvent, error) {
		return []wyzeapi.DetectionEvent{
			{ID: "motion", DeviceID: "AA", Timestamp: time.Now()},
			{ID: "sound", DeviceID: "AA", Timestamp: time.Now(), Kind: wyzeapi.SoundDetection},
		}, nil
	})
	p = New(source, func() []*camera.Camera { return []*camera.Camera{cam} }, &config.Config{
		SoundAPI: true, SoundReset: time.Second, MotionInterval: time.Hour, MotionMaxAge: 30 * time.Second,
	}, zerolog.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	p.OnSound = func(cam *camera.Camera) {
		if cam.Snapshot().SoundDetected {
			cancel()
		}
	}
	go func() { p.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sound-only polling did not deliver and stop")
	}
	if cam.Snapshot().MotionDetected || cam.Snapshot().SoundDetected || cam.Snapshot().SoundTimestamp.IsZero() {
		t.Fatal("sound-only polling triggered motion or failed to clear at shutdown")
	}
}
