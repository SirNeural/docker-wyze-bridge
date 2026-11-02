package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"

	"github.com/IDisposable/docker-wyze-bridge/internal/camera"
	"github.com/IDisposable/docker-wyze-bridge/internal/config"
	"github.com/IDisposable/docker-wyze-bridge/internal/wyzeapi"
)

type completedMotionToken struct{ paho.Token }

func (completedMotionToken) Wait() bool   { return true }
func (completedMotionToken) Error() error { return nil }

type motionPublication struct {
	topic, payload string
	retained       bool
}

type motionRecordingClient struct {
	paho.Client
	mu       sync.Mutex
	messages []motionPublication
}

func (r *motionRecordingClient) Publish(topic string, qos byte, retained bool, payload interface{}) paho.Token {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, motionPublication{topic, fmt.Sprint(payload), retained})
	return completedMotionToken{}
}

func (r *motionRecordingClient) Subscribe(string, byte, paho.MessageHandler) paho.Token {
	return completedMotionToken{}
}

func (r *motionRecordingClient) last(t *testing.T, topic string) motionPublication {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.messages) - 1; i >= 0; i-- {
		if r.messages[i].topic == topic {
			return r.messages[i]
		}
	}
	t.Fatalf("topic was not published: %s", topic)
	return motionPublication{}
}

func TestMotionPublication_ReconnectResetAndAvailability(t *testing.T) {
	cam := camera.NewCamera(wyzeapi.CameraInfo{Name: "front", Nickname: "Front", MAC: "AA"}, "hd", true, false)
	mgr := camera.NewManager(&config.Config{}, nil, nil, zerolog.Nop())
	mgr.InjectCamera("front", cam)
	recorder := &motionRecordingClient{}
	c := &Client{
		paho: recorder, camMgr: mgr, topic: "test", dtopic: "ha",
		publishSem: make(chan struct{}, maxInflightPublishes), log: zerolog.Nop(),
		motionEnabled: true,
	}
	timestamp := time.Unix(1700000000, 0)
	cam.SetMotion(true, timestamp)
	c.PublishCameraMotion(cam)
	if msg := recorder.last(t, "test/front/motion"); msg.payload != "1" || !msg.retained {
		t.Fatal("active motion was not retained")
	}
	c.PublishDetectionAvailability(true)
	c.onConnect()
	if recorder.last(t, "test/front/motion").payload != "1" || recorder.last(t, "test/bridge/motion_state").payload != "online" {
		t.Fatal("reconnect did not restore current motion and cloud availability")
	}
	cam.SetMotion(false, time.Time{})
	c.PublishCameraMotion(cam)
	c.PublishDetectionAvailability(false)
	c.onConnect()
	if recorder.last(t, "test/front/motion").payload != "2" || recorder.last(t, "test/front/motion_ts").payload != "1700000000" {
		t.Fatal("clear or reconnect lost the last real detection timestamp")
	}
	if recorder.last(t, "test/bridge/motion_state").payload != "offline" {
		t.Fatal("reconnect falsely restored cloud availability after a failure")
	}
	// Restarted camera objects must overwrite an old retained pulse.
	mgr.InjectCamera("front", camera.NewCamera(cam.GetInfo(), "hd", true, false))
	c.onConnect()
	if recorder.last(t, "test/front/motion").payload != "2" || recorder.last(t, "test/front/motion_ts").payload != "0" {
		t.Fatal("restart replayed retained motion")
	}
	c.camMgr = camera.NewManager(&config.Config{}, nil, nil, zerolog.Nop())
	c.onConnect()
	if recorder.last(t, "ha/binary_sensor/AA_motion/config").payload != "" {
		t.Fatal("reconnect left discovery for a camera removed while offline")
	}
}

func TestMotionDiscovery_RenameDisableAndRemoval(t *testing.T) {
	recorder := &motionRecordingClient{}
	c := &Client{
		paho: recorder, topic: "test", dtopic: "ha", motionEnabled: true,
		publishSem: make(chan struct{}, maxInflightPublishes), log: zerolog.Nop(),
	}
	cam := camera.NewCamera(wyzeapi.CameraInfo{Name: "renamed", Nickname: "Front", MAC: "AA"}, "hd", true, false)
	c.PublishDetectionDiscovery(cam)
	msg := recorder.last(t, "ha/binary_sensor/AA_motion/config")
	var entity struct {
		StateTopic       string `json:"state_topic"`
		AvailabilityMode string `json:"availability_mode"`
		Availability     []struct {
			Topic string `json:"topic"`
		} `json:"availability"`
	}
	if err := json.Unmarshal([]byte(msg.payload), &entity); err != nil {
		t.Fatal(err)
	}
	if entity.StateTopic != "test/renamed/motion" || entity.AvailabilityMode != "all" || len(entity.Availability) != 2 ||
		entity.Availability[0].Topic != "test/bridge/state" || entity.Availability[1].Topic != "test/bridge/motion_state" {
		t.Fatal("motion discovery used stale name or video-stream availability")
	}
	c.RemoveDiscovery("AA")
	if recorder.last(t, "ha/binary_sensor/AA_motion/config").payload != "" {
		t.Fatal("removed camera left motion discovery behind")
	}
	c.motionEnabled = false
	cam.SetMotion(true, time.Now())
	c.PublishDetectionDiscovery(cam)
	c.PublishCameraMotion(cam)
	if recorder.last(t, "ha/binary_sensor/AA_motion/config").payload != "" || recorder.last(t, "test/renamed/motion").payload != "2" {
		t.Fatal("disabling motion left a discoverable or active retained sensor")
	}
}

func TestSoundPublication_DiscoveryReconnectClearAndDisable(t *testing.T) {
	recorder := &motionRecordingClient{}
	cam := camera.NewCamera(wyzeapi.CameraInfo{Name: "front", MAC: "AA"}, "hd", true, false)
	mgr := camera.NewManager(&config.Config{}, nil, nil, zerolog.Nop())
	mgr.InjectCamera("front", cam)
	c := &Client{
		paho: recorder, camMgr: mgr, topic: "test", dtopic: "ha", soundEnabled: true,
		publishSem: make(chan struct{}, maxInflightPublishes), log: zerolog.Nop(),
	}
	cam.SetSound(true, time.Unix(1700000000, 0))
	c.PublishDetectionAvailability(true)
	c.onConnect()
	if recorder.last(t, "test/front/sound").payload != "1" || recorder.last(t, "test/front/motion").payload != "2" ||
		recorder.last(t, "test/bridge/sound_state").payload != "online" || recorder.last(t, "test/bridge/motion_state").payload != "offline" {
		t.Fatal("sound-only reconnect incorrectly published motion or availability")
	}
	var entity map[string]interface{}
	if err := json.Unmarshal([]byte(recorder.last(t, "ha/binary_sensor/AA_sound/config").payload), &entity); err != nil {
		t.Fatal(err)
	}
	if entity["device_class"] != "sound" || entity["state_topic"] != "test/front/sound" {
		t.Fatal("sound discovery was not separate")
	}
	cam.SetSound(false, time.Time{})
	c.PublishCameraSound(cam)
	if recorder.last(t, "test/front/sound").payload != "2" || recorder.last(t, "test/front/sound_ts").payload != "1700000000" {
		t.Fatal("sound clear lost its real event timestamp")
	}
	c.soundEnabled = false
	cam.SetSound(true, time.Now())
	c.PublishDetectionDiscovery(cam)
	c.PublishCameraSound(cam)
	if recorder.last(t, "ha/binary_sensor/AA_sound/config").payload != "" || recorder.last(t, "test/front/sound").payload != "2" {
		t.Fatal("disabled sound retained discovery or an active state")
	}
}

// TestPublishTopics verifies topic string construction for camera state publishing.
func TestPublishTopics(t *testing.T) {
	topic := "wyzebridge"
	camName := "front_door"

	tests := []struct {
		format string
		want   string
	}{
		{"%s/%s/state", "wyzebridge/front_door/state"},
		{"%s/%s/quality", "wyzebridge/front_door/quality"},
		{"%s/%s/audio", "wyzebridge/front_door/audio"},
		{"%s/%s/net_mode", "wyzebridge/front_door/net_mode"},
		{"%s/%s/camera_info", "wyzebridge/front_door/camera_info"},
		{"%s/%s/stream_info", "wyzebridge/front_door/stream_info"},
		{"%s/%s/thumbnail", "wyzebridge/front_door/thumbnail"},
		{"%s/bridge/state", "wyzebridge/bridge/state"},
	}

	for _, tt := range tests {
		var got string
		if tt.format == "%s/bridge/state" {
			got = fmt.Sprintf(tt.format, topic)
		} else {
			got = fmt.Sprintf(tt.format, topic, camName)
		}
		if got != tt.want {
			t.Errorf("topic = %q, want %q", got, tt.want)
		}
	}
}

func TestCameraInfoJSON(t *testing.T) {
	info := wyzeapi.CameraInfo{
		LanIP:     "192.168.1.10",
		Model:     "HL_CAM4",
		FWVersion: "4.52.9.4188",
		MAC:       "AABBCCDDEEFF",
	}

	cameraInfo := map[string]string{
		"ip":         info.LanIP,
		"model":      info.Model,
		"fw_version": info.FWVersion,
		"mac":        info.MAC,
	}
	data, err := json.Marshal(cameraInfo)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed map[string]string
	json.Unmarshal(data, &parsed)

	if parsed["ip"] != "192.168.1.10" {
		t.Errorf("ip = %q", parsed["ip"])
	}
	if parsed["mac"] != "AABBCCDDEEFF" {
		t.Errorf("mac = %q", parsed["mac"])
	}
}

func TestStreamInfoJSON(t *testing.T) {
	bridgeIP := "192.168.1.50"
	name := "front_door"

	streamInfo := map[string]string{
		"rtsp_url":   fmt.Sprintf("rtsp://%s:8554/%s", bridgeIP, name),
		"webrtc_url": fmt.Sprintf("http://%s:8889/%s", bridgeIP, name),
		"hls_url":    fmt.Sprintf("http://%s:1984/api/stream.m3u8?src=%s", bridgeIP, name),
	}
	data, err := json.Marshal(streamInfo)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed map[string]string
	json.Unmarshal(data, &parsed)

	if parsed["rtsp_url"] != "rtsp://192.168.1.50:8554/front_door" {
		t.Errorf("rtsp_url = %q", parsed["rtsp_url"])
	}
}

func TestCameraStateStrings(t *testing.T) {
	cam := camera.NewCamera(wyzeapi.CameraInfo{Name: "test"}, "hd", true, false)

	// Offline → "disconnected"
	state := cam.GetState()
	stateStr := "disconnected"
	if state == camera.StateStreaming {
		stateStr = "connected"
	}
	if stateStr != "disconnected" {
		t.Errorf("offline should be disconnected, got %q", stateStr)
	}

	// Streaming → "connected"
	cam.SetState(camera.StateStreaming)
	state = cam.GetState()
	stateStr = "disconnected"
	if state == camera.StateStreaming {
		stateStr = "connected"
	}
	if stateStr != "connected" {
		t.Errorf("streaming should be connected, got %q", stateStr)
	}
}
