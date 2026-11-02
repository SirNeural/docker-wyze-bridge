package mqtt

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/IDisposable/docker-wyze-bridge/internal/camera"
)

// PublishCameraState publishes the current state for a camera.
func (c *Client) PublishCameraState(cam *camera.Camera) {
	name := cam.Name()
	state := cam.GetState()

	stateStr := "disconnected"
	powerStr := "off"
	if state == camera.StateStreaming {
		stateStr = "connected"
		powerStr = "on"
	}

	c.publish(fmt.Sprintf("%s/%s/state", c.topic, name), stateStr, true)
	c.publish(fmt.Sprintf("%s/%s/power", c.topic, name), powerStr, true)
	c.publish(fmt.Sprintf("%s/%s/net_mode", c.topic, name), "lan", true)
	c.publish(fmt.Sprintf("%s/%s/quality", c.topic, name), cam.GetQuality(), true)

	audioStr := "false"
	if cam.GetAudioOn() {
		audioStr = "true"
	}
	c.publish(fmt.Sprintf("%s/%s/audio", c.topic, name), audioStr, true)
	c.PublishCameraMotion(cam)
	c.PublishCameraSound(cam)
}

// PublishCameraMotion also clears retained signals when motion is disabled.
// Serialize capture/publication against reconnect and camera-state publishers.
func (c *Client) PublishCameraMotion(cam *camera.Camera) {
	c.publishCameraDetection(cam, false)
}

func (c *Client) PublishCameraSound(cam *camera.Camera) {
	c.publishCameraDetection(cam, true)
}

func (c *Client) publishCameraDetection(cam *camera.Camera, sound bool) {
	c.detectionPublishMu.Lock()
	defer c.detectionPublishMu.Unlock()
	snap := cam.Snapshot()
	kind, enabled, detected, last := "motion", c.motionEnabled, snap.MotionDetected, snap.MotionTimestamp
	if sound {
		kind, enabled, detected, last = "sound", c.soundEnabled, snap.SoundDetected, snap.SoundTimestamp
	}
	value := "2"
	var timestamp int64
	if enabled {
		if detected {
			value = "1"
		}
		if !last.IsZero() {
			timestamp = last.Unix()
		}
	}
	base := fmt.Sprintf("%s/%s/%s", c.topic, snap.Info.Name, kind)
	c.publish(base, value, true)
	c.publish(base+"_ts", strconv.FormatInt(timestamp, 10), true)
}

func (c *Client) PublishDetectionAvailability(healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.detectionHealthy == healthy {
		return
	}
	c.detectionHealthy = healthy
	c.publishDetectionAvailabilityLocked()
}

func (c *Client) publishDetectionAvailabilityLocked() {
	for _, kind := range []struct {
		name    string
		enabled bool
	}{
		{"motion", c.motionEnabled}, {"sound", c.soundEnabled},
	} {
		value := "offline"
		if kind.enabled && c.detectionHealthy {
			value = "online"
		}
		c.publish(fmt.Sprintf("%s/bridge/%s_state", c.topic, kind.name), value, true)
	}
}

// PublishCameraInfo publishes static camera information.
func (c *Client) PublishCameraInfo(cam *camera.Camera) {
	name := cam.Name()
	info := cam.GetInfo()

	cameraInfo := map[string]string{
		"ip":         info.LanIP,
		"model":      info.Model,
		"fw_version": info.FWVersion,
		"mac":        info.MAC,
	}
	data, _ := json.Marshal(cameraInfo)
	c.publish(fmt.Sprintf("%s/%s/camera_info", c.topic, name), string(data), true)

	streamInfo := map[string]string{
		"rtsp_url":   fmt.Sprintf("rtsp://%s:8554/%s", c.bridgeIP, name),
		"webrtc_url": fmt.Sprintf("http://%s:8889/%s", c.bridgeIP, name),
		"hls_url":    fmt.Sprintf("http://%s:1984/api/stream.m3u8?src=%s", c.bridgeIP, name),
	}
	data, _ = json.Marshal(streamInfo)
	c.publish(fmt.Sprintf("%s/%s/stream_info", c.topic, name), string(data), true)
}

// PublishThumbnail publishes a JPEG snapshot to the camera's thumbnail topic.
func (c *Client) PublishThumbnail(camName string, jpeg []byte) {
	c.publishBytes(fmt.Sprintf("%s/%s/thumbnail", c.topic, camName), jpeg, true)
}

// PublishBridgeState publishes the bridge online/offline state.
func (c *Client) PublishBridgeState(online bool) {
	state := "offline"
	if online {
		state = "online"
	}
	c.publish(fmt.Sprintf("%s/bridge/state", c.topic), state, true)
}
