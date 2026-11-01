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
}

// PublishCameraMotion also clears retained signals when motion is disabled.
// Serialize capture/publication against reconnect and camera-state publishers.
func (c *Client) PublishCameraMotion(cam *camera.Camera) {
	c.motionPublishMu.Lock()
	defer c.motionPublishMu.Unlock()
	snap := cam.Snapshot()
	value := "2"
	var timestamp int64
	if c.motionEnabled {
		if snap.MotionDetected {
			value = "1"
		}
		if !snap.MotionTimestamp.IsZero() {
			timestamp = snap.MotionTimestamp.Unix()
		}
	}
	base := fmt.Sprintf("%s/%s", c.topic, snap.Info.Name)
	c.publish(base+"/motion", value, true)
	c.publish(base+"/motion_ts", strconv.FormatInt(timestamp, 10), true)
}

func (c *Client) PublishMotionAvailability(healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.motionHealthy == healthy {
		return
	}
	c.motionHealthy = healthy
	c.publishMotionAvailabilityLocked()
}

func (c *Client) publishMotionAvailabilityLocked() {
	value := "offline"
	if c.motionEnabled && c.motionHealthy {
		value = "online"
	}
	c.publish(fmt.Sprintf("%s/bridge/motion_state", c.topic), value, true)
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
