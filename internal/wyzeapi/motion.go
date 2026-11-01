package wyzeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

type DetectionKind uint8

const (
	MotionDetection DetectionKind = iota
	SoundDetection
)

// DetectionEvent is a camera-generated detection delivered by Wyze's
// cloud event feed. It contains no thumbnails, recordings, or signed URLs.
type DetectionEvent struct {
	ID        string
	DeviceID  string
	Timestamp time.Time
	Kind      DetectionKind
}

type MotionEvent = DetectionEvent

// GetMotionEvents requests only Wyze motion alarm types, not sound, smoke,
// doorbell, or the motion-detection enabled/disabled setting.
func (c *Client) GetMotionEvents(ctx context.Context, macs []string, beginMS, endMS int64) ([]MotionEvent, error) {
	return c.GetDetectionEvents(ctx, macs, beginMS, endMS, true, false)
}

func (c *Client) GetDetectionEvents(ctx context.Context, macs []string, beginMS, endMS int64, motion, sound bool) ([]DetectionEvent, error) {
	values := make([]int, 0, 5)
	if motion {
		values = append(values, 1, 6, 7, 13)
	}
	if sound {
		values = append(values, 2)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("at least one detection type must be enabled")
	}
	raw, err := c.getEventList(ctx, macs, beginMS, endMS, values)
	if err != nil {
		return nil, err
	}
	events := make([]DetectionEvent, 0, len(raw))
	for i, item := range raw {
		value, err := eventInteger(item["event_value"])
		if err != nil {
			return nil, fmt.Errorf("detection event %d: invalid event_value: %w", i, err)
		}
		kind := MotionDetection
		switch value {
		case 1, 6, 7, 13:
			if !motion {
				continue
			}
		case 2:
			if !sound {
				continue
			}
			kind = SoundDetection
		default:
			continue
		}
		id, err := eventID(item["event_id"])
		if err != nil {
			return nil, fmt.Errorf("detection event %d: invalid event_id: %w", i, err)
		}
		deviceID, _ := item["device_id"].(string)
		if deviceID == "" {
			deviceID, _ = item["device_mac"].(string)
		}
		ts, err := eventInteger(item["event_ts"])
		if deviceID == "" || err != nil || ts <= 0 {
			return nil, fmt.Errorf("detection event %d: missing device ID or invalid event_ts", i)
		}
		events = append(events, DetectionEvent{ID: id, DeviceID: deviceID, Timestamp: time.UnixMilli(ts), Kind: kind})
	}
	return events, nil
}

func eventID(value interface{}) (string, error) {
	if text, ok := value.(string); ok && text != "" {
		return text, nil
	}
	n, err := eventInteger(value)
	if err != nil || n <= 0 {
		return "", fmt.Errorf("expected a nonempty string or positive integer")
	}
	return strconv.FormatInt(n, 10), nil
}

func eventInteger(value interface{}) (int64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case json.Number:
		return v.Int64()
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v || math.Abs(v) > 1<<53 {
			return 0, fmt.Errorf("expected an exactly representable integer")
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("expected an integer")
	}
}
