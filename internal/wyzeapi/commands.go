package wyzeapi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// RunAction sends a run_action command to a camera via the Wyze cloud API.
func (c *Client) RunAction(cam CameraInfo, action string) error {
	if err := c.EnsureAuth(); err != nil {
		return err
	}

	payload := c.authenticatedPayload("run_action")
	payload["action_params"] = map[string]interface{}{}
	payload["action_key"] = action
	payload["instance_id"] = cam.MAC
	payload["provider_key"] = cam.Model
	payload["custom_string"] = ""

	_, err := c.postJSON(c.WyzeURL+"/v2/auto/run_action", c.defaultHeaders(), payload)
	if err != nil {
		return fmt.Errorf("run_action: %w", err)
	}
	return nil
}

// GetEventList fetches recent events for the given MAC addresses.
func (c *Client) GetEventList(macs []string, beginTimeMS, endTimeMS int64) ([]map[string]interface{}, error) {
	return c.getEventList(context.Background(), macs, beginTimeMS, endTimeMS, nil)
}

func (c *Client) getEventList(ctx context.Context, macs []string, beginTimeMS, endTimeMS int64, values []int) ([]map[string]interface{}, error) {
	if len(macs) == 0 || beginTimeMS < 0 || endTimeMS < beginTimeMS {
		return nil, fmt.Errorf("get_event_list: camera IDs and a valid time window are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.EnsureAuth(); err != nil {
		return nil, err
	}

	payload := make(map[string]interface{})

	// Deduplicate MACs
	macSet := make(map[string]bool)
	for _, m := range macs {
		macSet[m] = true
	}
	uniqueMACs := make([]string, 0, len(macSet))
	for m := range macSet {
		uniqueMACs = append(uniqueMACs, m)
	}
	sort.Strings(uniqueMACs)

	payload["count"] = 20
	payload["order_by"] = 1
	payload["begin_time"] = beginTimeMS
	payload["end_time"] = endTimeMS
	payload["device_id_list"] = uniqueMACs
	if values == nil {
		values = []int{}
	}
	payload["nonce"] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	payload["event_value_list"] = values
	payload["event_tag_list"] = []interface{}{}

	sorted := sortDict(payload)
	headers := c.signPayloadHeaders("9319141212m2ik", sorted)

	resp, err := c.postRawContext(ctx, c.CloudURL+"/v4/device/get_event_list", headers, sorted)
	var apiErr *WyzeAPIError
	if errors.As(err, &apiErr) && apiErr.AccessTokenExpired() {
		c.ensureAuthMu.Lock()
		_, refreshErr := c.RefreshToken()
		c.ensureAuthMu.Unlock()
		if refreshErr != nil {
			return nil, fmt.Errorf("get_event_list: refresh expired token: %w", refreshErr)
		}
		headers = c.signPayloadHeaders("9319141212m2ik", sorted)
		resp, err = c.postRawContext(ctx, c.CloudURL+"/v4/device/get_event_list", headers, sorted)
	}
	if err != nil {
		return nil, fmt.Errorf("get_event_list: %w", err)
	}

	eventList, ok := resp["event_list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("get_event_list: missing or invalid event_list")
	}

	var result []map[string]interface{}
	for i, e := range eventList {
		if m, ok := e.(map[string]interface{}); ok {
			result = append(result, m)
		} else {
			return nil, fmt.Errorf("get_event_list: invalid event at index %d", i)
		}
	}
	return result, nil
}
