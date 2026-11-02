package wyzeapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func motionTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c := NewClient(Credentials{}, "test", zerolog.Nop())
	c.CloudURL = server.URL + "/app"
	c.WyzeURL = server.URL + "/app"
	c.httpClient = server.Client()
	c.SetAuth(&AuthState{AccessToken: "test-token", RefreshToken: "test-refresh", PhoneID: "test-phone", ExpiresAt: time.Now().Add(time.Hour)})
	return c
}

func TestGetMotionEvents_SignedRequestAndAlarmTypes(t *testing.T) {
	c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/app/v4/device/get_event_list" {
			t.Errorf("unexpected endpoint: %s", r.URL.Path)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		expected := map[string]interface{}{
			"begin_time": float64(1000), "end_time": float64(2000),
			"count": float64(20), "order_by": float64(1),
			"device_id_list":   []interface{}{"AA", "BB"},
			"event_tag_list":   []interface{}{},
			"event_value_list": []interface{}{float64(1), float64(6), float64(7), float64(13)},
			"nonce":            payload["nonce"],
		}
		canonical, _ := json.Marshal(expected)
		if !bytes.Equal(body, canonical) || payload["nonce"] == nil {
			t.Errorf("unexpected signed request shape")
		}
		key := md5.Sum([]byte("test-token" + "wyze_app_secret_key_132"))
		mac := hmac.New(md5.New, []byte(hex.EncodeToString(key[:])))
		mac.Write(body)
		if r.Header.Get("signature2") != hex.EncodeToString(mac.Sum(nil)) {
			t.Error("signature does not match the exact request bytes")
		}
		if r.Header.Get("access_token") != "test-token" || r.Header.Get("phoneid") != "test-phone" || r.Header.Get("appid") != "9319141212m2ik" {
			t.Error("missing signed authentication headers")
		}
		fmt.Fprint(w, `{"code":1,"data":{"event_list":[
			{"event_id":9007199254740993,"device_id":"AA","event_ts":1700000000000,"event_value":"1"},
			{"event_id":"person","device_mac":"BB","event_ts":"1700000000001","event_value":6},
			{"event_id":"vehicle","device_id":"AA","event_ts":1700000000002,"event_value":7},
			{"event_id":"pet","device_id":"AA","event_ts":1700000000003,"event_value":13},
			{"event_id":"sound","device_id":"AA","event_ts":1700000000004,"event_value":2},
			{"event_id":"other","device_id":"AA","event_ts":1700000000005,"event_value":99}
		]}}`)
	})
	events, err := c.GetMotionEvents(context.Background(), []string{"BB", "AA", "AA"}, 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 || events[0].ID != "9007199254740993" || events[1].DeviceID != "BB" || events[0].Timestamp.UnixMilli() != 1700000000000 {
		t.Fatalf("incorrect motion filtering, identifiers, or timestamps: %+v", events)
	}
}

func TestGetMotionEvents_InvalidResponsesAndRateLimits(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		remaining  string
		wantRate   bool
	}{
		{"missing list", `{"code":1,"data":{}}`, 200, "", false},
		{"invalid list", `{"code":1,"data":{"event_list":[null]}}`, 200, "", false},
		{"missing type", `{"code":1,"data":{"event_list":[{}]}}`, 200, "", false},
		{"missing device", `{"code":1,"data":{"event_list":[{"event_id":"a","event_ts":1700000000000,"event_value":1}]}}`, 200, "", false},
		{"fractional timestamp", `{"code":1,"data":{"event_list":[{"event_id":"a","device_id":"AA","event_ts":1.5,"event_value":1}]}}`, 200, "", false},
		{"HTTP 429 without JSON", `rate limited`, 429, "", true},
		{"zero remaining", `{"code":1,"data":{"event_list":[]}}`, 200, "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.remaining != "" {
					w.Header().Set("X-RateLimit-Remaining", tc.remaining)
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			})
			_, err := c.GetMotionEvents(context.Background(), []string{"AA"}, 0, 1)
			if err == nil {
				t.Fatal("malformed or rate-limited response was accepted")
			}
			var rate *RateLimitError
			if errors.As(err, &rate) != tc.wantRate {
				t.Fatalf("incorrect error classification: %v", err)
			}
		})
	}
}

func TestGetMotionEvents_EmptyFeedAndCapacityHeader(t *testing.T) {
	c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "5")
		fmt.Fprint(w, `{"code":1,"data":{"event_list":[]}}`)
	})
	events, err := c.GetMotionEvents(context.Background(), []string{"AA"}, 0, 1)
	if err != nil || len(events) != 0 {
		t.Fatalf("empty feed is a valid result; capacity is not remaining calls: %v", err)
	}
}

func TestGetMotionEvents_RetryAfterAndPrivateEventLogging(t *testing.T) {
	t.Run("Retry-After", func(t *testing.T) {
		c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "180")
			w.WriteHeader(http.StatusTooManyRequests)
		})
		_, err := c.GetMotionEvents(context.Background(), []string{"AA"}, 0, 1)
		var rate *RateLimitError
		if !errors.As(err, &rate) || rate.RetryAfter != 3*time.Minute {
			t.Fatalf("lost retry interval: %v", err)
		}
	})
	t.Run("event media not logged", func(t *testing.T) {
		c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"code":1,"data":{"event_list":[{"event_id":"a","device_id":"AA","event_ts":1700000000000,"event_value":1,"file_list":[{"url":"https://example.invalid/private-event-media"}]}]}}`)
		})
		var output bytes.Buffer
		c.log = zerolog.New(&output).Level(zerolog.TraceLevel)
		if _, err := c.GetMotionEvents(context.Background(), []string{"AA"}, 0, 1); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output.String(), "private-event-media") || strings.Contains(output.String(), "test-token") {
			t.Fatal("motion polling logged private media or authentication")
		}
	})
}

func TestGetMotionEvents_Cancellation(t *testing.T) {
	started := make(chan struct{})
	c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.GetMotionEvents(ctx, []string{"AA"}, 0, 1)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation was not preserved: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("event request was not canceled")
	}
}

func TestGetMotionEvents_RefreshesRejectedTokenOnce(t *testing.T) {
	requests := 0
	c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/user/refresh_token") {
			fmt.Fprint(w, `{"code":1,"data":{"access_token":"renewed","refresh_token":"new-refresh"}}`)
			return
		}
		requests++
		if requests == 1 {
			fmt.Fprint(w, `{"code":2001,"msg":"expired"}`)
			return
		}
		if r.Header.Get("access_token") != "renewed" {
			t.Error("retry did not use refreshed authentication")
		}
		fmt.Fprint(w, `{"code":1,"data":{"event_list":[]}}`)
	})
	if _, err := c.GetMotionEvents(context.Background(), []string{"AA"}, 0, 1); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("expected one retry, got %d event requests", requests)
	}
}

func TestConcurrentAuthSnapshotsForMotionSigning(t *testing.T) {
	c := NewClient(Credentials{}, "test", zerolog.Nop())
	c.SetAuth(&AuthState{PhoneID: "a", AccessToken: "a"})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			token := fmt.Sprint(i)
			c.SetAuth(&AuthState{PhoneID: token, AccessToken: token})
		}
	})
	wg.Go(func() {
		for range 1000 {
			headers := c.signPayloadHeaders("9319141212m2ik", "{}")
			if headers["phoneid"] != headers["access_token"] {
				t.Error("request combined two different auth states")
			}
			snapshot := c.Auth()
			snapshot.AccessToken = "caller-owned-copy"
		}
	})
	wg.Wait()
}

func TestGetDetectionEvents_MotionAndSoundFilters(t *testing.T) {
	for _, tc := range []struct {
		name          string
		motion, sound bool
		count         int
	}{
		{"motion only", true, false, 1},
		{"sound only", false, true, 1},
		{"both", true, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			c := motionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				var request struct {
					Values []int `json:"event_value_list"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				hasSound := false
				for _, value := range request.Values {
					if value == 2 {
						hasSound = true
					}
				}
				if hasSound != tc.sound {
					t.Error("incorrect server-side sound filter")
				}
				fmt.Fprint(w, `{"code":1,"data":{"event_list":[
					{"event_id":"motion","device_id":"AA","event_ts":1700000000000,"event_value":1},
					{"event_id":"sound","device_id":"AA","event_ts":1700000000001,"event_value":"2"},
					{"event_id":"smoke","device_id":"AA","event_ts":1700000000002,"event_value":4}
				]}}`)
			})
			events, err := c.GetDetectionEvents(context.Background(), []string{"AA"}, 0, 1, tc.motion, tc.sound)
			if err != nil || len(events) != tc.count || requests != 1 {
				t.Fatalf("incorrect combined fetch: %v, %+v", err, events)
			}
			for _, event := range events {
				if event.ID == "sound" && event.Kind != SoundDetection {
					t.Fatal("sound was classified as motion")
				}
				if event.ID == "motion" && event.Kind != MotionDetection {
					t.Fatal("motion was classified as sound")
				}
			}
		})
	}
}
