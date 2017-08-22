package main

import (
	"encoding/json"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/signalfx/golib/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestProcessMessage(t *testing.T) {
	consumer := AlertsConsumer{
		deployEnv: "test-env",
	}
	rawmsg := `2017-08-15T18:39:07.000000+00:00 my-hostname production--my-app/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fbe5eafc1-8e44-489a-8942-aaaaaaaaaaaa[3337]: {"level":"info","source":"oauth","title":"login_start","action":"login","type":"counter","session_id":"sss","auth_method":"auth","district":"ddd","client_id":"ccc","app_id":"aaa","request_id":"","_kvmeta":{"team":"eng-team","kv_version":"3.8.2","kv_language":"js","routes":[{"type":"analytics","series":"series-name","rule":"login-events"},{"type":"alerts","series":"oauth.login_start","dimensions":["district","title","auth_method"],"stat_type":"counter","value_field":"value","rule":"login-start"}]}}`
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.NoError(t, err)

	expectedTags := []string{"default"}
	assert.Equal(t, expectedTags, tags)

	// Verify the message
	t.Log("verify the message")
	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(msg, &pts)
	assert.NoError(t, err)

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "oauth.login_start",
			Dimensions: map[string]string{
				"district":    "ddd",
				"title":       "login_start",
				"auth_method": "auth",
				"Hostname":    "my-hostname",
				"env":         "test-env",
			},
			Value:      datapoint.NewIntValue(1),
			MetricType: datapoint.Counter,
			Timestamp:  time.Unix(1502822347, 0).UTC(),
		},
	}

	assert.Equal(t, expectedPts, pts)
}

// TestEncodeMessage tests the encodeMessage() helper used in ProcessMessage()
func TestEncodeMessage(t *testing.T) {
	t.Log("writes a single Counter as a datapoint")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(123),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{"dim_a", "dim_b"},
					"stat_type":   "counter",
					"value_field": "value",
					"rule":        "rule-1",
				},
			},
		},
	}

	output, tags, err := consumer.encodeMessage(input)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewIntValue(123),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
		},
	}

	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(output, &pts)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, pts)
}

func TestEncodeMessageWithNonStringDimensions(t *testing.T) {
	t.Log("is able to write SFX dimensions from JSON fields which have float or bool values")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(123),
		"dim_a":     "dim_a_val",
		"dim_float": float64(3.2),
		"dim_bool":  true,
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{"dim_a", "dim_float", "dim_bool"},
					"stat_type":   "counter",
					"value_field": "value",
					"rule":        "rule-1",
				},
			},
		},
	}

	output, tags, err := consumer.encodeMessage(input)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":     "dim_a_val",
				"dim_float": "3",
				"dim_bool":  "true",
				"Hostname":  "my-hostname",
				"env":       "my-env",
			},
			Value:      datapoint.NewIntValue(123),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
		},
	}

	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(output, &pts)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, pts)
}

func TestEncodeMessageErrorsIfInvalidDimensionType(t *testing.T) {
	t.Log("message error if trying to cast unknown type as SFX dimension")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(123),
		"dim_error": struct{}{}, // invalid type
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{"dim_error"},
					"stat_type":   "counter",
					"value_field": "value",
					"rule":        "rule-1",
				},
			},
		},
	}

	_, _, err := consumer.encodeMessage(input)
	assert.Error(t, err)
	assert.EqualError(t, err, "error casting dimension value. rule=rule-1 dim=dim_error val={}")
}

func TestEncodeMessageErrorsIfValueExistsAndIsInvalidType(t *testing.T) {
	t.Log("message error if trying to cast unknown type as SFX dimension")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     "12345", //should fail, even though it's numeric its not the right type
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{},
					"stat_type":   "counter",
					"value_field": "value",
					"rule":        "rule-1",
				},
			},
		},
	}

	_, _, err := consumer.encodeMessage(input)
	assert.Error(t, err)
	assert.EqualError(t, err, "value exists but is wrong type. rule=rule-1 value_field=value value=12345")
}

func TestEncodeMessageWithGauge(t *testing.T) {
	t.Log("writes a single Gauge as a datapoint")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(9.5),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{"dim_a", "dim_b"},
					"stat_type":   "gauge",
					"value_field": "value",
					"rule":        "rule-1",
				},
			},
		},
	}

	output, tags, err := consumer.encodeMessage(input)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(output, &pts)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Value, pts[0].Value)
	assert.Equal(t, expectedPts, pts)
}

func TestEncodeMessageWithMultipleRoutes(t *testing.T) {
	t.Log("If message has multiple routes, it will write multiple datapoints")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(9.5),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{},
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name",
					"dimensions":  []interface{}{"dim_a", "dim_b"},
					"stat_type":   "gauge",
					"value_field": "value",
					"rule":        "rule-1",
				},
				map[string]interface{}{
					"type":        "alerts",
					"series":      "series-name-2",
					"dimensions":  []interface{}{"dim_a", "dim_b"},
					"stat_type":   "gauge",
					"value_field": "value",
					"rule":        "rule-2",
				},
			},
		},
	}

	output, tags, err := consumer.encodeMessage(input)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
		datapoint.Datapoint{
			Metric: "series-name-2",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(output, &pts)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Value, pts[0].Value)
	assert.Equal(t, expectedPts, pts)
}

func TestEncodeMessageWithNoAlertsRoutes(t *testing.T) {
	t.Log("If message has no Alerts routes, it will write 0 datapoints")
	consumer := AlertsConsumer{}

	// Not an alert
	input := map[string]interface{}{
		"rawlog": "...",
		"_kvmeta": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"type":    "metric",
					"channel": "#test",
					"message": "Hello World",
					"user":    "testbot",
					"icon":    ":bot:",
				},
			},
		},
	}
	_, _, err := consumer.encodeMessage(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "intentionally skipped")
}

type MockSink struct {
	pts []datapoint.Datapoint
}

func (s *MockSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	for _, p := range points {
		s.pts = append(s.pts, *p)
	}
	return nil
}

func TestSendBatch(t *testing.T) {
	pts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		}, datapoint.Datapoint{
			Metric: "series-name-2",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}
	pts2 := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name-3",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		}, datapoint.Datapoint{
			Metric:     "series-name-4",
			Dimensions: map[string]string{"dim_a": "dim_a_val"},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	b, err := json.Marshal(pts)
	assert.NoError(t, err)
	input := [][]byte{b}

	b2, err := json.Marshal(pts2)
	assert.NoError(t, err)
	input2 := [][]byte{b2}

	mockSink := &MockSink{}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
	}
	t.Log("Send first batch")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts), mockSink.pts)

	t.Log("Send second batch")
	err = consumer.SendBatch(input2, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts, pts2...), mockSink.pts)
}

func TestSendBatchWithMultipleEntries(t *testing.T) {
	pts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		}, datapoint.Datapoint{
			Metric: "series-name-2",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}
	pts2 := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name-3",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		}, datapoint.Datapoint{
			Metric:     "series-name-4",
			Dimensions: map[string]string{"dim_a": "dim_a_val"},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	b, err := json.Marshal(pts)
	assert.NoError(t, err)

	b2, err := json.Marshal(pts2)
	assert.NoError(t, err)

	input := [][]byte{b, b2}

	mockSink := &MockSink{}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
	}
	t.Log("Send batch with multiple entries")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts, pts2...), mockSink.pts)
}

func TestSendBatchResetsTimeForRecentDatapoints(t *testing.T) {
	now := time.Now().UTC()
	oneMinuteAgo := now.Add(-1 * time.Minute)

	pts := []datapoint.Datapoint{
		datapoint.Datapoint{
			Metric: "series-name",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  now, // This should be reset to time.Time{}
		}, datapoint.Datapoint{
			Metric: "series-name-2",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"Hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  oneMinuteAgo, // This time should not be modified
		},
	}

	b, err := json.Marshal(pts)
	assert.NoError(t, err)
	input := [][]byte{b}

	mockSink := &MockSink{}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
	}
	t.Log("Send first batch")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)

	t.Log("Datapoints with recent timestamps should ignore their timestamp and instead get a timestamp on arrival to SignalFX")
	pts[0].Timestamp = time.Time{}
	assert.Equal(t, pts, mockSink.pts)
}
