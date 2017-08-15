package main

import (
	"encoding/json"
	"testing"
	"time"

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
			// TODO: Figure out why dimensions aren't getting written...
			Dimensions: map[string]string{
				"district":    "ddd",
				"title":       "login_start",
				"auth_method": "auth",
				"hostname":    "my-hostname",
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
		"hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{}, // TODO
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
				"hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewIntValue(123),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{}, // TODO
		},
	}

	pts := []datapoint.Datapoint{}
	err = json.Unmarshal(output, &pts)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, pts)
}

func TestEncodeMessageWithGauge(t *testing.T) {
	t.Log("writes a single Gauge as a datapoint")
	consumer := AlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(9.5),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{}, // TODO
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
				"hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{}, // TODO
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
		"hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Time{}, // TODO
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
				"hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{}, // TODO
		},
		datapoint.Datapoint{
			Metric: "series-name-2",
			Dimensions: map[string]string{
				"dim_a":    "dim_a_val",
				"dim_b":    "dim_b_val",
				"hostname": "my-hostname",
				"env":      "my-env",
			},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{}, // TODO
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

// TODO
// func TestSendBatch(t *testing.T) {
// 	input1 := []byte(`todo`)
// 	input2 := []byte(`todo2`)
// 	batchedInput := [][]byte{input1, input2}

// 	consumer := AlertsConsumer{}

// 	t.Log("verify can SendBatch to 'tag'")
// 	err := consumer.SendBatch(batchedInput, "tag")
// 	assert.NoError(t, err)
// }
