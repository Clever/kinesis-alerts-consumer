package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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
	eo := EncodeOutput{}
	err = json.Unmarshal(msg, &eo)
	assert.NoError(t, err)

	expectedPts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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

	assert.Equal(t, expectedPts, eo.Datapoints)
}

func TestProcessMessageSupportsAppLifecycleEvents(t *testing.T) {
	consumer := AlertsConsumer{
		deployEnv: "test-env",
	}
	rawmsg := `2017-08-15T18:39:07.000000+00:00 my-hostname production--my-app/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fbe5eafc1-8e44-489a-8942-aaaaaaaaaaaa[3337]: {"title":"app_deploying","category":"app_lifecycle"}`
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.NoError(t, err)

	expectedTags := []string{"default"}
	assert.Equal(t, expectedTags, tags)

	// Verify the message
	t.Log("verify the message")
	eo := EncodeOutput{}
	err = json.Unmarshal(msg, &eo)
	assert.NoError(t, err)

	expected := EncodeOutput{
		Datapoints: []*datapoint.Datapoint{},
		Events: []*event.Event{
			&event.Event{
				EventType: "app_lifecycle",
				Category:  event.USERDEFINED,
				Dimensions: map[string]string{
					"container_app": "my-app",
					"container_env": "production",
					"title":         "app_deploying",
				},
				Properties: map[string]interface{}{},
				Timestamp:  time.Unix(1502822347, 0).UTC(),
			},
		},
		MetricData: []*cloudwatch.MetricDatum{},
	}

	assert.Equal(t, expected, eo)
}

func TestProcessMessageSupportsCloudwatch(t *testing.T) {
	consumer := AlertsConsumer{
		deployEnv: "test-env",
	}
	rawmsg := `2017-08-15T18:39:07.000000+00:00 my-hostname production--my-app/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fbe5eafc1-8e44-489a-8942-aaaaaaaaaaaa[3337]: {"_kvmeta":{"kv_language":"go","kv_version":"6.16.0","routes":[{"dimensions":["dimension1"],"rule":"unexpected-stop","series":"ContainerExitCount","stat_type":"counter","type":"alerts","value_field":"value"}],"team":"eng-infra"},"category":"app_lifecycle","level":"info","title":"title","dimension1":"dim","region":"reg","type":"counter","value":1}`
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.NoError(t, err)

	expectedTags := []string{"reg"}
	assert.Equal(t, expectedTags, tags)

	// Verify the message
	t.Log("verify the message")
	eo := EncodeOutput{}
	err = json.Unmarshal(msg, &eo)
	assert.NoError(t, err)

	timestamp, _ := time.Parse(time.RFC3339Nano, "2017-08-15T18:39:07.000000Z")
	expected := EncodeOutput{
		Datapoints: []*datapoint.Datapoint{
			&datapoint.Datapoint{
				Metric: "ContainerExitCount",
				Dimensions: map[string]string{
					"Hostname":   "my-hostname",
					"env":        "test-env",
					"dimension1": "dim",
				},
				Value:      datapoint.NewIntValue(1),
				MetricType: datapoint.Counter,
				Timestamp:  timestamp,
			},
		},
		Events: []*event.Event{},
		MetricData: []*cloudwatch.MetricDatum{
			&cloudwatch.MetricDatum{
				Dimensions: []*cloudwatch.Dimension{
					&cloudwatch.Dimension{
						Name:  aws.String("dimension1"),
						Value: aws.String("dim"),
					},
				},
				MetricName:        aws.String("ContainerExitCount"),
				Timestamp:         aws.Time(timestamp),
				Value:             aws.Float64(1),
				StorageResolution: aws.Int64(1),
			},
		},
	}

	assert.Equal(t, expected, eo)
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

	output, tags, err := consumer.encodeMessage(input, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, eo.Datapoints)
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

	output, tags, err := consumer.encodeMessage(input, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, eo.Datapoints)
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

	_, _, err := consumer.encodeMessage(input, 0)
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

	_, _, err := consumer.encodeMessage(input, 0)
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

	output, tags, err := consumer.encodeMessage(input, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Value, eo.Datapoints[0].Value)
	assert.Equal(t, expectedPts, eo.Datapoints)
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

	output, tags, err := consumer.encodeMessage(input, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, string("default"), tags[0])

	expectedPts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		&datapoint.Datapoint{
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

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Value, eo.Datapoints[0].Value)
	assert.Equal(t, expectedPts, eo.Datapoints)
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
	_, _, err := consumer.encodeMessage(input, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "intentionally skipped")
}

type MockHTTPSink struct {
	pts  []*datapoint.Datapoint
	evts []*event.Event
}

func (s *MockHTTPSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	for _, p := range points {
		s.pts = append(s.pts, p)
	}
	return nil
}

func (s *MockHTTPSink) AddEvents(ctx context.Context, events []*event.Event) (err error) {
	for _, e := range events {
		s.evts = append(s.evts, e)
	}
	return nil
}

type MockCW struct {
	cloudwatchiface.CloudWatchAPI
	inputs []*cloudwatch.PutMetricDataInput
}

func (cw *MockCW) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	cw.inputs = append(cw.inputs, input)
	return nil, nil
}

func TestSendBatch(t *testing.T) {
	pts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		&datapoint.Datapoint{
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
	pts2 := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		},
		&datapoint.Datapoint{
			Metric:     "series-name-4",
			Dimensions: map[string]string{"dim_a": "dim_a_val"},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	b, err := json.Marshal(EncodeOutput{
		Datapoints: pts,
	})
	assert.NoError(t, err)
	input := [][]byte{b}

	b2, err := json.Marshal(EncodeOutput{
		Datapoints: pts2,
	})
	assert.NoError(t, err)
	input2 := [][]byte{b2}

	mockSink := &MockHTTPSink{}
	mockCWUSWest1 := &MockCW{}
	mockCWs := map[string]cloudwatchiface.CloudWatchAPI{
		"us-west-1": mockCWUSWest1,
	}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
		cwAPIs:  mockCWs,
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

func TestSendBatchToCloudwatch(t *testing.T) {
	dats := []*cloudwatch.MetricDatum{
		&cloudwatch.MetricDatum{
			Dimensions: []*cloudwatch.Dimension{
				&cloudwatch.Dimension{
					Name:  aws.String("Hostname"),
					Value: aws.String("my-hostname"),
				},
				&cloudwatch.Dimension{
					Name:  aws.String("env"),
					Value: aws.String("test-env"),
				},
			},
			MetricName: aws.String("series-1"),
			Value:      aws.Float64(1),
		},
		&cloudwatch.MetricDatum{
			Dimensions: []*cloudwatch.Dimension{
				&cloudwatch.Dimension{
					Name:  aws.String("Hostname"),
					Value: aws.String("my-hostname"),
				},
				&cloudwatch.Dimension{
					Name:  aws.String("env"),
					Value: aws.String("test-env"),
				},
			},
			MetricName: aws.String("series-2"),
			Value:      aws.Float64(1),
		},
	}

	expected := []*cloudwatch.PutMetricDataInput{
		&cloudwatch.PutMetricDataInput{
			Namespace: aws.String("LogMetrics"),
			MetricData: []*cloudwatch.MetricDatum{
				&cloudwatch.MetricDatum{
					Dimensions: []*cloudwatch.Dimension{
						&cloudwatch.Dimension{
							Name:  aws.String("Hostname"),
							Value: aws.String("my-hostname"),
						},
						&cloudwatch.Dimension{
							Name:  aws.String("env"),
							Value: aws.String("test-env"),
						},
					},
					MetricName: aws.String("series-1"),
					Value:      aws.Float64(1),
				},
				&cloudwatch.MetricDatum{
					Dimensions: []*cloudwatch.Dimension{
						&cloudwatch.Dimension{
							Name:  aws.String("Hostname"),
							Value: aws.String("my-hostname"),
						},
						&cloudwatch.Dimension{
							Name:  aws.String("env"),
							Value: aws.String("test-env"),
						},
					},
					MetricName: aws.String("series-2"),
					Value:      aws.Float64(1),
				},
			},
		},
	}

	b, err := json.Marshal(EncodeOutput{
		MetricData: dats,
	})
	assert.NoError(t, err)
	input := [][]byte{b}

	mockSink := &MockHTTPSink{}
	mockCWUSWest1 := &MockCW{}
	mockCWs := map[string]cloudwatchiface.CloudWatchAPI{
		"us-west-1": mockCWUSWest1,
	}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
		cwAPIs:  mockCWs,
	}
	t.Log("Send batch")
	err = consumer.SendBatch(input, "us-west-1")
	assert.NoError(t, err)
	assert.Equal(t, expected, mockCWUSWest1.inputs)
}

func TestSendBatchWithMultipleEntries(t *testing.T) {
	pts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		&datapoint.Datapoint{
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
	pts2 := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		},
		&datapoint.Datapoint{
			Metric:     "series-name-4",
			Dimensions: map[string]string{"dim_a": "dim_a_val"},
			Value:      datapoint.NewFloatValue(float64(9.5)),
			MetricType: datapoint.Gauge,
			Timestamp:  time.Time{},
		},
	}

	b, err := json.Marshal(EncodeOutput{
		Datapoints: pts,
	})
	assert.NoError(t, err)

	b2, err := json.Marshal(EncodeOutput{
		Datapoints: pts2,
	})
	assert.NoError(t, err)

	input := [][]byte{b, b2}

	mockSink := &MockHTTPSink{}
	mockCWUSWest1 := MockCW{}
	mockCWs := map[string]cloudwatchiface.CloudWatchAPI{
		"us-west-1": &mockCWUSWest1,
	}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
		cwAPIs:  mockCWs,
	}
	t.Log("Send batch with multiple entries")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts, pts2...), mockSink.pts)
}

func TestSendBatchResetsTimeForRecentDatapoints(t *testing.T) {
	now := time.Now().UTC()
	oneMinuteAgo := now.Add(-1 * time.Minute)

	pts := []*datapoint.Datapoint{
		&datapoint.Datapoint{
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
		},
		&datapoint.Datapoint{
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

	b, err := json.Marshal(EncodeOutput{
		Datapoints: pts,
	})
	assert.NoError(t, err)
	input := [][]byte{b}

	mockSink := &MockHTTPSink{}
	mockCWUSWest1 := MockCW{}
	mockCWs := map[string]cloudwatchiface.CloudWatchAPI{
		"us-west-1": &mockCWUSWest1,
	}
	consumer := AlertsConsumer{
		sfxSink: mockSink,
		cwAPIs:  mockCWs,
	}
	t.Log("Send first batch")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)

	t.Log("Datapoints with recent timestamps should ignore their timestamp and instead get a timestamp on arrival to SignalFX")
	pts[0].Timestamp = time.Time{}
	assert.Equal(t, pts, mockSink.pts)
}
