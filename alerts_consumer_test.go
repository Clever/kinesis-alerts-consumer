package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/kayvee-go/v7/logger"
	datadog "github.com/DataDog/datadog-api-client-go/api/v2/datadog"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/stretchr/testify/assert"
)

// Add this interface for mocking
// This matches the v2 SDK's PutMetricData signature
// Both *cloudwatch.Client and MockCW will implement this

type CloudWatchPutMetricDataAPI interface {
	PutMetricData(ctx context.Context, params *cloudwatch.PutMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// TestAlertsConsumer overrides the cwAPIs field for testing
type TestAlertsConsumer struct {
	AlertsConsumer
	cwAPIs map[string]CloudWatchPutMetricDataAPI
}

func TestProcessMessage(t *testing.T) {
	consumer := TestAlertsConsumer{
		AlertsConsumer: AlertsConsumer{
			deployEnv: "test-env",
		},
	}
	rawmsg := `2017-08-15T18:39:07.000000+00:00 my-hostname production--my-app/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fbe5eafc1-8e44-489a-8942-aaaaaaaaaaaa[3337]: {"level":"info","source":"oauth","title":"login_start","action":"login","type":"counter","session_id":"sss","auth_method":"auth","district":"ddd","client_id":"ccc","app_id":"aaa","request_id":"","_kvmeta":{"team":"eng-team","kv_version":"3.8.2","kv_language":"js","routes":[{"type":"analytics","series":"series-name","rule":"login-events"},{"type":"alerts","series":"oauth.login_start","dimensions":["district","title","auth_method"],"stat_type":"counter","value_field":"value","rule":"login-start"}]}}`
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.NoError(t, err)

	expectedTags := []string{"default"}
	assert.Equal(t, expectedTags, tags)

	// Verify the message
	eo := EncodeOutput{}
	err = json.Unmarshal(msg, &eo)
	assert.NoError(t, err)

	expectedPts := []datadog.MetricSeries{
		{
			Metric: "kv.oauth.login_start",
			Tags: []string{
				"district:ddd",
				"title:login_start",
				"auth_method:auth",
				"Hostname:my-hostname",
				"env:test-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(1),
				Timestamp: aws.Int64(1502822347),
			}},
			Type: datadog.METRICINTAKETYPE_COUNT.Ptr(),
		},
	}

	assert.Equal(t, expectedPts, eo.DDMetrics)
}

func TestProcessMessageSupportsCloudwatch(t *testing.T) {
	consumer := TestAlertsConsumer{
		AlertsConsumer: AlertsConsumer{
			deployEnv: "test-env",
		},
	}
	rawmsg := `2017-08-15T18:39:07.000000+00:00 my-hostname production--my-app/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fbe5eafc1-8e44-489a-8942-aaaaaaaaaaaa[3337]: {"_kvmeta":{"kv_language":"go","kv_version":"6.16.0","routes":[{"dimensions":["dimension1"],"rule":"unexpected-stop","series":"ContainerExitCount","stat_type":"counter","type":"alerts","value_field":"value"}],"team":"eng-infra"},"category":"app_lifecycle","level":"info","title":"title","dimension1":"dim","region":"reg","type":"counter","value":1}`
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.NoError(t, err)

	expectedTags := []string{"reg"}
	assert.Equal(t, expectedTags, tags)

	// Verify the message
	eo := EncodeOutput{}
	err = json.Unmarshal(msg, &eo)
	assert.NoError(t, err)

	timestamp, _ := time.Parse(time.RFC3339Nano, "2017-08-15T18:39:07.000000Z")
	expected := EncodeOutput{
		DDMetrics: []datadog.MetricSeries{{
			Metric: "kv.ContainerExitCount",
			Type:   datadog.METRICINTAKETYPE_COUNT.Ptr(),
			Tags:   []string{"dimension1:dim", "Hostname:my-hostname", "env:test-env"},
			Points: []datadog.MetricPoint{
				{
					Timestamp: datadog.PtrInt64(timestamp.Unix()),
					Value:     aws.Float64(1),
				},
			},
		}},
		CWMetrics: []types.MetricDatum{
			{
				Dimensions: []types.Dimension{
					{
						Name:  aws.String("dimension1"),
						Value: aws.String("dim"),
					},
				},
				MetricName:        aws.String("ContainerExitCount"),
				Timestamp:         aws.Time(timestamp),
				Value:             aws.Float64(1),
				StorageResolution: aws.Int32(1),
			},
		},
	}

	assert.Equal(t, expected, eo)
}

// TestEncodeMessage tests the encodeMessage() helper used in ProcessMessage()
func TestEncodeMessage(t *testing.T) {
	consumer := TestAlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(123),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Unix(0, 0),
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

	expectedPts := []datadog.MetricSeries{{
		Metric: "kv.series-name",
		Tags:   []string{"dim_a:dim_a_val", "dim_b:dim_b_val", "Hostname:my-hostname", "env:my-env"},
		Points: []datadog.MetricPoint{{
			Value:     aws.Float64(123),
			Timestamp: aws.Int64(0),
		}},
		Type: datadog.METRICINTAKETYPE_COUNT.Ptr(),
	}}

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, eo.DDMetrics)
}

func TestEncodeMessageWithNonStringDimensions(t *testing.T) {
	consumer := TestAlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(123),
		"dim_a":     "dim_a_val",
		"dim_float": float64(3.2),
		"dim_bool":  true,
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Unix(0, 0),
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

	expectedPts := []datadog.MetricSeries{{
		Metric: "kv.series-name",
		Tags: []string{
			"dim_a:dim_a_val",
			"dim_float:3",
			"dim_bool:true",
			"Hostname:my-hostname",
			"env:my-env",
		},
		Points: []datadog.MetricPoint{{
			Value:     aws.Float64(123),
			Timestamp: aws.Int64(0),
		}},
		Type: datadog.METRICINTAKETYPE_COUNT.Ptr(),
	}}

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts, eo.DDMetrics)
}

func TestEncodeMessageErrorsIfInvalidDimensionType(t *testing.T) {
	consumer := TestAlertsConsumer{}
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
	consumer := TestAlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     "12345", // should fail, even though it's numeric its not the right type
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
	consumer := TestAlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(9.5),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Unix(0, 0),
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

	expectedPts := []datadog.MetricSeries{{
		Metric: "kv.series-name",
		Type:   datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		Tags:   []string{"dim_a:dim_a_val", "dim_b:dim_b_val", "Hostname:my-hostname", "env:my-env"},
		Points: []datadog.MetricPoint{
			{
				Timestamp: datadog.PtrInt64(0),
				Value:     aws.Float64(9.5),
			},
		},
	}}

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Points[0].Value, eo.DDMetrics[0].Points[0].Value)
	assert.Equal(t, expectedPts, eo.DDMetrics)
}

func TestEncodeMessageWithMultipleRoutes(t *testing.T) {
	consumer := TestAlertsConsumer{}
	input := map[string]interface{}{
		"rawlog":    "...",
		"value":     float64(9.5),
		"dim_a":     "dim_a_val",
		"dim_b":     "dim_b_val",
		"Hostname":  "my-hostname",
		"env":       "my-env",
		"timestamp": time.Unix(0, 0),
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

	expectedPts := []datadog.MetricSeries{
		{
			Metric: "kv.series-name",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
		},
		{
			Metric: "kv.series-name-2",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
		},
	}

	eo := EncodeOutput{}
	err = json.Unmarshal(output, &eo)
	assert.NoError(t, err)

	assert.Equal(t, expectedPts[0].Points[0].Value, eo.DDMetrics[0].Points[0].Value)
	assert.Equal(t, expectedPts, eo.DDMetrics)
}

func TestEncodeMessageWithNoAlertsRoutes(t *testing.T) {
	t.Log("If message has no Alerts routes, it will write 0 datapoints")
	consumer := TestAlertsConsumer{}

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

type MockCW struct {
	inputs []*cloudwatch.PutMetricDataInput
}

func (cw *MockCW) PutMetricData(ctx context.Context, input *cloudwatch.PutMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	cw.inputs = append(cw.inputs, input)
	return &cloudwatch.PutMetricDataOutput{}, nil
}

type MockDD struct {
	DDMetricsAPI
	inputs []datadog.MetricSeries
}

func (dd *MockDD) SubmitMetrics(ctx context.Context, body datadog.MetricPayload, o ...datadog.SubmitMetricsOptionalParameters) (datadog.IntakePayloadAccepted, *http.Response, error) {
	dd.inputs = append(dd.inputs, body.Series...)
	return datadog.IntakePayloadAccepted{}, nil, nil
}

func TestSendBatch(t *testing.T) {
	pts := []datadog.MetricSeries{
		{
			Metric: "series-name",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
		{
			Metric: "series-name-2",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
	}
	pts2 := []datadog.MetricSeries{
		{
			Metric: "series-name-3",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
		{
			Metric: "series-name-4",
			Tags:   []string{"dim_a:dim_a_val"},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
	}

	b, err := json.Marshal(EncodeOutput{
		DDMetrics: pts,
	})
	assert.NoError(t, err)
	input := [][]byte{b}

	b2, err := json.Marshal(EncodeOutput{
		DDMetrics: pts2,
	})
	assert.NoError(t, err)
	input2 := [][]byte{b2}

	mockCWUSWest1 := &MockCW{}
	mockCWs := map[string]CloudWatchPutMetricDataAPI{
		"us-west-1": mockCWUSWest1,
	}
	mockDD := &MockDD{}
	consumer := TestAlertsConsumer{
		AlertsConsumer: AlertsConsumer{
			dd: mockDD,
		},
		cwAPIs: mockCWs,
	}
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)
	assert.Equal(t, pts, mockDD.inputs)

	err = consumer.SendBatch(input2, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts, pts2...), mockDD.inputs)
}

func TestSendBatchToCloudwatch(t *testing.T) {
	dats := []types.MetricDatum{
		{
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Hostname"),
					Value: aws.String("my-hostname"),
				},
				{
					Name:  aws.String("env"),
					Value: aws.String("test-env"),
				},
			},
			MetricName: aws.String("ContainerExitCount"),
			Value:      aws.Float64(1),
		},
		{
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Hostname"),
					Value: aws.String("my-hostname"),
				},
				{
					Name:  aws.String("env"),
					Value: aws.String("test-env"),
				},
			},
			MetricName: aws.String("ContainerExitCount"),
			Value:      aws.Float64(1),
		},
	}

	expected := []*cloudwatch.PutMetricDataInput{
		{
			Namespace: aws.String("LogMetrics"),
			MetricData: []types.MetricDatum{
				{
					Dimensions: []types.Dimension{
						{
							Name:  aws.String("Hostname"),
							Value: aws.String("my-hostname"),
						},
						{
							Name:  aws.String("env"),
							Value: aws.String("test-env"),
						},
					},
					MetricName: aws.String("ContainerExitCount"),
					Value:      aws.Float64(1),
				},
				{
					Dimensions: []types.Dimension{
						{
							Name:  aws.String("Hostname"),
							Value: aws.String("my-hostname"),
						},
						{
							Name:  aws.String("env"),
							Value: aws.String("test-env"),
						},
					},
					MetricName: aws.String("ContainerExitCount"),
					Value:      aws.Float64(1),
				},
			},
		},
	}

	b, err := json.Marshal(EncodeOutput{
		DDMetrics: []datadog.MetricSeries{}, // Empty DataDog metrics
		CWMetrics: dats,
	})
	assert.NoError(t, err)
	input := [][]byte{b}

	mockCWUSWest1 := &MockCW{}
	mockCWs := map[string]CloudWatchPutMetricDataAPI{
		"us-west-1": mockCWUSWest1,
	}
	mockDD := &MockDD{}
	consumer := TestAlertsConsumer{
		AlertsConsumer: AlertsConsumer{
			dd: mockDD,
		},
		cwAPIs: mockCWs,
	}
	t.Log("Send batch")
	err = consumer.SendBatch(input, "us-west-1")
	assert.NoError(t, err)
	t.Logf("Mock CW inputs: %+v", mockCWUSWest1.inputs)
	assert.Equal(t, expected, mockCWUSWest1.inputs)
}

func TestSendBatchWithMultipleEntries(t *testing.T) {
	pts := []datadog.MetricSeries{
		{
			Metric: "kv.series-name",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
		{
			Metric: "kv.series-name-2",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
	}
	pts2 := []datadog.MetricSeries{
		{
			Metric: "kv.series-name-3",
			Tags: []string{
				"dim_a:dim_a_val",
				"dim_b:dim_b_val",
				"Hostname:my-hostname",
				"env:my-env",
			},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
		{
			Metric: "kv.series-name-4",
			Tags:   []string{"dim_a:dim_a_val"},
			Points: []datadog.MetricPoint{{
				Value:     aws.Float64(9.5),
				Timestamp: aws.Int64(0),
			}},
			Type: datadog.METRICINTAKETYPE_GAUGE.Ptr(),
		},
	}

	b, err := json.Marshal(EncodeOutput{
		DDMetrics: pts,
	})
	assert.NoError(t, err)

	b2, err := json.Marshal(EncodeOutput{
		DDMetrics: pts2,
	})
	assert.NoError(t, err)

	input := [][]byte{b, b2}

	mockCWUSWest1 := MockCW{}
	mockCWs := map[string]CloudWatchPutMetricDataAPI{
		"us-west-1": &mockCWUSWest1,
	}
	mockDD := &MockDD{}
	consumer := TestAlertsConsumer{
		AlertsConsumer: AlertsConsumer{
			dd: mockDD,
		},
		cwAPIs: mockCWs,
	}
	t.Log("Send batch with multiple entries")
	err = consumer.SendBatch(input, "default")
	assert.NoError(t, err)
	assert.Equal(t, append(pts, pts2...), mockDD.inputs)
	assert.Equal(t, datadog.METRICINTAKETYPE_GAUGE, *mockDD.inputs[0].Type)
	assert.Equal(t, "kv.series-name", mockDD.inputs[0].Metric)
	assert.Equal(t, "kv.series-name-4", mockDD.inputs[3].Metric)
}

// Override SendBatch to use the test's cwAPIs field
func (c *TestAlertsConsumer) SendBatch(batch [][]byte, tag string) error {
	metrics := []datadog.MetricSeries{}
	dats := []types.MetricDatum{}
	for _, b := range batch {
		eo := EncodeOutput{}
		err := json.Unmarshal(b, &eo)
		if err != nil {
			return err
		}

		metrics = append(metrics, eo.DDMetrics...)
		dats = append(dats, eo.CWMetrics...)
	}

	ts := make([]time.Time, len(metrics))
	for i, m := range metrics {
		// Our code only submits one point per metric submission
		ts[i] = time.Unix(*m.Points[0].Timestamp, 0)
	}
	updateMaxDelay(ts)

	retry := retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)

	err := retry.Run(func() error {
		lg.TraceD("dd-submit-metrics", logger.M{"point-count": len(metrics)})
		ctx := datadog.NewDefaultContext(context.Background())
		body := datadog.NewMetricPayload(metrics)
		_, _, err := c.dd.SubmitMetrics(ctx, *body)
		return err
	})
	if err != nil {
		lg.ErrorD("dd-submit-metrics", logger.M{"error": err.Error()})
		return kbc.PartialSendBatchError{ErrMessage: "failed to send metrics to datadog: " + err.Error(), FailedMessages: batch}
	}

	// only send to Cloudwatch if the tag is an AWS region
	if api, ok := c.cwAPIs[tag]; ok {
		lg.TraceD("cloudwatch-add-datapoints", logger.M{"point-count": len(dats)})
		_, err = api.PutMetricData(context.Background(), &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(cloudwatchNamespace),
			MetricData: dats,
		})
		if err != nil {
			lg.ErrorD("error-sending-to-cloudwatch", logger.M{"error": err.Error()})
		}
	}

	return nil
}
