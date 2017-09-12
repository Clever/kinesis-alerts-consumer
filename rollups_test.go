package main

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ByMetric []datapoint.Datapoint

func (m ByMetric) Len() int           { return len(m) }
func (m ByMetric) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m ByMetric) Less(i, j int) bool { return m[i].Metric < m[j].Metric }

func TestRollupRequestFinished(t *testing.T) {
	mockSink := &MockSink{}
	r := NewRollups(mockSink)
	r.scheduler.ReportingDelayNs = (1 * time.Second).Nanoseconds()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	for count := 0; count < 100; count++ {
		r.Process(map[string]interface{}{
			"deploy_env":    "production",
			"container_app": "app-service",
			"title":         "request-finished",
			"via":           "kayvee-middleware",
			"canary":        false,
			"response-time": float64(1000000),
			"status-code":   float64(200),
		})
	}

	time.Sleep(1*time.Second + 100*time.Millisecond)

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{Metric: "request-finished-response-time.count",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-response-time.sum",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false"},
			Value:      datapoint.NewFloatValue(100000000),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-response-time.sumsquare",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false"},
			Value:      datapoint.NewFloatValue(100000000000000),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-status-code.count",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false", "status-code": "200"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-status-code.sum",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false", "status-code": "200"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-status-code.sumsquare",
			Dimensions: map[string]string{"env": "production", "container_app": "app-service", "canary": "false", "status-code": "200"},
			Value:      datapoint.NewFloatValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
	}

	sort.Sort(ByMetric(expectedPts))
	sort.Sort(ByMetric(mockSink.pts))

	if len(expectedPts) != len(mockSink.pts) {
		// array Equal() prints less detail, but is sufficient to diagnose this failure case
		require.Equal(t, expectedPts, mockSink.pts)
	}
	for i := range expectedPts {
		assert.Equal(t, expectedPts[i], mockSink.pts[i])
	}
}

func TestRollupRequestFinishedThrift(t *testing.T) {
	mockSink := &MockSink{}
	r := NewRollups(mockSink)
	r.scheduler.ReportingDelayNs = (1 * time.Second).Nanoseconds()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	for count := 0; count < 100; count++ {
		r.Process(map[string]interface{}{
			"deploy_env":    "production",
			"container_app": "systemic",
			"title":         "request_finished",
			"proto":         "thrift",
			"response_time": float64(1000000),
			"type_id":       float64(2),
		})
	}

	time.Sleep(1*time.Second + 100*time.Millisecond)

	expectedPts := []datapoint.Datapoint{
		datapoint.Datapoint{Metric: "request-finished-response-time.count",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-response-time.sum",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic"},
			Value:      datapoint.NewFloatValue(100000000),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-response-time.sumsquare",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic"},
			Value:      datapoint.NewFloatValue(100000000000000),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-type-id.count",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic", "type_id": "2"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-type-id.sum",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic", "type_id": "2"},
			Value:      datapoint.NewIntValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
		datapoint.Datapoint{
			Metric:     "request-finished-type-id.sumsquare",
			Dimensions: map[string]string{"env": "production", "container_app": "systemic", "type_id": "2"},
			Value:      datapoint.NewFloatValue(100),
			MetricType: datapoint.Counter,
			Timestamp:  time.Time{},
			Meta:       map[interface{}]interface{}{},
		},
	}

	sort.Sort(ByMetric(expectedPts))
	sort.Sort(ByMetric(mockSink.pts))

	if len(expectedPts) != len(mockSink.pts) {
		// array Equal() prints less detail, but is sufficient to diagnose this failure case
		require.Equal(t, expectedPts, mockSink.pts)
	}
	for i := range expectedPts {
		assert.Equal(t, expectedPts[i], mockSink.pts[i])
	}
}
