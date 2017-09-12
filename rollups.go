package main

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/signalfx/golib/sfxclient"
)

// Rollups rolls up data and sends it periodically to signalfx.
type Rollups struct {
	sfxSink                                  sfxclient.Sink
	scheduler                                *sfxclient.Scheduler
	rollupsRequestFinishedResponseTime       map[string]*sfxclient.RollingBucket
	rollupsRequestFinishedStatusCode         map[string]*sfxclient.CumulativeBucket
	rollupsThriftRequestFinishedResponseTime map[string]*sfxclient.RollingBucket
	rollupsThriftRequestFinishedStatusCode   map[string]*sfxclient.CumulativeBucket
}

func NewRollups(sfxSink sfxclient.Sink) *Rollups {
	scheduler := sfxclient.NewScheduler()
	scheduler.Sink = sfxSink
	scheduler.SendZeroTime = true
	scheduler.ErrorHandler = func(err error) error {
		lg.ErrorD("rollup-error", map[string]interface{}{"error": err.Error()})
		return nil
	}
	return &Rollups{
		sfxSink:                                  sfxSink,
		scheduler:                                scheduler,
		rollupsRequestFinishedResponseTime:       map[string]*sfxclient.RollingBucket{},
		rollupsRequestFinishedStatusCode:         map[string]*sfxclient.CumulativeBucket{},
		rollupsThriftRequestFinishedResponseTime: map[string]*sfxclient.RollingBucket{},
		rollupsThriftRequestFinishedStatusCode:   map[string]*sfxclient.CumulativeBucket{},
	}
}

// Run this in a goroutine.
func (r *Rollups) Run(ctx context.Context) error {
	return r.scheduler.Schedule(ctx)
}

func (r *Rollups) Process(fields map[string]interface{}) error {

	if fields["title"] == "request-finished" && fields["via"] == "kayvee-middleware" &&
		contains(fields, "env") && contains(fields, "container_app") && contains(fields, "canary") && contains(fields, "response-time") && contains(fields, "status-code") {

		if _, ok := fields["env"].(string); !ok {
			return fmt.Errorf("expected env to be string, got %s", reflect.TypeOf(fields["env"]))
		}
		if _, ok := fields["container_app"].(string); !ok {
			return fmt.Errorf("expected container_app to be string, got %s", reflect.TypeOf(fields["container_app"]))
		}
		if _, ok := fields["canary"].(bool); !ok {
			return fmt.Errorf("expected canary to be bool, got %s", reflect.TypeOf(fields["canary"]))
		}
		if _, ok := fields["response-time"].(float64); !ok {
			return fmt.Errorf("expected response-time to be float64, got %s", reflect.TypeOf(fields["response-time"]))
		}
		if _, ok := fields["status-code"].(float64); !ok {
			return fmt.Errorf("expected status-code to be float64, got %s", reflect.TypeOf(fields["status-code"]))
		}

		// rollup response times
		{
			dimensions := map[string]string{
				"env":           fields["env"].(string),
				"container_app": fields["container_app"].(string),
				"canary":        fmt.Sprintf("%t", fields["canary"].(bool)),
			}
			dimensionsKey := join(dimensions, "-")
			bucket, ok := r.rollupsRequestFinishedResponseTime[dimensionsKey]
			if !ok {
				lg.InfoD("rollup-create", map[string]interface{}{"metric": "request-finished-response-time", "key": dimensionsKey})
				bucket = sfxclient.NewRollingBucket("request-finished-response-time", dimensions)
				r.rollupsRequestFinishedResponseTime[dimensionsKey] = bucket
				r.scheduler.AddCallback(bucket)
			}
			bucket.Add(fields["response-time"].(float64))
		}

		// count status codes
		{
			dimensions := map[string]string{
				"env":           fields["env"].(string),
				"container_app": fields["container_app"].(string),
				"canary":        fmt.Sprintf("%t", fields["canary"].(bool)),
				"status-code":   fmt.Sprintf("%d", int(fields["status-code"].(float64))),
			}
			dimensionsKey := join(dimensions, "-")
			bucket, ok := r.rollupsRequestFinishedStatusCode[dimensionsKey]
			if !ok {
				lg.InfoD("rollup-create", map[string]interface{}{"metric": "request-finished-status-code", "key": dimensionsKey})
				bucket = &sfxclient.CumulativeBucket{
					MetricName: "request-finished-status-code",
					Dimensions: dimensions,
				}
				r.rollupsRequestFinishedStatusCode[dimensionsKey] = bucket
				r.scheduler.AddCallback(bucket)
			}
			bucket.Add(1)
		}
	} else if fields["proto"] == "thrift" && fields["title"] == "request_finished" &&
		contains(fields, "env") && contains(fields, "container_app") && contains(fields, "response_time") && contains(fields, "type_id") {

		if _, ok := fields["env"].(string); !ok {
			return fmt.Errorf("expected env to be string, got %s", reflect.TypeOf(fields["env"]))
		}
		if _, ok := fields["container_app"].(string); !ok {
			return fmt.Errorf("expected container_app to be string, got %s", reflect.TypeOf(fields["container_app"]))
		}
		if _, ok := fields["response_time"].(float64); !ok {
			return fmt.Errorf("expected response_time to be float64, got %s", reflect.TypeOf(fields["response_time"]))
		}
		if _, ok := fields["type_id"].(float64); !ok {
			return fmt.Errorf("expected type_id to be float64, got %s", reflect.TypeOf(fields["type_id"]))
		}

		// rollup thrift response times
		{
			dimensions := map[string]string{
				"env":           fields["env"].(string),
				"container_app": fields["container_app"].(string),
			}
			dimensionsKey := join(dimensions, "-")
			bucket, ok := r.rollupsThriftRequestFinishedResponseTime[dimensionsKey]
			if !ok {
				lg.InfoD("rollup-create", map[string]interface{}{"metric": "request-finished-response-time", "key": dimensionsKey})
				bucket = sfxclient.NewRollingBucket("request-finished-response-time", dimensions)
				r.rollupsThriftRequestFinishedResponseTime[dimensionsKey] = bucket
				r.scheduler.AddCallback(bucket)
			}
			bucket.Add(fields["response_time"].(float64))
		}

		// count thrift status codes, i.e. type_id
		{
			dimensions := map[string]string{
				"env":           fields["env"].(string),
				"container_app": fields["container_app"].(string),
				"type_id":       fmt.Sprintf("%d", int(fields["type_id"].(float64))),
			}
			dimensionsKey := join(dimensions, "-")
			bucket, ok := r.rollupsThriftRequestFinishedStatusCode[dimensionsKey]
			if !ok {
				lg.InfoD("rollup-create", map[string]interface{}{"metric": "request-finished-type-id", "key": dimensionsKey})
				bucket = &sfxclient.CumulativeBucket{
					MetricName: "request-finished-type-id",
					Dimensions: dimensions,
				}
				r.rollupsThriftRequestFinishedStatusCode[dimensionsKey] = bucket
				r.scheduler.AddCallback(bucket)
			}
			bucket.Add(1)
		}
	}

	return nil
}

func contains(m map[string]interface{}, key string) bool {
	_, ok := m[key]
	return ok
}

// join map values together in sorted key order
func join(m map[string]string, sep string) string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Sort(sort.StringSlice(keys))

	vals := []string{}
	for _, k := range keys {
		vals = append(vals, m[k])
	}

	return strings.Join(vals, sep)
}
