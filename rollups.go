package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/signalfx/golib/sfxclient"
)

// Rollups rolls up data and sends it periodically to signalfx.
type Rollups struct {
	sfxSink                            sfxclient.Sink
	scheduler                          *sfxclient.Scheduler
	rollupsRequestFinishedResponseTime map[string]*sfxclient.RollingBucket
	rollupsRequestFinishedStatusCode   map[string]*sfxclient.CumulativeBucket
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
		sfxSink:                            sfxSink,
		scheduler:                          scheduler,
		rollupsRequestFinishedResponseTime: map[string]*sfxclient.RollingBucket{},
		rollupsRequestFinishedStatusCode:   map[string]*sfxclient.CumulativeBucket{},
	}
}

// Run this in a goroutine.
func (r *Rollups) Run(ctx context.Context) error {
	return r.scheduler.Schedule(ctx)
}

func (r *Rollups) Process(fields map[string]interface{}) error {
	if fields["env"] != "" && fields["container_app"] != "" && fields["title"] == "request-finished" && fields["via"] == "kayvee-middleware" {

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
				lg.InfoD("rollup-create", map[string]interface{}{"key": dimensionsKey})
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
				lg.InfoD("rollup-create", map[string]interface{}{"key": dimensionsKey})
				bucket = &sfxclient.CumulativeBucket{
					MetricName: "request-finished-status-code",
					Dimensions: dimensions,
				}
				r.rollupsRequestFinishedStatusCode[dimensionsKey] = bucket
				r.scheduler.AddCallback(bucket)
			}
			bucket.Add(1)
		}
	}

	return nil
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
