package main

import (
	"context"
	"fmt"

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
	// TODO: scheduler.ErrorHandler = ...
	go scheduler.Schedule(context.Background())

	return &Rollups{
		sfxSink:                            sfxSink,
		scheduler:                          scheduler,
		rollupsRequestFinishedResponseTime: map[string]*sfxclient.RollingBucket{},
		rollupsRequestFinishedStatusCode:   map[string]*sfxclient.CumulativeBucket{},
	}
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
				lg.InfoD("rollup-create", mapStringInterface(dimensions))
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
				lg.InfoD("rollup-create", mapStringInterface(dimensions))
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

func mapStringInterface(m map[string]string) map[string]interface{} {
	newM := map[string]interface{}{}
	for k, v := range m {
		newM[k] = v
	}
	return newM
}

func join(m map[string]string, sep string) string {
	var s string
	first := true
	for _, v := range m {
		if !first {
			s += sep
		} else {
			first = true
		}
		s += v
	}
	return s
}
