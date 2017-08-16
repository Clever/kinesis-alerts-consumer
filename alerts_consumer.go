package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"golang.org/x/net/context"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/amazon-kinesis-client-go/decode"
	"github.com/Clever/kayvee-go/logger"
)

var lg = logger.New("kinesis-alerts-consumer")
var sfxSink *sfxclient.HTTPSink

var defaultDimensions = []string{"hostname", "env"}

// AlertsConsumer sends datapoints to SignalFX
// It implements the kbc.Sender interface
type AlertsConsumer struct {
	sfxSink      sfxclient.Sink
	deployEnv    string
	minTimestamp time.Time
}

// ProcessMessage is called once per log to parse the log line and then reformat it
// so that it can be directly used by the output. The returned tags will be passed along
// with the encoded log to SendBatch()
func (c *AlertsConsumer) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	// Parse the log line
	fields, err := decode.ParseAndEnhance(string(rawmsg), c.deployEnv, false, false, c.minTimestamp)
	if err != nil {
		return nil, []string{}, err
	}

	return c.encodeMessage(fields)
}

func (c *AlertsConsumer) encodeMessage(fields map[string]interface{}) ([]byte, []string, error) {
	// Determine routes
	kvmeta := decode.ExtractKVMeta(fields)
	routes := kvmeta.Routes.AlertRoutes()
	routes = append(routes, c.globalRoutes(fields)...)
	if len(routes) <= 0 {
		return nil, nil, kbc.ErrMessageIgnored
	}

	timestamp, ok := fields["timestamp"].(time.Time)
	if !ok {
		return []byte{}, []string{}, fmt.Errorf("unable parse Time from message's 'timestamp' field")
	}
	if timestamp.Before(c.minTimestamp) {
		return []byte{}, []string{}, kbc.ErrMessageIgnored
	}

	// Create datapoints to send to SFX
	points := []datapoint.Datapoint{}
	for _, route := range routes {
		// Look up dimensions (custom + default)
		dims := map[string]string{}
		for _, dim := range append(route.Dimensions, defaultDimensions...) {
			dimVal, ok := fields[dim].(string)
			if ok {
				dims[dim] = dimVal
			}
		}

		// Create datapoint
		var pt *datapoint.Datapoint
		if route.StatType == "counter" {
			counterVal := int64(1)
			val, ok := fields[route.ValueField].(float64)
			if ok {
				counterVal = int64(val)
			}
			pt = datapoint.New(route.Series, dims, datapoint.NewIntValue(counterVal), datapoint.Counter, timestamp)
		} else if route.StatType == "gauge" {
			gaugeVal := float64(0)
			val, ok := fields[route.ValueField].(float64)
			if ok {
				gaugeVal = val
			}
			pt = datapoint.New(route.Series, dims, datapoint.NewFloatValue(gaugeVal), datapoint.Gauge, timestamp)
		} else {
			return []byte{}, []string{}, fmt.Errorf("invalid StatType in route: %s", route.StatType)
		}

		points = append(points, *pt)
	}

	// May return more than one SFX datapoint
	out, err := json.Marshal(points)
	if err != nil {
		return []byte{}, []string{}, err
	}

	return out, []string{"default"}, nil
}

// SendBatch is called once per batch per tag
func (c *AlertsConsumer) SendBatch(batch [][]byte, tag string) error {
	pts := []datapoint.Datapoint{}
	for _, b := range batch {
		batchPts := []datapoint.Datapoint{}
		err := json.Unmarshal(b, &batchPts)
		if err != nil {
			return err
		}

		pts = append(pts, batchPts...)
	}

	ptRefs := []*datapoint.Datapoint{}
	for idx := range pts {
		updateMaxDelay(pts[idx].Timestamp)

		// For fairly recent logs, let SignalFX assign a timestamp on arrival,
		// instead of the log's actual timestamp.  This ensures datapoints are sent
		// in-order per timeseries. (If out of order, SignalFX will drop them
		// silently.)
		//
		// For older logs, use their actual timestamp. This may result in some
		// dropped logs, but should be less important since we care mainly about
		// accurate alerting on recent data.
		if isRecent(pts[idx].Timestamp, 30*time.Second) {
			pts[idx].Timestamp = time.Time{}
		}
		ptRefs = append(ptRefs, &pts[idx])
	}

	return c.sfxSink.AddDatapoints(context.TODO(), ptRefs)
}

func (s *AlertsConsumer) globalRoutes(fields map[string]interface{}) []decode.AlertRoute {
	// TODO: Not yet supported
	return []decode.AlertRoute{}
}
