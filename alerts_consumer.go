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

var defaultDimensions = []string{"Hostname", "env"}

// AlertsConsumer sends datapoints to SignalFX
// It implements the kbc.Sender interface
type AlertsConsumer struct {
	sfxSink   sfxclient.Sink
	deployEnv string

	rollups *Rollups
}

func NewAlertsConsumer(sfxSink sfxclient.Sink, deployEnv string) *AlertsConsumer {
	rollups := NewRollups(sfxSink)
	go rollups.Run(context.Background())
	return &AlertsConsumer{
		sfxSink:   sfxSink,
		deployEnv: deployEnv,
		rollups:   rollups,
	}
}

// ProcessMessage is called once per log to parse the log line and then reformat it
// so that it can be directly used by the output. The returned tags will be passed along
// with the encoded log to SendBatch()
func (c *AlertsConsumer) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	// Parse the log line
	fields, err := decode.ParseAndEnhance(string(rawmsg), c.deployEnv)
	if err != nil {
		return nil, []string{}, err
	}

	if err := c.rollups.Process(fields); err != nil {
		return []byte{}, []string{}, err
	}

	return c.encodeMessage(fields)
}

func (c *AlertsConsumer) encodeMessage(fields map[string]interface{}) ([]byte, []string, error) {
	// Determine routes
	// KVMeta Routes
	kvmeta := decode.ExtractKVMeta(fields)
	routes := kvmeta.Routes.AlertRoutes()
	for idx := range routes {
		routes[idx].Dimensions = append(routes[idx].Dimensions, defaultDimensions...)
	}

	// Global Routes
	routes = append(routes, globalRoutes(fields)...)

	if len(routes) <= 0 {
		return nil, nil, kbc.ErrMessageIgnored
	}

	// For backwards compatibility, add `Hostname` field (capitalized)
	hostname, ok := fields["hostname"]
	if ok {
		fields["Hostname"] = hostname
	}

	timestamp, ok := fields["timestamp"].(time.Time)
	if !ok {
		return []byte{}, []string{}, fmt.Errorf("unable parse Time from message's 'timestamp' field")
	}
	// Create datapoints to send to SFX
	points := []datapoint.Datapoint{}
	for _, route := range routes {
		// Look up dimensions (custom + default)
		dims := map[string]string{}
		for _, dim := range route.Dimensions {
			dimVal, ok := fields[dim]
			if ok {
				switch t := dimVal.(type) {
				case string:
					dims[dim] = t
				case float64:
					// Drop data after the decimal and cast to string (ex. 3.2 => "3")
					dims[dim] = fmt.Sprintf("%.0f", t)
				case bool:
					dims[dim] = fmt.Sprintf("%t", t)
				default:
					return []byte{}, []string{}, fmt.Errorf("error casting dimension value. rule=%s dim=%s val=%s", route.RuleName, dim, dimVal)
				}
			}
		}

		// Create datapoint
		var pt *datapoint.Datapoint

		// 3 cases
		// 	(1) val exists and it's a float
		// 	(2) val exists but it's NOT a float (error)
		// 	(3) val doesnt exist => use default value
		val, valOk := fields[route.ValueField].(float64)
		if !valOk {
			valInterface, valueFieldExists := fields[route.ValueField]
			if valueFieldExists {
				// case (2)
				return []byte{}, []string{}, fmt.Errorf("value exists but is wrong type. rule=%s value_field=%s value=%s", route.RuleName, route.ValueField, valInterface)
			}
		}

		if route.StatType == "counter" {
			counterVal := int64(1)
			if valOk {
				counterVal = int64(val)
			}
			pt = datapoint.New(route.Series, dims, datapoint.NewIntValue(counterVal), datapoint.Counter, timestamp)
		} else if route.StatType == "gauge" {
			gaugeVal := float64(0)
			if valOk {
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
