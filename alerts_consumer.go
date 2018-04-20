package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"golang.org/x/net/context"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/amazon-kinesis-client-go/decode"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var lg = logger.New("kinesis-alerts-consumer")
var sfxSink *sfxclient.HTTPSink

var defaultDimensions = []string{"Hostname", "env"}

// AlertsConsumer sends datapoints to SignalFX
// It implements the kbc.Sender interface
type AlertsConsumer struct {
	sfxSink   httpSinkInterface
	deployEnv string

	rollups *Rollups
}

type httpSinkInterface interface {
	AddDatapoints(context.Context, []*datapoint.Datapoint) error
	AddEvents(context.Context, []*event.Event) error
}

func NewAlertsConsumer(sfxSink httpSinkInterface, deployEnv string) *AlertsConsumer {
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

type EncodeOutput struct {
	Datapoints []*datapoint.Datapoint
	Events     []*event.Event
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

	// Create batch item from message
	eo := EncodeOutput{
		Datapoints: []*datapoint.Datapoint{},
		Events:     []*event.Event{},
	}

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

		var pt *datapoint.Datapoint
		var evt *event.Event

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
		} else if route.StatType == "event" {
			// Custom Stat type NOT supported by Kayvee routing
			// Use Case: send app lifecycle events as "events" to SignalFX
			evt = event.New(route.Series, event.USERDEFINED, dims, timestamp)
		} else {
			return []byte{}, []string{}, fmt.Errorf("invalid StatType in route: %s", route.StatType)
		}

		if pt != nil {
			eo.Datapoints = append(eo.Datapoints, pt)
		} else if evt != nil {
			eo.Events = append(eo.Events, evt)
		}
	}

	out, err := json.Marshal(&eo)
	if err != nil {
		return []byte{}, []string{}, err
	}

	return out, []string{"default"}, nil
}

func (c *AlertsConsumer) sendPoints(pts []*datapoint.Datapoint, retries int) error {
	err := c.sfxSink.AddDatapoints(context.TODO(), pts)
	if retries <= 0 || err == nil {
		return nil
	}

	if err, ok := err.(net.Error); ok && err.Timeout() {
		lg.WarnD("retry-send-pts", logger.M{"retries": retries})
		time.Sleep(1 * time.Second)
		return c.sendPoints(pts, retries-1)
	}

	return err
}

func (c *AlertsConsumer) sendEvents(evts []*event.Event, retries int) error {
	err := c.sfxSink.AddEvents(context.TODO(), evts)
	if retries <= 0 || err == nil {
		return err
	}

	if err, ok := err.(net.Error); ok && err.Timeout() {
		lg.WarnD("retry-send-events", logger.M{"retries": retries})
		time.Sleep(1 * time.Second)
		return c.sendEvents(evts, retries-1)
	}

	return err
}

// SendBatch is called once per batch per tag
func (c *AlertsConsumer) SendBatch(batch [][]byte, tag string) error {
	pts := []*datapoint.Datapoint{}
	evts := []*event.Event{}
	for _, b := range batch {
		eo := EncodeOutput{}
		err := json.Unmarshal(b, &eo)
		if err != nil {
			return err
		}

		pts = append(pts, eo.Datapoints...)
		evts = append(evts, eo.Events...)
	}

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
	}

	if err := c.sendPoints(pts, 3); err != nil {
		if err.Error() == "invalid status code 400" { // internal buffer full on sfx's side
			return kbc.PartialSendBatchError{
				ErrMessage: "failed to add datapoints: " + err.Error(), FailedMessages: batch,
			}
		}
		return err
	}

	if err := c.sendEvents(evts, 3); err != nil {
		if err.Error() == "invalid status code 400" { // internal buffer full on sfx's side
			return kbc.PartialSendBatchError{
				ErrMessage: "failed to add events: " + err.Error(), FailedMessages: batch,
			}
		}
		return err
	}

	return nil
}
