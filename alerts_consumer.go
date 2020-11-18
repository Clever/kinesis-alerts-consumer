package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/eapache/go-resiliency/retrier"
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

const cloudwatchNamespace = "LogMetrics"

var sfxDefaultDimensions = []string{"Hostname", "env"}

// AlertsConsumer sends datapoints to SignalFX
// It implements the kbc.Sender interface
type AlertsConsumer struct {
	sfxSink   httpSinkInterface
	deployEnv string
	cwAPIs    map[string]cloudwatchiface.CloudWatchAPI
}

type httpSinkInterface interface {
	AddDatapoints(context.Context, []*datapoint.Datapoint) error
	AddEvents(context.Context, []*event.Event) error
}

func NewAlertsConsumer(sfxSink httpSinkInterface, deployEnv string, cwAPIs map[string]cloudwatchiface.CloudWatchAPI) *AlertsConsumer {
	return &AlertsConsumer{
		sfxSink:   sfxSink,
		deployEnv: deployEnv,
		cwAPIs:    cwAPIs,
	}
}

// Initialize - we don't currently need to do any custom initialization
func (c *AlertsConsumer) Initialize(shardID string) {}

// ProcessMessage is called once per log to parse the log line and then reformat it
// so that it can be directly used by the output. The returned tags will be passed along
// with the encoded log to SendBatch()
func (c *AlertsConsumer) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	// Parse the log line
	fields, err := decode.ParseAndEnhance(string(rawmsg), c.deployEnv)
	if err != nil {
		return nil, []string{}, err
	}

	return c.encodeMessage(fields)
}

type EncodeOutput struct {
	Datapoints []*datapoint.Datapoint
	Events     []*event.Event
	MetricData []*cloudwatch.MetricDatum
}

// returns true if s is in the slice
func contains(slice []string, s string) bool {
	for _, str := range slice {
		if str == s {
			return true
		}
	}
	return false
}

func (c *AlertsConsumer) encodeMessage(fields map[string]interface{}) ([]byte, []string, error) {
	// Determine routes
	// KVMeta Routes
	kvmeta := decode.ExtractKVMeta(fields)
	env, _ := fields["container_env"].(string)
	app, _ := fields["container_app"].(string)
	updatelogVolumes(env, app, kvmeta.Team)

	routes := kvmeta.Routes.AlertRoutes()
	for idx := range routes {
		routes[idx].Dimensions = append(routes[idx].Dimensions, sfxDefaultDimensions...)
	}

	// Global Routes
	routes = append(routes, globalRoutes(fields)...)
	routes = append(routes, globalRoutesWithCustomFields(&fields)...)

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
		MetricData: []*cloudwatch.MetricDatum{},
	}

	// This is set to an AWS region if any of the routes are for metrics that are whitelisted for CloudWatch.
	// It is used to set a tag to group data points together before pushing to CloudWatch.
	tag := "default"

	for _, route := range routes {
		// Look up dimensions (custom + default)
		dims := map[string]string{}
		cwDims := []*cloudwatch.Dimension{}
		for _, dim := range route.Dimensions {
			dimVal, ok := fields[dim]
			if ok {
				switch t := dimVal.(type) {
				case string:
					dims[dim] = t
					if !contains(sfxDefaultDimensions, dim) {
						cwDims = append(cwDims, &cloudwatch.Dimension{
							Name:  aws.String(dim),
							Value: &t,
						})
					}
				case float64:
					// Drop data after the decimal and cast to string (ex. 3.2 => "3")
					val := fmt.Sprintf("%.0f", t)
					dims[dim] = val
					cwDims = append(cwDims, &cloudwatch.Dimension{
						Name:  aws.String(dim),
						Value: &val,
					})
				case bool:
					val := fmt.Sprintf("%t", t)
					dims[dim] = val
					cwDims = append(cwDims, &cloudwatch.Dimension{
						Name:  aws.String(dim),
						Value: &val,
					})
				default:
					return []byte{}, []string{}, fmt.Errorf(
						"error casting dimension value. rule=%s dim=%s val=%s",
						route.RuleName, dim, dimVal,
					)
				}
			}
		}

		cwWhitelisted, _ := cloudwatchWhitelist[route.Series]

		var pt *datapoint.Datapoint
		var evt *event.Event
		var dat *cloudwatch.MetricDatum

		// 3 cases
		// 	(1) val exists and it's a float
		// 	(2) val exists but it's NOT a float (error)
		// 	(3) val doesnt exist => use default value
		val, valOk := fields[route.ValueField].(float64)
		if !valOk {
			valInterface, valueFieldExists := fields[route.ValueField]
			if valueFieldExists {
				// case (2)
				return []byte{}, []string{}, fmt.Errorf(
					"value exists but is wrong type. rule=%s value_field=%s value=%s",
					route.RuleName, route.ValueField, valInterface,
				)
			}
		}

		if route.StatType == "counter" {
			counterVal := int64(1)
			if valOk {
				counterVal = int64(val)
			}
			pt = sfxclient.Cumulative(route.Series, dims, counterVal)
			pt.Timestamp = timestamp
			if cwWhitelisted {
				dat = &cloudwatch.MetricDatum{
					MetricName:        &route.Series,
					Dimensions:        cwDims,
					Value:             aws.Float64(float64(counterVal)),
					Timestamp:         &timestamp,
					StorageResolution: aws.Int64(1),
				}
			}
		} else if route.StatType == "gauge" {
			gaugeVal := float64(0)
			if valOk {
				gaugeVal = val
			}
			pt = sfxclient.GaugeF(route.Series, dims, gaugeVal)
			pt.Timestamp = timestamp
			if cwWhitelisted {
				dat = &cloudwatch.MetricDatum{
					MetricName:        &route.Series,
					Dimensions:        cwDims,
					Value:             &gaugeVal,
					Timestamp:         &timestamp,
					StorageResolution: aws.Int64(1),
				}
			}
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
		if dat != nil {
			if region, ok := fields["region"].(string); ok {
				tag = region
				eo.MetricData = append(eo.MetricData, dat)
			} else if podRegion, ok := fields["pod-region"].(string); ok {
				tag = podRegion
				eo.MetricData = append(eo.MetricData, dat)
			} else {
				lg.Error("region-missing")
			}
		}
	}

	out, err := json.Marshal(&eo)
	if err != nil {
		return []byte{}, []string{}, err
	}

	return out, []string{tag}, nil
}

// SendBatch is called once per batch per tag
// The tags should always be either "default" or an AWS region (e.g. "us-west-1")
func (c *AlertsConsumer) SendBatch(batch [][]byte, tag string) error {
	pts := []*datapoint.Datapoint{}
	evts := []*event.Event{}
	dats := []*cloudwatch.MetricDatum{}
	for _, b := range batch {
		eo := EncodeOutput{}
		err := json.Unmarshal(b, &eo)
		if err != nil {
			return err
		}

		pts = append(pts, eo.Datapoints...)
		evts = append(evts, eo.Events...)
		dats = append(dats, eo.MetricData...)
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

	var err error
	retry := retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)

	err = retry.Run(func() error {
		lg.TraceD("sfx-add-datapoints", logger.M{"point-count": len(pts)})
		return c.sfxSink.AddDatapoints(context.Background(), pts)
	})
	if err != nil && err.Error() == "invalid status code 400" { // internal buffer full in sfx
		return kbc.PartialSendBatchError{ErrMessage: "failed to add datapoints: " + err.Error(), FailedMessages: batch}
	}

	err = retry.Run(func() error {
		lg.TraceD("sfx-events", logger.M{"point-count": len(evts)})
		return c.sfxSink.AddEvents(context.Background(), evts)
	})
	if err != nil && err.Error() == "invalid status code 400" { // internal buffer full in sfx
		return kbc.PartialSendBatchError{ErrMessage: "failed to add events: " + err.Error(), FailedMessages: batch}
	}

	// only send to Cloudwatch if the tag is an AWS region
	if tag == "us-west-1" || tag == "us-west-2" || tag == "us-east-1" || tag == "us-east-2" {
		lg.TraceD("cloudwatch-add-datapoints", logger.M{"point-count": len(dats)})
		cw, _ := c.cwAPIs[tag]
		_, err = cw.PutMetricData(&cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(cloudwatchNamespace),
			MetricData: dats,
		})
		// TODO: We could return that we failed to process this batch (as above) but for now just log
		if err != nil {
			lg.ErrorD("error-sending-to-cloudwatch", logger.M{"error": err.Error()})
		}
	}

	return nil
}
