package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	datadog "github.com/DataDog/datadog-api-client-go/api/v2/datadog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/eapache/go-resiliency/retrier"
	"golang.org/x/net/context"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/amazon-kinesis-client-go/decode"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var (
	lg                = logger.New("kinesis-alerts-consumer")
	defaultDimensions = []string{"Hostname", "env"}
)

const cloudwatchNamespace = "LogMetrics"

// AlertsConsumer sends datapoints to DataDog
// It implements the kbc.Sender interface
type AlertsConsumer struct {
	dd        DDMetricsAPI
	deployEnv string
	cwAPIs    map[string]cloudwatchiface.CloudWatchAPI
}

// DDMetricsAPI is the subset of the Datadog Metrics API that we use
type DDMetricsAPI interface {
	SubmitMetrics(ctx context.Context, body datadog.MetricPayload, o ...datadog.SubmitMetricsOptionalParameters) (datadog.IntakePayloadAccepted, *http.Response, error)
}

func NewAlertsConsumer(dd DDMetricsAPI, deployEnv string, cwAPIs map[string]cloudwatchiface.CloudWatchAPI) *AlertsConsumer {
	return &AlertsConsumer{
		dd:        dd,
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

	return c.encodeMessage(fields, len(rawmsg))
}

type EncodeOutput struct {
	DDMetrics []datadog.MetricSeries
	CWMetrics []*cloudwatch.MetricDatum
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

func (c *AlertsConsumer) encodeMessage(fields map[string]interface{}, numBytes int) ([]byte, []string, error) {
	// Determine routes
	// KVMeta Routes
	kvmeta := decode.ExtractKVMeta(fields)
	env, _ := fields["container_env"].(string)
	app, _ := fields["container_app"].(string)
	team, _ := fields["team"].(string)
	if team == "" {
		team = kvmeta.Team
	}
	recordMetrics(env, app, team, numBytes, kvmeta.Routes.RuleNames())

	routes := kvmeta.Routes.AlertRoutes()
	for idx := range routes {
		routes[idx].Dimensions = append(routes[idx].Dimensions, defaultDimensions...)
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
		DDMetrics: []datadog.MetricSeries{},
		CWMetrics: []*cloudwatch.MetricDatum{},
	}

	// This is set to an AWS region if any of the routes are for metrics that are whitelisted for CloudWatch.
	// It is used to set a tag to group data points together before pushing to CloudWatch.
	tag := "default"

	for _, route := range routes {
		// Look up dimensions (custom + default)
		tags := []string{}
		cwDims := []*cloudwatch.Dimension{}
		for _, dim := range route.Dimensions {
			dimVal, ok := fields[dim]
			if ok {
				switch t := dimVal.(type) {
				case string:
					tags = append(tags, fmt.Sprintf("%s:%s", dim, t))
					if !contains(defaultDimensions, dim) {
						cwDims = append(cwDims, &cloudwatch.Dimension{
							Name:  aws.String(dim),
							Value: &t,
						})
					}
				case float64:
					// Drop data after the decimal and cast to string (ex. 3.2 => "3")
					val := fmt.Sprintf("%.0f", t)
					tags = append(tags, fmt.Sprintf("%s:%s", dim, val))
					cwDims = append(cwDims, &cloudwatch.Dimension{
						Name:  aws.String(dim),
						Value: &val,
					})
				case bool:
					val := fmt.Sprintf("%t", t)
					tags = append(tags, fmt.Sprintf("%s:%s", dim, val))
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

		// 3 cases
		// 	(1) val exists and it's a float
		// 	(2) val exists but it's NOT a float (error)
		// 	(3) val doesn't exist => use default value
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

		var (
			metricValue float64
			metricType  datadog.MetricIntakeType
		)
		switch route.StatType {
		case "counter":
			metricType = datadog.METRICINTAKETYPE_COUNT
			metricValue = 1
			if valOk {
				metricValue = val
			}
		case "gauge":
			metricType = datadog.METRICINTAKETYPE_GAUGE
			metricValue = 0
			if valOk {
				metricValue = val
			}
		default:
			return nil, nil, fmt.Errorf("invalid StatType: %s", route.StatType)
		}

		eo.DDMetrics = append(eo.DDMetrics, datadog.MetricSeries{
			Metric: "kv." + route.Series,
			Type:   metricType.Ptr(),
			Tags:   tags,
			Points: []datadog.MetricPoint{
				{
					Timestamp: datadog.PtrInt64(timestamp.Unix()),
					Value:     aws.Float64(metricValue),
				},
			},
		})

		if _, ok := cloudwatchAllowList[route.Series]; ok {
			dat := &cloudwatch.MetricDatum{
				MetricName:        &route.Series,
				Dimensions:        cwDims,
				Value:             aws.Float64(metricValue),
				Timestamp:         &timestamp,
				StorageResolution: aws.Int64(1),
			}
			if region, ok := fields["region"].(string); ok {
				tag = region
				eo.CWMetrics = append(eo.CWMetrics, dat)
			} else if podRegion, ok := fields["pod-region"].(string); ok {
				tag = podRegion
				eo.CWMetrics = append(eo.CWMetrics, dat)
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
	metrics := []datadog.MetricSeries{}
	dats := []*cloudwatch.MetricDatum{}
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
		_, err = api.PutMetricData(&cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(cloudwatchNamespace),
			MetricData: dats,
		})
		if err != nil {
			lg.ErrorD("error-sending-to-cloudwatch", logger.M{"error": err.Error()})
		}
	}

	return nil
}
