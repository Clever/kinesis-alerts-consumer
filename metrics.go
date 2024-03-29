package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/DataDog/datadog-api-client-go/api/v2/datadog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eapache/go-resiliency/retrier"
	"golang.org/x/net/context"

	"github.com/Clever/kayvee-go/v7/logger"
)

type envAppTeam struct {
	env  string
	app  string
	team string
}

type logRoute struct {
	app      string
	env      string
	ruleName string
}

type volume struct {
	count int
	size  int
}

type work struct {
	eat  *envAppTeam
	lr   *logRoute
	size int
}

var (
	envAppTeamVolumes = map[envAppTeam]volume{}
	logRouteVolumes   = map[logRoute]int{}
	retry             = retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)
	// 10000 is hopefully sufficiently large to prevent metrics recording from blocking
	chMetrics = make(chan work, 10000)
)

// A thread safe way to record metrics pipeline metrics.
func recordMetrics(env, app, team string, numBytes int, routeNames []string) {
	if env == "" {
		env = "unknown"
	}
	if app == "" {
		app = "unknown"
	}
	if team == "" {
		team = "unknown"
	}
	chMetrics <- work{
		eat:  &envAppTeam{env, app, team},
		size: numBytes,
	}

	for _, n := range routeNames {
		chMetrics <- work{
			lr: &logRoute{app, env, n},
		}
	}
}

// processMetrics aggregates all metrics sent over the channel by recordMetrics. On the interval of the ticker
// the aggregates will be shipped to DD and reset. Process is not thread safe and should only be run by one
// goroutine.
func processMetrics(dd DDMetricsAPI, tic <-chan time.Time) {
	for {
		select {
		case <-tic:
			shipMetrics(dd)
		case w := <-chMetrics:
			if w.eat != nil {
				vol := envAppTeamVolumes[*w.eat]
				envAppTeamVolumes[*w.eat] = volume{vol.count + 1, vol.size + w.size}
			}
			if w.lr != nil {
				n := logRouteVolumes[*w.lr]
				logRouteVolumes[*w.lr] = n + 1
			}
		}
	}
}

func shipMetrics(dd DDMetricsAPI) {
	eatCopy := envAppTeamVolumes
	envAppTeamVolumes = map[envAppTeam]volume{}

	lrCopy := logRouteVolumes
	logRouteVolumes = map[logRoute]int{}

	// do work that involves network calls async
	go func() {
		var (
			metrics               []datadog.MetricSeries
			totalCount, totalSize int
		)
		for eat, vol := range eatCopy {
			tags := []string{
				"env:" + eat.env,
				"application:" + eat.app,
				"team:" + eat.team,
			}
			metrics = append(metrics,
				datadog.MetricSeries{
					Metric: "kinesis_alerts_consumer.log_volume_count",
					Type:   datadog.METRICINTAKETYPE_COUNT.Ptr(),
					Tags:   tags,
					Points: []datadog.MetricPoint{
						{
							Timestamp: datadog.PtrInt64(time.Now().Unix()),
							Value:     aws.Float64(float64(vol.count)),
						},
					},
				},
				datadog.MetricSeries{
					Metric: "kinesis_alerts_consumer.log_volume_size",
					Type:   datadog.METRICINTAKETYPE_COUNT.Ptr(),
					Tags:   tags,
					Points: []datadog.MetricPoint{
						{
							Timestamp: datadog.PtrInt64(time.Now().Unix()),
							Value:     aws.Float64(float64(vol.size)),
						},
					},
				},
			)
			totalCount += vol.count
			totalSize += vol.size
		}

		for lr, n := range lrCopy {
			tags := []string{
				"env:" + lr.env,
				"application:" + lr.app,
				"route:" + lr.ruleName,
			}
			metrics = append(metrics,
				datadog.MetricSeries{
					Metric: "kinesis_alerts_consumer.log_route_count",
					Type:   datadog.METRICINTAKETYPE_COUNT.Ptr(),
					Tags:   tags,
					Points: []datadog.MetricPoint{
						{
							Timestamp: datadog.PtrInt64(time.Now().Unix()),
							Value:     aws.Float64(float64(n)),
						},
					},
				},
			)
		}

		err := retry.Run(func() error {
			acc, res, err := dd.SubmitMetrics(datadog.NewDefaultContext(context.Background()), *datadog.NewMetricPayload(metrics))
			lg.TraceD("send-log-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "point-count": len(metrics), "dd-response": acc.Status})
			if res.StatusCode != 202 || err != nil {
				// Make a best attempt at reading the body, if we error here then ¯\_(ツ)_/¯
				b, _ := ioutil.ReadAll(res.Body)
				return fmt.Errorf("status code %d received from DD api Err = %v RawBody = %s", res.StatusCode, err, b)
			}
			return nil
		})
		if err != nil {
			lg.ErrorD("failed-sending-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "error": err.Error()})
		}
	}()
}
