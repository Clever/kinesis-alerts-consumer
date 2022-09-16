package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/DataDog/datadog-api-client-go/api/v2/datadog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eapache/go-resiliency/retrier"
	"golang.org/x/net/context"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

type envAppTeam struct {
	env  string
	app  string
	team string
}

type logRoute struct {
	app  string
	name string
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
	logRouteVolumes   = map[logRoute]volume{}
	retry             = retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)
	// 10000 is hopefully sufficiently large to prevent metrics recording from blocking
	chMetrics = make(chan work, 10000)
)

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
			lr:   &logRoute{app, n},
			size: numBytes,
		}
	}
}

func processMetrics(dd DDMetricsAPI, tic <-chan time.Time) {
	for {
		select {
		case <-tic:
			logVolumesAndReset(dd)
		case w := <-chMetrics:
			if w.eat != nil {
				vol := envAppTeamVolumes[*w.eat]
				envAppTeamVolumes[*w.eat] = volume{vol.count + 1, vol.size + w.size}
			}
			if w.lr != nil {
				vol := logRouteVolumes[*w.lr]
				logRouteVolumes[*w.lr] = volume{vol.count + 1, vol.size + w.size}
			}
		}
	}
}

func logVolumesAndReset(dd DDMetricsAPI) {
	logVolumesCopy := envAppTeamVolumes
	envAppTeamVolumes = map[envAppTeam]volume{}

	// do work that involves network calls async
	go func() {
		var metrics []datadog.MetricSeries
		var totalCount, totalSize int
		for eat, vol := range logVolumesCopy {
			tags := []string{
				fmt.Sprintf("env:%s", eat.env),
				fmt.Sprintf("application:%s", eat.app),
				fmt.Sprintf("team:%s", eat.team),
			}
			metrics = append(metrics,
				datadog.MetricSeries{
					Metric: "kinesis-consumer.log-volume-count",
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
					Metric: "kinesis-consumer.log-volume-size",
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
