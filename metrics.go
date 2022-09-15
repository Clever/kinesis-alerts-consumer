package main

import (
	"fmt"
	"io/ioutil"
	"sync"
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
	count int64
	size  int64
}

var (
	logVolumesByEnvAppTeam = map[envAppTeam]volume{}
	logRouteVolumes        = map[logRoute]volume{}
	metricMu               = sync.Mutex{}
	retry                  = retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)
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
	metricMu.Lock()
	defer metricMu.Unlock()
	vol := logVolumesByEnvAppTeam[envAppTeam{
		env:  env,
		app:  app,
		team: team,
	}]
	vol.count += 1
	vol.size += int64(numBytes)
	logVolumesByEnvAppTeam[envAppTeam{
		env:  env,
		app:  app,
		team: team,
	}] = vol

	for _, n := range routeNames {
		v := logRouteVolumes[logRoute{app, n}]
		// TODO: handle size
		logRouteVolumes[logRoute{app, n}] = volume{v.count + 1, v.size + int64(numBytes)}
	}
}

func logVolumesAndReset(dd DDMetricsAPI) {
	metricMu.Lock()
	logVolumesCopy := logVolumesByEnvAppTeam
	logVolumesByEnvAppTeam = map[envAppTeam]volume{}
	metricMu.Unlock()

	var metrics []datadog.MetricSeries
	var totalCount, totalSize int64
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
		acc, res, err := dd.SubmitMetrics(context.Background(), *datadog.NewMetricPayload(metrics))
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
}
