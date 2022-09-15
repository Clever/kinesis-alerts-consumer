package main

import (
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"golang.org/x/net/context"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

type envAppTeam struct {
	env  string
	app  string
	team string
}

type volume struct {
	count int64
	size  int64
}

type logRoute struct {
	app  string
	name string
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
		logRouteVolumes[logRoute{app, n}] = volume{v.count + 1, v.size + int64(numBytes)}
	}
}

func logVolumesAndReset(dd DDMetricsAPI) {
	metricMu.Lock()
	logVolumesCopy := logVolumesByEnvAppTeam
	logVolumesByEnvAppTeam = map[envAppTeam]volume{}
	metricMu.Unlock()

	var dps []*datapoint.Datapoint
	var totalCount, totalSize int64
	for eat, vol := range logVolumesCopy {
		dps = append(dps,
			sfxclient.Cumulative("kinesis-consumer.log-volume-count", map[string]string{
				"env":         eat.env,
				"application": eat.app,
				"team":        eat.team,
			}, vol.count),
			sfxclient.Cumulative("kinesis-consumer.log-volume-size", map[string]string{
				"env":         eat.env,
				"application": eat.app,
				"team":        eat.team,
			}, vol.size))
		totalCount += vol.count
		totalSize += vol.size
	}
	err := retry.Run(func() error {
		acc, res, err := dd.SubmitMetrics(context.Background(), *datadogMetricPayloadFromPts(dps))
		lg.TraceD("send-log-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "point-count": len(dps), "dd-response": acc.Status})
		if res.StatusCode != 202 || err != nil {
			b, _ := ioutil.ReadAll(res.Body)
			return fmt.Errorf("status code %d received from DD api Err = %v RawBody = %s", res.StatusCode, err, b)
		}
		return nil
	})
	if err != nil {
		lg.ErrorD("failed-sending-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "error": err.Error()})
	}
}
