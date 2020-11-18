package main

import (
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

var logVolumesByEnvAppTeam = map[envAppTeam]int64{}
var logVolumesLock = sync.Mutex{}
var retry = retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)

func updatelogVolumes(env, app, team string) {
	if env == "" {
		env = "unknown"
	}
	if app == "" {
		app = "unknown"
	}
	if team == "" {
		team = "unknown"
	}
	logVolumesLock.Lock()
	defer logVolumesLock.Unlock()
	logVolumesByEnvAppTeam[envAppTeam{
		env:  env,
		app:  app,
		team: team,
	}] += 1
}

func logVolumesAndReset(sfx *sfxclient.HTTPSink) {
	logVolumesCopy := logVolumesByEnvAppTeam
	logVolumesLock.Lock()
	logVolumesByEnvAppTeam = map[envAppTeam]int64{}
	logVolumesLock.Unlock()

	var dps []*datapoint.Datapoint
	var totalCount int64
	for eat, count := range logVolumesCopy {
		dps = append(dps, sfxclient.Cumulative("kinesis-consumer.log-volume", map[string]string{
			"env":         eat.env,
			"application": eat.app,
			"team":        eat.team,
		}, count))
		totalCount += count
	}
	err := retry.Run(func() error {
		lg.TraceD("send-log-volumes", logger.M{"total-logs": totalCount, "point-count": len(dps)})
		return sfx.AddDatapoints(context.Background(), dps)
	})
	if err != nil {
		lg.ErrorD("failed-sending-volumes", logger.M{"total-logs": totalCount, "error": err.Error()})
	}
}
