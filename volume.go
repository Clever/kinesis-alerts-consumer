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

type volume struct {
	count int64
	size  int64
}

var logVolumesByEnvAppTeam = map[envAppTeam]volume{}
var logVolumesLock = sync.Mutex{}
var retry = retrier.New(retrier.ExponentialBackoff(5, 50*time.Millisecond), nil)

func updatelogVolumes(env, app, team string, numBytes int) {
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
}

func logVolumesAndReset(sfx *sfxclient.HTTPSink) {
	logVolumesCopy := logVolumesByEnvAppTeam
	logVolumesLock.Lock()
	logVolumesByEnvAppTeam = map[envAppTeam]volume{}
	logVolumesLock.Unlock()

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
		lg.TraceD("send-log-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "point-count": len(dps)})
		return sfx.AddDatapoints(context.Background(), dps)
	})
	if err != nil {
		lg.ErrorD("failed-sending-volumes", logger.M{"total-logs": totalCount, "total-size": totalSize, "error": err.Error()})
	}
}
