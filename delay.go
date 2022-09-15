package main

import (
	"sync/atomic"
	"time"
)

var maxDelay = atomic.Int64{}

func updateMaxDelay(t time.Time) {
	cur := maxDelay.Load()
	// If a timestamp is set
	if (t != time.Time{}) {
		// how long ago is the log from?
		lag := int64(time.Now().Sub(t))
		if lag > cur {
			maxDelay.Store(lag)
		}
	}
}

func isRecent(t time.Time, allowedDelay time.Duration) bool {
	return time.Now().Sub(t) <= allowedDelay
}

func logMaxDelayThenReset() {
	lg.GaugeFloat("max-log-delay", time.Duration(maxDelay.Load()).Seconds())
	// Reset
	maxDelay.Store(0)
}
