package main

import (
	"sync/atomic"
	"time"
)

var maxDelay = atomic.Int64{}

func updateMaxDelay(ts []time.Time) {
	cur := maxDelay.Load()
	// If a timestamp is set
	var max int64
	for _, t := range ts {
		if (t != time.Time{}) {
			// how long ago is the log from?
			lag := int64(time.Since(t))
			if lag > cur {
				max = lag
			}
		}
	}
	if max > cur {
		maxDelay.Store(max)
	}
}

func isRecent(t time.Time, allowedDelay time.Duration) bool {
	return time.Since(t) <= allowedDelay
}

func logMaxDelayThenReset() {
	lg.GaugeFloat("max-log-delay", time.Duration(maxDelay.Load()).Seconds())
	// Reset
	maxDelay.Store(0)
}
