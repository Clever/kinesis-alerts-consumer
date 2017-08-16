package main

import (
	"sync"
	"time"
)

var maxDelay = 0 * time.Millisecond
var maxDelayLock = sync.RWMutex{}

func updateMaxDelay(t time.Time) {
	maxDelayLock.Lock()
	defer maxDelayLock.Unlock()

	// If a timestamp is set
	if (t != time.Time{}) {
		// how long ago is the log from?
		lag := time.Now().Sub(t)
		if lag > maxDelay {
			maxDelay = lag
		}
	}
}

func isRecent(t time.Time, allowedDelay time.Duration) bool {
	if (t == time.Time{}) {
		return false
	}

	// how long ago is the log from?
	lag := time.Now().Sub(t)
	if lag > allowedDelay {
		return false
	}

	return false
}

func logMaxDelayThenReset() {
	maxDelayLock.Lock()
	defer maxDelayLock.Unlock()
	lg.GaugeFloat("max-log-delay", maxDelay.Seconds())
	// Reset
	maxDelay = 0
}
