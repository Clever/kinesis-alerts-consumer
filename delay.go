package main

import (
	"sync/atomic"
	"time"
)

var maxDelay int64 = 0

func updateMaxDelay(ts []time.Time) {
	cur := atomic.LoadInt64(&maxDelay)
	var max int64
	for _, t := range ts {
		// If a timestamp is set
		if (t != time.Time{}) {
			// how long ago is the log from?
			lag := int64(time.Since(t))
			if lag > cur {
				max = lag
			}
		}
	}
	if max > cur {
		atomic.StoreInt64(&maxDelay, max)
	}
}

func logMaxDelayThenReset() {
	lg.GaugeFloat("max-log-delay", time.Duration(atomic.LoadInt64(&maxDelay)).Seconds())
	// Reset
	atomic.StoreInt64(&maxDelay, 0)
}
