package main

import (
	"sync/atomic"
	"time"
)

// Generally you want to steer clear of sync/atomic, but simple numbers like this where
// you are concurrently storing a usage statistic is one of the few cases where it
// makes sense over a mutex. atomic does not provide read/write ordering guarantees
// which don't matter here as much as general trends do.
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
	// Reset the value
	val := atomic.SwapInt64(&maxDelay, 0)
	lg.GaugeFloat("max-log-delay", time.Duration(val).Seconds())
}
