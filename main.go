package main

import (
	"log"
	"os"
	"time"

	"github.com/signalfx/golib/sfxclient"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
)

// getEnv returns required environment variable
func getEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Panicf("Missing env var: %s\n", key)
	}
	return value
}

func main() {
	config := kbc.Config{
		LogFile: "/tmp/kinesis-consumer-" + time.Now().Format(time.RFC3339),
		// Match config in the KPL library we're using
		BatchCount:    500,
		BatchSize:     51200, // 50 KB
		BatchInterval: time.Second,
	}

	sfxSink := sfxclient.NewHTTPSink()
	sfxSink.AuthToken = getEnv("SFX_API_TOKEN")
	ac := &AlertsConsumer{
		sfxSink:      sfxSink,
		deployEnv:    "todo",      // TODO
		minTimestamp: time.Time{}, // TODO
	}
	consumer := kbc.NewBatchConsumer(config, ac)
	consumer.Start()
}
