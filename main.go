package main

import (
	"log"
	"os"
	"strconv"
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

// getIntEnv returns the required environment variable as an int
func getIntEnv(key string) int {
	value, err := strconv.Atoi(getEnv(key))
	if err != nil {
		log.Panicf("Environment variable `%s` is not an integer: %s", key, err.Error())
	}
	return value
}

func main() {
	config := kbc.Config{
		LogFile:    "/tmp/kinesis-consumer-" + time.Now().Format(time.RFC3339),
		BatchCount: 1, // call SendBatch immediately; batching is handled by sfxclient HTTP Sink
	}

	sfxSink := sfxclient.NewHTTPSink()
	sfxSink.AuthToken = getEnv("SFX_API_TOKEN")
	minTimestamp := getIntEnv("MINIMUM_TIMESTAMP")
	ac := &AlertsConsumer{
		sfxSink:      sfxSink,
		deployEnv:    getEnv("DEPLOY_ENV"),
		minTimestamp: time.Unix(int64(minTimestamp), 0),
	}
	consumer := kbc.NewBatchConsumer(config, ac)
	consumer.Start()
}
