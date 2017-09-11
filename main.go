package main

import (
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/kardianos/osext"
	"github.com/signalfx/golib/sfxclient"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/kayvee-go/logger"
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

func setupLogRouting() {
	dir, err := osext.ExecutableFolder()
	if err != nil {
		log.Fatal(err)
	}
	err = logger.SetGlobalRouting(path.Join(dir, "kvconfig.yml"))
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	setupLogRouting()

	config := kbc.Config{
		LogFile:       "/tmp/kinesis-consumer-" + time.Now().Format(time.RFC3339),
		BatchCount:    100,
		BatchInterval: time.Second * 5,
		ReadRateLimit: getIntEnv("READ_RATE_LIMIT"),
	}

	sfxSink := sfxclient.NewHTTPSink()
	sfxSink.AuthToken = getEnv("SFX_API_TOKEN")
	minTimestamp := getIntEnv("MINIMUM_TIMESTAMP")
	ac := NewAlertsConsumer(
		sfxSink,
		getEnv("DEPLOY_ENV"),
		time.Unix(int64(minTimestamp), 0),
	)

	// Track Max Delay
	go func() {
		c := time.Tick(15 * time.Second)
		for _ = range c {
			logMaxDelayThenReset()
		}
	}()

	consumer := kbc.NewBatchConsumer(config, ac)
	consumer.Start()
}
