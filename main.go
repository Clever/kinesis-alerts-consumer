package main

import (
	"context"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/kayvee-go/v7/logger"
	datadog "github.com/DataDog/datadog-api-client-go/api/v2/datadog"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/kardianos/osext"
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

	consumerConfig := kbc.Config{
		FailedLogsFile: "/tmp/kinesis-consumer-" + time.Now().Format(time.RFC3339),
		BatchCount:     100,
		BatchInterval:  time.Second * 5,
		ReadRateLimit:  getIntEnv("READ_RATE_LIMIT"),
	}

	// Initialize AWS SDK v2 config
	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("unable to load SDK config: ", err)
	}

	cwAPIs := map[string]CloudWatchAPI{
		"us-west-1": cloudwatch.NewFromConfig(awsConfig, func(o *cloudwatch.Options) {
			o.Region = "us-west-1"
		}),
		"us-west-2": cloudwatch.NewFromConfig(awsConfig, func(o *cloudwatch.Options) {
			o.Region = "us-west-2"
		}),
		"us-east-1": cloudwatch.NewFromConfig(awsConfig, func(o *cloudwatch.Options) {
			o.Region = "us-east-1"
		}),
		"us-east-2": cloudwatch.NewFromConfig(awsConfig, func(o *cloudwatch.Options) {
			o.Region = "us-east-2"
		}),
	}

	ddAPIClient := datadog.NewAPIClient(datadog.NewConfiguration())

	ac := NewAlertsConsumer(ddAPIClient.MetricsApi, getEnv("DEPLOY_ENV"), cwAPIs)

	// Track Max Delay
	go func() {
		for range time.Tick(15 * time.Second) {
			logMaxDelay()
		}
	}()

	// Track Volume
	go func() {
		tic := time.Tick(time.Minute)
		processMetrics(ddAPIClient.MetricsApi, tic)
	}()

	consumer := kbc.NewBatchConsumer(consumerConfig, ac)
	consumer.Start()
}
