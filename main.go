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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

	config := kbc.Config{
		FailedLogsFile: "/tmp/kinesis-consumer-" + time.Now().Format(time.RFC3339),
		BatchCount:     100,
		BatchInterval:  time.Second * 5,
		ReadRateLimit:  getIntEnv("READ_RATE_LIMIT"),
	}

	// Create AWS config for each region
	ctx := context.Background()
	cwAPIs := make(map[string]*cloudwatch.Client)

	regions := []string{"us-west-1", "us-west-2", "us-east-1", "us-east-2"}
	for _, region := range regions {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		if err != nil {
			log.Printf("Failed to load AWS config for region %s: %v", region, err)
			continue
		}
		cwAPIs[region] = cloudwatch.NewFromConfig(cfg)
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

	consumer := kbc.NewBatchConsumer(config, ac)
	consumer.Start()
}
