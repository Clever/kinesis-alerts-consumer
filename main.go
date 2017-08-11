package main

import (
	"log"
	"os"
	"time"

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

	consumer := kbc.NewBatchConsumer(config, &AlertsConsumer{})
	consumer.Start()
}
