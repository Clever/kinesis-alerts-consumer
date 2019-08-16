package main

// For now, we will only send metrics to Cloudwatch when they are present in this whitelist.
// Ideally if we move to putting everything in Cloudwatch, we will eventually remove this.

// Also note that Cloudwatch can only take inputs with up to 20 different metrics, so if this list
// gets large we will have to reduce the batch count in main.go
var cloudwatchWhitelist = map[string]bool{
	"ContainerExitCount": true,
}
