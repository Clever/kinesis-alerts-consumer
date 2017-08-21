package main

import (
	"fmt"
	"strings"

	"github.com/Clever/amazon-kinesis-client-go/decode"
)

const defaultValueField = "value"
const statTypeCounter = "counter"
const statTypeGauge = "gauge"

func globalRoutes(fields map[string]interface{}) []decode.AlertRoute {
	routes := []decode.AlertRoute{}

	// chain and append all global routes here
	// TODO: After initial migration, revisit these routes and ensure they all
	// emit hostname+env via default dimensions
	routes = append(routes, processMetricsRoutes(fields)...)
	routes = append(routes, rsyslogRateLimitRoutes(fields)...)
	routes = append(routes, gearmanRoutes(fields)...)
	routes = append(routes, gearcmdPassfailRoutes(fields)...)
	routes = append(routes, gearcmdDurationRoutes(fields)...)
	routes = append(routes, gearcmdHeartbeatRoutes(fields)...)
	routes = append(routes, wagCircuitBreakerRoutes(fields)...)

	return routes
}

// Metrics emitted by node-process-metrics and go-process-metrics libraries
func processMetricsRoutes(fields map[string]interface{}) []decode.AlertRoute {
	via, ok := fields["via"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if via != "process-metrics" {
		return []decode.AlertRoute{}
	}

	_, ok = fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	statType, ok := fields["type"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     fmt.Sprintf("process-metrics.%s", title),
			Dimensions: []string{"Hostname", "env", "source"},
			StatType:   statType,
			ValueField: defaultValueField,
			RuleName:   "global-process-metrics",
		},
	}
}

// This log serves as a regression test, in case we later mistakenly re-enable RSyslog rate limiting.
func rsyslogRateLimitRoutes(fields map[string]interface{}) []decode.AlertRoute {
	programname, ok := fields["programname"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !strings.Contains(programname, "rsyslog") {
		return []decode.AlertRoute{}
	}

	rawlog, ok := fields["rawlog"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	if !strings.Contains(rawlog, "imuxsock begins to drop messages") {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "rsyslog.rate-limit-triggered",
			Dimensions: []string{"Hostname", "env"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-rsyslog-rate-limit",
		},
	}
}

// Legacy logs for gearman workers
func gearmanRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearman" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !(title == "success" || title == "failure") {
		return []decode.AlertRoute{}
	}

	env, ok := fields["env"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if env != "production" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     fmt.Sprintf("gearman.%s", title),
			Dimensions: []string{"Hostname", "function"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-gearman",
		},
	}
}

// Track failure rates for Gearcmd based workers
func gearcmdPassfailRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "END" {
		return []decode.AlertRoute{}
	}

	env, ok := fields["env"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if env != "production" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.passfail",
			Dimensions: []string{"Hostname", "function"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-passfail",
		},
	}
}

// Track duration for Gearcmd based workers
func gearcmdDurationRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "duration" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.duration",
			Dimensions: []string{"Hostname", "function", "env"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-duration",
		},
	}
}

// Gearman workers pipe heartbeats, to track job execution time
func gearcmdHeartbeatRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "heartbeat" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.heartbeat",
			Dimensions: []string{"Hostname", "env", "function", "job_id", "try_number", "unit"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-heartbeat",
		},
	}
}

// Wag client circuit breaker error rates
func wagCircuitBreakerRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !strings.Contains(source, "wagclient") {
		return []decode.AlertRoute{}
	}

	_, ok = fields["errorPercentage"].(float64)
	if !ok {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "wag.client-circuit-breakers",
			Dimensions: []string{"container_env", "container_app", "title"},
			StatType:   statTypeGauge,
			ValueField: "errorPercentage",
			RuleName:   "global-wag-circuit-breakers",
		},
	}
}
